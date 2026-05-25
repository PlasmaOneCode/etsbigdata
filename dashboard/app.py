from flask import Flask, render_template, jsonify
import json, os, glob, math
import pandas as pd

app = Flask(__name__, template_folder='templates')

DATA_DIR      = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
LAKEHOUSE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lakehouse_data')

DEBUG_MODE        = os.getenv('FLASK_DEBUG', 'False').lower() in ('true', '1', 'yes')
MAX_DISPLAY_ITEMS = 50
MIN_ITEMS_THRESHOLD = 10


# ─────────────────────────────────────────────
#  UTILITY: sanitize NaN / Inf / Timestamps
# ─────────────────────────────────────────────
def _clean_value(v):
    """Convert a single value to JSON-safe form."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    # pandas Timestamp → ISO string
    if hasattr(v, 'isoformat'):
        return v.isoformat()
    return v


def sanitize_df(df: pd.DataFrame) -> list:
    """
    Convert DataFrame to list-of-dicts with all NaN / Inf / Timestamp
    replaced by JSON-safe equivalents.
    This is THE fix for: SyntaxError: Unexpected token 'N', ...NaN... is not valid JSON
    """
    records = df.to_dict(orient='records')
    return [{k: _clean_value(v) for k, v in row.items()} for row in records]


def read_delta(relative_path: str) -> pd.DataFrame:
    """Read all parquet files in a Delta Lake table directory."""
    full = os.path.join(LAKEHOUSE_DIR, relative_path)
    parts = glob.glob(os.path.join(full, 'part-*.parquet'))
    if not parts:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)


def read_delta_log(relative_path: str) -> list:
    """Parse _delta_log to extract Time Travel history."""
    log_dir = os.path.join(LAKEHOUSE_DIR, relative_path, '_delta_log')
    entries = []
    if not os.path.exists(log_dir):
        return entries
    for fname in sorted(os.listdir(log_dir)):
        if not fname.endswith('.json') or fname.startswith('.'):
            continue
        try:
            with open(os.path.join(log_dir, fname)) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    if 'commitInfo' not in obj:
                        continue
                    ci  = obj['commitInfo']
                    ts  = ci.get('timestamp', 0) / 1000
                    from datetime import datetime, timezone
                    dt  = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                    m   = ci.get('operationMetrics', {})
                    entries.append({
                        'version':      int(fname.split('.')[0]),
                        'timestamp':    dt,
                        'operation':    ci.get('operation', ''),
                        'rows_written': int(m.get('numOutputRows', 0)),
                        'num_files':    int(m.get('numFiles', 0)),
                        'bytes':        int(m.get('numOutputBytes', 0)),
                    })
        except Exception:
            pass
    return entries


# ─────────────────────────────────────────────
#  EXISTING ROUTES
# ─────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/data')
def get_data():
    data = {
        "live_api": [],
        "live_rss": [],
        "spark": {"top_words": [], "source_dist": [], "hourly_vol": []}
    }

    def load_json(filename):
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
        try:
            with open(path, 'r', encoding='utf-8') as f:
                result = []
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            result.append(json.loads(line))
                        except Exception:
                            pass
                return result if result else None
        except Exception:
            return None

    def merge_with_fallback(live_file, fallback_file):
        live = load_json(live_file)
        if not isinstance(live, list):
            live = []
        fallback_raw = load_json(fallback_file)
        fallback = []
        if isinstance(fallback_raw, list):
            fallback = fallback_raw
        elif isinstance(fallback_raw, dict):
            fallback = [fallback_raw]
        seen = {}
        for item in fallback:
            if not isinstance(item, dict):
                continue
            key = item.get('url') or item.get('title', '')
            if key:
                seen[key] = item
        for item in live:
            if not isinstance(item, dict):
                continue
            key = item.get('url') or item.get('title', '')
            if key:
                seen[key] = item
        merged = sorted(seen.values(), key=lambda x: x.get('timestamp', ''), reverse=True)
        return merged[:MAX_DISPLAY_ITEMS]

    data['live_api'] = merge_with_fallback('live_api.json', 'api_from_hdfs.json')
    data['live_rss'] = merge_with_fallback('live_rss.json', 'rss_from_hdfs.json')

    spark = load_json('spark_results.json')
    if isinstance(spark, dict):
        data['spark'] = spark

    return jsonify(data)


# ─────────────────────────────────────────────
#  LAKEHOUSE ROUTES
# ─────────────────────────────────────────────
@app.route('/lakehouse')
def lakehouse():
    return render_template('lakehouse.html')


@app.route('/api/lakehouse')
def api_lakehouse():
    """
    Serve lakehouse stats to the dashboard.
    FIX: semua DataFrame di-sanitize sebelum jsonify()
         sehingga NaN → null (valid JSON), bukan NaN (tidak valid).
    """
    try:
        # ── Bronze ──────────────────────────────
        df_api = read_delta('bronze/news_api')
        df_rss = read_delta('bronze/news_rss')

        bronze_api_count = len(df_api)
        bronze_rss_count = len(df_rss)

        last_api = ''
        last_rss = ''
        if not df_api.empty and '_ingested_at' in df_api.columns:
            last_api = str(df_api['_ingested_at'].max())
        if not df_rss.empty and '_ingested_at' in df_rss.columns:
            last_rss = str(df_rss['_ingested_at'].max())

        # ── Silver ──────────────────────────────
        df_silver = read_delta('silver/news')
        silver_count = len(df_silver)
        silver_cols  = list(df_silver.columns) if not df_silver.empty else []

        # ── Gold ────────────────────────────────
        df_wf  = read_delta('gold/word_freq')
        df_nps = read_delta('gold/news_per_source')
        df_wv  = read_delta('gold/word_velocity')
        df_cst = read_delta('gold/cross_source_topics')

        # Deduplicate gold tables (multiple WRITE operations may stack)
        if not df_wf.empty:
            df_wf = df_wf.drop_duplicates()
        if not df_nps.empty:
            df_nps = df_nps.drop_duplicates()
        if not df_wv.empty:
            df_wv = df_wv.drop_duplicates()

        # ── Time Travel (from delta log) ─────────
        time_travel = read_delta_log('gold/word_freq')

        # ── Assemble response (NaN-safe) ─────────
        response = {
            'stats': {
                'bronze': {
                    'api_count':   bronze_api_count,
                    'rss_count':   bronze_rss_count,
                    'total':       bronze_api_count + bronze_rss_count,
                    'last_api_ingested': last_api,
                    'last_rss_ingested': last_rss,
                },
                'silver': {
                    'count':   silver_count,
                    'columns': silver_cols,
                    'transformations': [
                        'Deduplication (drop duplicate URLs)',
                        'Null filter (title + summary tidak boleh kosong)',
                        'Timestamp parsing → parsed_timestamp, jam, tanggal',
                        'Text normalization → title_clean (lowercase, hapus tanda baca)',
                        'Source standardization → source_normalized (uppercase)',
                    ]
                },
                'gold': {
                    'tables': 4,
                    'word_freq_rows':         len(df_wf),
                    'news_per_source_rows':   len(df_nps),
                    'word_velocity_rows':     len(df_wv),
                    'cross_source_rows':      len(df_cst),
                }
            },
            # sanitize_df() adalah kunci fix — NaN → None → JSON null
            'word_freq':          sanitize_df(df_wf.head(30))   if not df_wf.empty  else [],
            'news_per_source':    sanitize_df(df_nps)            if not df_nps.empty else [],
            'word_velocity':      sanitize_df(df_wv.head(20))   if not df_wv.empty  else [],
            'cross_source_topics':sanitize_df(df_cst.head(20))  if not df_cst.empty else [],
            'time_travel':        time_travel,
        }

        return jsonify(response)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=DEBUG_MODE, port=5000)
