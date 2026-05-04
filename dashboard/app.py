from flask import Flask, render_template, jsonify
import json
import os

# Inisialisasi Flask dengan folder template yang benar
app = Flask(__name__, template_folder='templates')

# Lokasi folder data (relatif terhadap app.py)
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

# Debug mode control via environment variable (default: False for security)
DEBUG_MODE = os.getenv('FLASK_DEBUG', 'False').lower() in ('true', '1', 'yes')

@app.route('/')
def index():
    return render_template('index.html')

# PASTIKAN bagian ini ada dan tidak typo
@app.route('/api/data')
def get_data():
    data = {
        "live_api": [],
        "live_rss": [],
        "spark": {"top_words": [], "source_dist": [], "hourly_vol": []}
    }
    
    def load_json(filename):
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return None
        return None

    # Mengambil data dari file JSON
    api = load_json('live_api.json')
    if api: data['live_api'] = api
    
    rss = load_json('live_rss.json')
    if rss: data['live_rss'] = rss
    
    spark = load_json('spark_results.json')
    if spark: data['spark'] = spark
            
    return jsonify(data)

if __name__ == '__main__':
    # Jalankan Flask
    app.run(debug=DEBUG_MODE, port=5000)