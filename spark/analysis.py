from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, hour, to_timestamp, count, desc
import json
import os
import platform
import shutil
import subprocess
from datetime import datetime


def _candidate_java_homes_windows():
    candidates = []
    for env_name in ("ProgramFiles", "ProgramFiles(x86)"):
        base = os.environ.get(env_name)
        if not base:
            continue
        for vendor_dir in (
            os.path.join(base, "Java"),
            os.path.join(base, "Eclipse Adoptium"),
            os.path.join(base, "Amazon Corretto"),
        ):
            if not os.path.isdir(vendor_dir):
                continue
            try:
                for entry in os.listdir(vendor_dir):
                    full_path = os.path.join(vendor_dir, entry)
                    java_exe = os.path.join(full_path, "bin", "java.exe")
                    if os.path.isfile(java_exe):
                        candidates.append(full_path)
            except Exception:
                continue
    return candidates


def ensure_java_ready():
    """
    Pastikan Java tersedia untuk PySpark.
    Return tuple: (ready: bool, message: str)
    """
    java_home = os.environ.get("JAVA_HOME")
    if java_home and os.path.isfile(os.path.join(java_home, "bin", "java.exe" if os.name == "nt" else "java")):
        return True, f"JAVA_HOME terdeteksi: {java_home}"

    java_bin = shutil.which("java")
    if java_bin:
        derived_home = os.path.dirname(os.path.dirname(java_bin))
        os.environ["JAVA_HOME"] = derived_home
        return True, f"JAVA_HOME otomatis diset dari PATH: {derived_home}"

    if platform.system().lower() == "windows":
        for candidate in _candidate_java_homes_windows():
            os.environ["JAVA_HOME"] = candidate
            return True, f"JAVA_HOME otomatis diset dari instalasi Java: {candidate}"

    return (
        False,
        "Java tidak ditemukan. Install JDK 11/17 lalu set JAVA_HOME ke folder instalasi JDK "
        "(contoh: C:\\Program Files\\Java\\jdk-17)."
    )


def read_hdfs_topic_data(spark, hdfs_candidates, topic_type):
    """Mencoba membaca data topik dari beberapa endpoint HDFS."""
    last_error = None
    for hdfs_uri in hdfs_candidates:
        path = f"{hdfs_uri}/data/news/{topic_type}/*.json"
        try:
            print(f"[{datetime.now()}] 📖 Mencoba baca HDFS: {path}")
            df = spark.read.json(path)
            row_count = df.count()
            if row_count > 0:
                print(f"[{datetime.now()}] ✅ {topic_type.upper()} dari {hdfs_uri}: {row_count} rows")
                return df, hdfs_uri
            print(f"[{datetime.now()}] ⚠️  {topic_type.upper()} dari {hdfs_uri} kosong")
        except Exception as e:
            last_error = e
            print(f"[{datetime.now()}] ⚠️  Gagal baca {path}: {e}")

    if last_error:
        print(f"[{datetime.now()}] ⚠️  Semua endpoint HDFS gagal untuk {topic_type}: {last_error}")
    return None, None


def export_hdfs_topic_via_docker(topic_type, output_dir):
    """
    Fallback Windows: ekspor data HDFS lewat docker exec,
    lalu Spark baca file lokal hasil ekspor.
    Data baru di-MERGE dengan data lama (tidak overwrite) supaya
    histori batch lama tidak hilang.
    """
    os.makedirs(output_dir, exist_ok=True)
    local_file = os.path.join(output_dir, f"{topic_type}_from_hdfs.json")
    hdfs_glob = f"/data/news/{topic_type}/*.json"
    try:
        export_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", hdfs_glob]
        result = subprocess.run(export_cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0 or not result.stdout.strip():
            print(f"[{datetime.now()}] ⚠️  Fallback docker HDFS gagal ({topic_type}): {result.stderr}")
            return None  # jaga file lama, jangan overwrite dengan kosong

        # --- Baca data lama dari file yang sudah ada (jika ada) ---
        existing_items = {}
        if os.path.exists(local_file):
            try:
                with open(local_file, "r", encoding="utf-8") as f_old:
                    for line in f_old:
                        line = line.strip()
                        if line:
                            try:
                                item = json.loads(line)
                                key = item.get("url") or item.get("title", "")
                                if key:
                                    existing_items[key] = item
                            except Exception:
                                pass
            except Exception:
                pass

        # --- Merge data baru dari HDFS ke dalam existing ---
        for line in result.stdout.splitlines():
            line = line.strip()
            if line:
                try:
                    item = json.loads(line)
                    key = item.get("url") or item.get("title", "")
                    if key:
                        existing_items[key] = item  # data HDFS menimpa duplikat
                except Exception:
                    pass

        # --- Tulis ulang file (semua data gabungan) ---
        with open(local_file, "w", encoding="utf-8") as f:
            for item in existing_items.values():
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        print(f"[{datetime.now()}] ✅ Fallback ekspor HDFS sukses: {local_file} ({len(existing_items)} items)")
        return local_file
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️  Error fallback ekspor HDFS ({topic_type}): {e}")
        return None


def save_result_to_hdfs(result_file, timestamp):
    """Simpan hasil Spark ke HDFS via container namenode."""
    hdfs_result_dir = "/data/news/hasil"
    hdfs_file = f"spark_results_{timestamp}.json"
    try:
        subprocess.run(
            ["docker", "cp", result_file, f"namenode:/{hdfs_file}"],
            capture_output=True,
            text=True,
            check=False
        )
        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_result_dir],
            capture_output=True,
            text=True,
            check=False
        )
        put_result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", f"/{hdfs_file}", f"{hdfs_result_dir}/{hdfs_file}"],
            capture_output=True,
            text=True,
            check=False
        )
        subprocess.run(
            ["docker", "exec", "namenode", "rm", f"/{hdfs_file}"],
            capture_output=True,
            text=True,
            check=False
        )

        if put_result.returncode == 0:
            print(f"[{datetime.now()}] ✅ Hasil Spark tersimpan ke HDFS: {hdfs_result_dir}/{hdfs_file}")
        else:
            print(f"[{datetime.now()}] ⚠️  Gagal simpan hasil ke HDFS: {put_result.stderr}")
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️  Error simpan hasil ke HDFS: {e}")


def local_path_for_spark(path):
    normalized = path.replace("\\", "/")
    return f"file:///{normalized}"

def run_analysis():
    print(f"[{datetime.now()}] 🚀 Memulai inisialisasi Spark...")

    java_ready, java_msg = ensure_java_ready()
    if java_ready:
        print(f"[{datetime.now()}] ☕ {java_msg}")
    else:
        print(f"[{datetime.now()}] ❌ {java_msg}")
        return

    # Kandidat endpoint HDFS untuk berbagai environment.
    hdfs_candidates = [
        os.environ.get("HDFS_DEFAULT_FS"),
        "hdfs://localhost:8020",
        "hdfs://127.0.0.1:8020",
        "hdfs://namenode:8020"
    ]
    hdfs_candidates = [uri for uri in hdfs_candidates if uri]
    default_hdfs = hdfs_candidates[0]

    # Inisialisasi Spark
    try:
        spark = SparkSession.builder \
            .appName("NewsPulseAnalysis") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", default_hdfs) \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        print(f"[{datetime.now()}] ✅ SparkSession berhasil dibuat")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error membuat SparkSession: {e}")
        return

    print(f"[{datetime.now()}] 📂 Mencari data dari HDFS...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    dashboard_data_dir = os.path.join(base_dir, "dashboard", "data")
    print(f"[{datetime.now()}] 📍 Path dashboard data: {dashboard_data_dir}")

    try:
        # Di Windows + Docker Desktop, akses blok HDFS langsung dari Spark host sering gagal.
        # Fallback via docker exec diprioritaskan agar data tetap benar-benar berasal dari HDFS.
        api_export = export_hdfs_topic_via_docker("api", dashboard_data_dir)
        rss_export = export_hdfs_topic_via_docker("rss", dashboard_data_dir)
        if api_export:
            df_api = spark.read.json(local_path_for_spark(api_export))
            api_hdfs = "docker-fallback"
        else:
            df_api, api_hdfs = read_hdfs_topic_data(spark, hdfs_candidates, "api")

        if rss_export:
            df_rss = spark.read.json(local_path_for_spark(rss_export))
            rss_hdfs = "docker-fallback"
        else:
            df_rss, rss_hdfs = read_hdfs_topic_data(spark, hdfs_candidates, "rss")

        if df_api is not None and df_rss is not None:
            df = df_api.unionByName(df_rss, allowMissingColumns=True)
        elif df_api is not None:
            df = df_api
        elif df_rss is not None:
            df = df_rss
        else:
            print(f"[{datetime.now()}] ❌ Tidak ada data sama sekali")
            spark.stop()
            return
        
        hdfs_used = api_hdfs or rss_hdfs or default_hdfs
        print(f"[{datetime.now()}] 📍 Endpoint HDFS aktif: {hdfs_used}")

        total_records = df.count()
        print(f"[{datetime.now()}] ✅ Total records untuk analisis: {total_records}")
        
        if total_records == 0:
            print(f"[{datetime.now()}] ⚠️  Data kosong, tidak bisa analisis")
            spark.stop()
            return
        
        df.createOrReplaceTempView("news")
        
        print(f"[{datetime.now()}] 🔬 Mulai melakukan analisis...")
        
        # --- ANALISIS 1: Kata Paling Sering Muncul ---
        print(f"[{datetime.now()}] 📊 Analisis 1: Kata paling sering muncul...")
        try:
            stopwords = ['dan', 'yang', 'di', 'ke', 'dari', 'untuk', 'dengan', 'pada', 'ini', 'itu', 
                        'dalam', 'oleh', 'sebagai', 'akan', 'telah', 'harus', 'dapat', 'juga', 'tidak', 
                        'adalah', 'ke', 'karena', 'merupakan', 'namun', 'saat', 'kali', 'berikut']
            
            df_clean = df.withColumn("clean_title", regexp_replace(lower(col("title")), r"[^\w\s]", ""))
            df_words = df_clean.withColumn("word", explode(split(col("clean_title"), " ")))
            df_words = df_words.filter((col("word") != "") & (~col("word").isin(stopwords)) & (col("word").cast("int").isNull()))
            df_words = df_words.filter(~regexp_replace(col("word"), r"[0-9]", "").isin(["", " "]))
            
            top_words_df = df_words.groupBy("word").count().filter(col("word").cast("int").isNull()).orderBy(col("count").desc()).limit(15)
            top_words_list = [{"word": row.word, "count": int(row["count"])} for row in top_words_df.collect()]
            print(f"[{datetime.now()}] ✅ Top words: {len(top_words_list)} kata")
        except Exception as e:
            print(f"[{datetime.now()}] ⚠️  Error analisis 1: {e}")
            top_words_list = []
        
        # --- ANALISIS 2: Distribusi Berita per Sumber ---
        print(f"[{datetime.now()}] 📊 Analisis 2: Distribusi per sumber...")
        try:
            source_dist_df = spark.sql("SELECT source, COUNT(*) as total_news FROM news GROUP BY source ORDER BY total_news DESC")
            source_dist_list = [{"source": row.source, "total_news": int(row.total_news)} for row in source_dist_df.collect()]
            print(f"[{datetime.now()}] ✅ Sumber: {len(source_dist_list)} sumber")
        except Exception as e:
            print(f"[{datetime.now()}] ⚠️  Error analisis 2: {e}")
            source_dist_list = []
        
        # --- ANALISIS 3: Volume Publikasi per Jam ---
        print(f"[{datetime.now()}] 📊 Analisis 3: Volume per jam...")
        try:
            df_time = df.withColumn("jam_publikasi", hour(to_timestamp(col("timestamp"))))
            hourly_vol_df = df_time.filter(col("jam_publikasi").isNotNull()).groupBy("jam_publikasi").count().orderBy("jam_publikasi")
            hourly_vol_list = [{"jam_publikasi": row.jam_publikasi, "count": int(row["count"])} for row in hourly_vol_df.collect()]
            print(f"[{datetime.now()}] ✅ Jam publikasi: {len(hourly_vol_list)} jam berbeda")
        except Exception as e:
            print(f"[{datetime.now()}] ⚠️  Error analisis 3: {e}")
            hourly_vol_list = []
        
        # Simpan hasil
        print(f"[{datetime.now()}] 💾 Menyimpan hasil analisis...")
        results = {
            "timestamp": datetime.now().isoformat(),
            "total_records_analyzed": total_records,
            "top_words": top_words_list,
            "source_dist": source_dist_list,
            "hourly_vol": hourly_vol_list
        }
        
        # Simpan untuk Dashboard
        os.makedirs(dashboard_data_dir, exist_ok=True)
        result_file = os.path.join(dashboard_data_dir, "spark_results.json")
        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        file_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        save_result_to_hdfs(result_file, file_timestamp)
        
        print(f"[{datetime.now()}] ✅ Analisis Spark berhasil!")
        print(f"[{datetime.now()}] 📄 Hasil tersimpan di: {result_file}")
        
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error dalam analisis: {e}")
    finally:
        spark.stop()
        print(f"[{datetime.now()}] 🛑 SparkSession ditutup")

def run_continuous(interval_seconds=300):
    """
    Jalankan analisis Spark secara terus-menerus setiap `interval_seconds` detik.
    Default: 5 menit (300 detik).
    """
    import time
    iteration = 0
    print(f"[{datetime.now()}] 🔄 Mode kontinu aktif — interval {interval_seconds} detik")
    while True:
        iteration += 1
        print(f"[{datetime.now()}] ========== ITERASI #{iteration} ==========")
        try:
            run_analysis()
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Iterasi #{iteration} gagal: {e}")
        print(f"[{datetime.now()}] ⏳ Menunggu {interval_seconds} detik sebelum analisis berikutnya...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NewsPulse Spark Analysis")
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Jalankan analisis secara terus-menerus (loop)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Interval antar analisis dalam detik (default: 300 = 5 menit)"
    )
    args = parser.parse_args()

    if args.continuous:
        run_continuous(interval_seconds=args.interval)
    else:
        run_analysis()