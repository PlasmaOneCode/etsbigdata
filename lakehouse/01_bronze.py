"""
Bronze Layer: Ingest data dari HDFS ke Delta Lake
Script ini membaca data JSON dari HDFS (API & RSS sources) dan menyimpannya ke Bronze layer.
Urutan prioritas sumber data:
  1. Auto-export dari HDFS via Docker (jika Docker aktif)
  2. Fallback ke file lokal lama jika Docker tidak aktif
"""

import os
import subprocess
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from delta import configure_spark_with_delta_pip


def init_spark():
    """Inisialisasi SparkSession dengan Delta Lake configuration"""
    print("=" * 80)
    print("BRONZE LAYER: Initializing SparkSession with Delta Lake")
    print("=" * 80)

    builder = configure_spark_with_delta_pip(SparkSession.builder) \
        .appName("News Lakehouse - Bronze Layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = builder.getOrCreate()
    print("✓ SparkSession initialized")
    print(f"✓ Spark version: {spark.version}")
    return spark


def is_docker_running():
    """Cek apakah Docker dan container namenode sedang aktif"""
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format={{.State.Running}}", "namenode"],
            capture_output=True, text=True, timeout=5
        )
        return result.returncode == 0 and "true" in result.stdout
    except Exception:
        return False


def export_hdfs_to_local(hdfs_path, local_output_path, label="data"):
    """
    Auto-export data dari HDFS ke folder lokal via docker exec + docker cp.
    Dipanggil otomatis setiap kali script dijalankan selama Docker aktif.

    Args:
        hdfs_path     : path di dalam HDFS, contoh /data/news/api
        local_output_path : path lokal tujuan, contoh ./news_data_local/api/
        label         : label untuk logging (api/rss)

    Returns:
        local_output_path jika berhasil, None jika gagal
    """
    try:
        print(f"\n→ Auto-exporting {label} from HDFS: {hdfs_path}")

        # Path sementara di dalam container
        container_tmp = f"/tmp/bronze_export_{label}"

        # Hapus folder tmp lama di container jika ada
        subprocess.run(
            ["docker", "exec", "namenode", "rm", "-rf", container_tmp],
            capture_output=True
        )

        # Cek apakah path HDFS ada isinya
        check = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", hdfs_path],
            capture_output=True, text=True
        )
        if check.returncode != 0:
            print(f"⚠ HDFS path tidak ditemukan atau kosong: {hdfs_path}")
            return None

        # Export dari HDFS ke folder tmp di dalam container
        get_result = subprocess.run(
            ["docker", "exec", "namenode",
             "hdfs", "dfs", "-get", hdfs_path, container_tmp],
            capture_output=True, text=True
        )
        if get_result.returncode != 0:
            print(f"⚠ hdfs dfs -get gagal: {get_result.stderr}")
            return None

        # Hapus folder lokal lama agar tidak menumpuk data lama
        if os.path.exists(local_output_path):
            shutil.rmtree(local_output_path)
        os.makedirs(local_output_path, exist_ok=True)

        # Copy dari container ke Windows
        cp_result = subprocess.run(
            ["docker", "cp", f"namenode:{container_tmp}/.", local_output_path],
            capture_output=True, text=True
        )
        if cp_result.returncode != 0:
            print(f"⚠ docker cp gagal: {cp_result.stderr}")
            return None

        # Hitung jumlah file yang berhasil di-export
        files = [f for f in os.listdir(local_output_path)
                 if f.endswith(".json")]
        print(f"✓ Auto-export berhasil: {len(files)} file JSON → {local_output_path}")
        return local_output_path

    except subprocess.TimeoutExpired:
        print("⚠ Docker timeout — mungkin Docker tidak aktif")
        return None
    except Exception as e:
        print(f"⚠ Auto-export error: {e}")
        return None


def read_from_local(spark, path):
    """
    Baca data JSON dari file/folder lokal.
    Returns: DataFrame atau None jika gagal
    """
    try:
        print(f"\n→ Reading from local: {path}")
        if not os.path.exists(path):
            print(f"✗ Path not found: {path}")
            return None

        df = spark.read \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .json(path)
        count = df.count()
        print(f"✓ Successfully read {count} records from local")
        return df
    except Exception as e:
        print(f"✗ Error reading from local: {e}")
        return None


def add_metadata(df, source_type):
    """
    Tambahkan kolom metadata ke dataframe:
    - _ingested_at : timestamp saat ingest
    - _source      : 'api' atau 'rss'
    """
    return df.withColumn("_ingested_at", current_timestamp()) \
             .withColumn("_source", lit(source_type))


def ingest_source(spark, label, hdfs_path, local_export_path,
                  fallback_path, bronze_output_path):
    """
    Generik ingest untuk satu sumber (API atau RSS).
    Urutan:
      1. Coba auto-export dari HDFS (jika Docker aktif)
      2. Fallback ke file lokal lama
    """
    print("\n" + "=" * 80)
    print(f"INGESTING NEWS {label.upper()}")
    print("=" * 80)

    df = None

    # --- Langkah 1: Auto-export dari HDFS jika Docker aktif ---
    if is_docker_running():
        exported_path = export_hdfs_to_local(hdfs_path, local_export_path, label)
        if exported_path:
            df = read_from_local(spark, exported_path)
    else:
        print("⚠ Docker tidak aktif, skip auto-export")

    # --- Langkah 2: Fallback ke file lama ---
    if df is None:
        print(f"\n⚠ Menggunakan fallback: {fallback_path}")
        df = read_from_local(spark, fallback_path)

    if df is None:
        print(f"✗ Tidak ada data untuk {label}")
        return None

    # Tambah metadata
    df = add_metadata(df, label)

    # Validasi schema
    expected_cols = {"title", "source", "url", "summary", "timestamp"}
    actual_cols = set(df.columns) - {"_ingested_at", "_source"}
    if expected_cols.issubset(actual_cols):
        print("✓ Schema validation passed")
    else:
        print(f"⚠ Schema warning: expected {expected_cols}, got {actual_cols}")

    # Simpan ke Bronze Delta Layer
    print(f"\n→ Writing to Bronze: {bronze_output_path}")
    try:
        df.write.format("delta").mode("append").save(bronze_output_path)
        print(f"✓ Successfully wrote {df.count()} records to Bronze")
    except Exception as e:
        print(f"✗ Error writing to Bronze: {e}")
        return None

    return df


def display_summary(df_api, df_rss):
    """Tampilkan ringkasan hasil ingestion"""
    print("\n" + "=" * 80)
    print("INGESTION SUMMARY")
    print("=" * 80)

    for label, df in [("API", df_api), ("RSS", df_rss)]:
        if df is not None:
            print(f"\n📊 {label} Records Ingested: {df.count()}")
            df.printSchema()
            df.show(3, truncate=False)
        else:
            print(f"\n⚠ No {label} data ingested")


def main():
    spark = init_spark()

    try:
        # Ingest API
        df_api = ingest_source(
            spark,
            label="api",
            hdfs_path="/data/news/api",
            local_export_path="./news_data_local/api/",
            fallback_path="./news-api_2026-04-27_11-57.json",
            bronze_output_path="./lakehouse_data/bronze/news_api"
        )

        # Ingest RSS
        df_rss = ingest_source(
            spark,
            label="rss",
            hdfs_path="/data/news/rss",
            local_export_path="./news_data_local/rss/",
            fallback_path=None,  # Tidak ada fallback lokal untuk RSS
            bronze_output_path="./lakehouse_data/bronze/news_rss"
        )

        if df_api is not None or df_rss is not None:
            display_summary(df_api, df_rss)
            print("\n" + "=" * 80)
            print("✓ BRONZE LAYER INGESTION COMPLETED")
            print("=" * 80)
            print("\nNext step: python lakehouse/02_silver.py")
        else:
            print("\n✗ INGESTION FAILED: No data available")

    except Exception as e:
        print(f"\n✗ CRITICAL ERROR: {e}")
        raise

    finally:
        spark.stop()
        print("\n✓ SparkSession stopped")


if __name__ == "__main__":
    main()
