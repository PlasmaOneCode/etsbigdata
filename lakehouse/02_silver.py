"""
Silver Layer: Cleaning & Transformation dari Bronze ke Silver
Script ini melakukan 5 transformasi cleaning dan menyimpan hasil ke Silver layer.
Transformasi: duplikat, null/empty, timestamp parsing, text normalization, source standardization
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, upper, trim, split, size, to_timestamp, hour,
    to_date, dayofweek, count as spark_count, when,
    regexp_extract, concat_ws, lpad, coalesce
)
from delta import configure_spark_with_delta_pip

def init_spark():
    """Inisialisasi SparkSession dengan Delta Lake configuration"""
    print("=" * 80)
    print("SILVER LAYER: Initializing SparkSession with Delta Lake")
    print("=" * 80)
    
    builder = configure_spark_with_delta_pip(SparkSession.builder) \
        .appName("News Lakehouse - Silver Layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            
    spark = builder.getOrCreate()
    print("✓ SparkSession initialized")
    print(f"✓ Spark version: {spark.version}")
    return spark

def read_bronze_layer(spark):
    """Baca data dari Bronze layer (API & RSS)"""
    print("\n" + "=" * 80)
    print("READING BRONZE LAYER")
    print("=" * 80)
    
    try:
        df_api = spark.read.format("delta").load("./lakehouse_data/bronze/news_api")
        api_count = df_api.count()
        print(f"✓ Read API data from Bronze: {api_count} records")
    except Exception as e:
        print(f"⚠ Warning: Cannot read API Bronze: {e}")
        df_api = None
        api_count = 0
    
    try:
        df_rss = spark.read.format("delta").load("./lakehouse_data/bronze/news_rss")
        rss_count = df_rss.count()
        print(f"✓ Read RSS data from Bronze: {rss_count} records")
    except Exception as e:
        print(f"⚠ Warning: Cannot read RSS Bronze: {e}")
        df_rss = None
        rss_count = 0
    
    # Gabung API & RSS menggunakan unionByName
    if df_api is not None and df_rss is not None:
        df_bronze = df_api.unionByName(df_rss, allowMissingColumns=True)
    elif df_api is not None:
        df_bronze = df_api
    elif df_rss is not None:
        df_bronze = df_rss
    else:
        print("✗ No Bronze data available")
        return None, api_count + rss_count
    
    total_count = df_bronze.count()
    print(f"✓ Total records before cleaning: {total_count}")
    
    return df_bronze, total_count

def transformation_1_remove_duplicates(df):
    """
    TRANSFORMASI 1: Hapus duplikat berdasarkan title & url
    """
    print("\n" + "-" * 80)
    print("TRANSFORMATION 1: Removing Duplicates")
    print("-" * 80)
    
    count_before = df.count()
    df_clean = df.dropDuplicates(["title", "url"])
    count_after = df_clean.count()
    
    duplicates_removed = count_before - count_after
    print(f"→ Rows before: {count_before}")
    print(f"→ Rows after: {count_after}")
    print(f"→ Duplicates removed: {duplicates_removed}")
    
    return df_clean, duplicates_removed

def transformation_2_filter_null_empty(df):
    """
    TRANSFORMASI 2: Filter null & empty values
    - title tidak boleh NULL atau kosong ("")
    - timestamp tidak boleh NULL atau kosong
    """
    print("\n" + "-" * 80)
    print("TRANSFORMATION 2: Filtering Null & Empty Values")
    print("-" * 80)
    
    count_before = df.count()
    
    # Filter: title dan timestamp tidak null, title tidak kosong
    df_clean = df.filter(
        (col("title").isNotNull()) & 
        (col("timestamp").isNotNull()) &
        (col("title") != "")
    )
    
    count_after = df_clean.count()
    null_removed = count_before - count_after
    
    print(f"→ Rows before: {count_before}")
    print(f"→ Rows after: {count_after}")
    print(f"→ Null/Empty rows removed: {null_removed}")
    
    return df_clean, null_removed

def transformation_3_parse_timestamp(df):
    """
    TRANSFORMASI 3: Parse & ekstrak timestamp dengan aman menggunakan Regex Parser
    - parsed_timestamp: konversi string timestamp ke TIMESTAMP format
    - jam: ekstrak jam (0-23)
    - tanggal: ekstrak tipe Date
    - hari_dalam_minggu: hari dalam seminggu (1=Ahad, 7=Sabtu)
    """
    print("\n" + "-" * 80)
    print("TRANSFORMATION 3: Parsing & Extracting Timestamp (Robust Multi-Format)")
    print("-" * 80)
    
    # Pola regex untuk mengekstrak format RFC 822 (RSS): "Mon, 4 May 2026 13:26:26 +0700"
    rss_pattern = r"^([A-Za-z]{3}),\s+(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s+(\d{2}:\d{2}:\d{2})\s+([+-]\d{4})"
    
    day_col = regexp_extract(col("timestamp"), rss_pattern, 2)
    month_col = regexp_extract(col("timestamp"), rss_pattern, 3)
    year_col = regexp_extract(col("timestamp"), rss_pattern, 4)
    time_col = regexp_extract(col("timestamp"), rss_pattern, 5)
    offset_col = regexp_extract(col("timestamp"), rss_pattern, 6)
    
    # Mapping nama bulan Indonesia & Inggris ke format angka "01"-"12"
    month_num = when(month_col == "Jan", "01") \
               .when(month_col == "Feb", "02") \
               .when(month_col == "Mar", "03") \
               .when(month_col == "Apr", "04") \
               .when(month_col == "May", "05") \
               .when(month_col == "Mei", "05") \
               .when(month_col == "Jun", "06") \
               .when(month_col == "Jul", "07") \
               .when(month_col == "Aug", "08") \
               .when(month_col == "Ago", "08") \
               .when(month_col == "Sep", "09") \
               .when(month_col == "Oct", "10") \
               .when(month_col == "Okt", "10") \
               .when(month_col == "Nov", "11") \
               .when(month_col == "Dec", "12") \
               .when(month_col == "Des", "12") \
               .otherwise("01")
               
    # Rekonstruksi ke string tanggal standar: "yyyy-MM-dd HH:mm:ss Z"
    rss_formatted_str = concat_ws(
        " ",
        concat_ws("-", year_col, month_num, lpad(day_col, 2, "0")),
        time_col,
        offset_col
    )
    
    # Gunakan coalesce untuk mencoba parser ISO-8601 (API) kemudian parser Kustom (RSS)
    df_parsed = df.withColumn(
        "parsed_timestamp",
        coalesce(
            to_timestamp(col("timestamp")),  # Coba parser default ISO (untuk API)
            to_timestamp(rss_formatted_str, "yyyy-MM-dd HH:mm:ss Z")  # Parser kustom (untuk RSS)
        )
    ).withColumn(
        "jam", hour(col("parsed_timestamp"))
    ).withColumn(
        "tanggal", to_date(col("parsed_timestamp"))
    ).withColumn(
        "hari_dalam_minggu", dayofweek(col("parsed_timestamp"))
    )
    
    # Filter kualitas data: Hapus baris yang gagal di-parse (opsional, sebagai pengaman)
    count_before = df_parsed.count()
    df_clean = df_parsed.filter(col("parsed_timestamp").isNotNull())
    count_after = df_clean.count()
    unparseable_removed = count_before - count_after
    
    print(f"→ Rows before parsing: {count_before}")
    print(f"→ Rows after parsing: {count_after}")
    print(f"→ Unparseable timestamps removed: {unparseable_removed}")
    
    print("\nSample parsed timestamp values:")
    df_clean.select("timestamp", "parsed_timestamp", "jam", "tanggal", 
                    "hari_dalam_minggu").show(10, truncate=False)
    
    return df_clean

def transformation_4_normalize_text(df):
    """
    TRANSFORMASI 4: Normalisasi teks judul
    - title_clean: lowercase & trim
    - title_word_count: jumlah kata di judul
    """
    print("\n" + "-" * 80)
    print("TRANSFORMATION 4: Normalizing Text")
    print("-" * 80)
    
    df_clean = df.withColumn(
        "title_clean", lower(trim(col("title")))
    ).withColumn(
        "title_word_count", size(split(col("title_clean"), " "))
    )
    
    print("✓ New columns added: title_clean, title_word_count")
    print("\nSample text normalization:")
    df_clean.select("title", "title_clean", "title_word_count").show(5, truncate=False)
    
    return df_clean

def transformation_5_standardize_source(df):
    """
    TRANSFORMASI 5: Standarisasi nama sumber
    - source_normalized: uppercase & trim
    """
    print("\n" + "-" * 80)
    print("TRANSFORMATION 5: Standardizing Source Names")
    print("-" * 80)
    
    df_clean = df.withColumn(
        "source_normalized", upper(trim(col("source")))
    )
    
    print("✓ New column added: source_normalized")
    print("\nUnique sources after standardization:")
    df_clean.select("source_normalized").distinct().show(10, truncate=False)
    
    return df_clean

def main():
    # Initialize Spark
    spark = init_spark()
    
    try:
        # Baca Bronze
        df_bronze, total_before = read_bronze_layer(spark)
        
        if df_bronze is None:
            print("✗ SILVER LAYER PROCESSING FAILED: No Bronze data")
            return
        
        # Transformasi 1: Remove Duplicates
        df_silver = df_bronze
        df_silver, dup_removed = transformation_1_remove_duplicates(df_silver)
        
        # Transformasi 2: Filter Null & Empty
        df_silver, null_removed = transformation_2_filter_null_empty(df_silver)
        
        # Transformasi 3: Parse Timestamp
        df_silver = transformation_3_parse_timestamp(df_silver)
        
        # Transformasi 4: Normalize Text
        df_silver = transformation_4_normalize_text(df_silver)
        
        # Transformasi 5: Standardize Source
        df_silver = transformation_5_standardize_source(df_silver)
        
        # Display Final Summary
        print("\n" + "=" * 80)
        print("SILVER LAYER TRANSFORMATION SUMMARY")
        print("=" * 80)
        
        total_after = df_silver.count()
        total_removed = total_before - total_after
        removal_pct = (total_removed / total_before * 100) if total_before > 0 else 0
        
        print(f"\n📊 STATISTICS:")
        print(f"   Total records BEFORE cleaning: {total_before}")
        print(f"   Total records AFTER cleaning: {total_after}")
        print(f"   Total records removed: {total_removed} ({removal_pct:.2f}%)")
        print(f"   - Duplicates: {dup_removed}")
        print(f"   - Null/Empty: {null_removed}")
        
        # Display Schema
        print(f"\n📋 SILVER SCHEMA:")
        df_silver.printSchema()
        
        # Display Sample
        print(f"\n📊 SAMPLE DATA (5 rows):")
        df_silver.show(5, truncate=False)
        
        # Simpan ke Silver Layer
        silver_path = "./lakehouse_data/silver/news"
        print(f"\n→ Writing to Silver: {silver_path}")
        
        # Solusi schema mismatch: Tambahkan opsi overwriteSchema ke "true"
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        
        print("\n" + "=" * 80)
        print("✓ SILVER LAYER TRANSFORMATION COMPLETED")
        print("=" * 80)
        print(f"\nSilver table saved at: {silver_path}")
        print("Next step: python lakehouse/03_gold.py")
    
    except Exception as e:
        print(f"\n✗ CRITICAL ERROR: {e}")
        raise
    
    finally:
        spark.stop()
        print("\n✓ SparkSession stopped")

if __name__ == "__main__":
    main()