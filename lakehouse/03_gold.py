"""
Gold Layer: Aggregation & Analytics dari Silver
Script ini menghasilkan 4 Gold tables:
1. word_freq: Top 50 kata + Top 15 printed (Repro ETS)
2. news_per_source: Distribusi berita per sumber (Repro ETS)
3. word_velocity: ENHANCED - Deteksi trending words per jam via momentum
4. cross_source_topics: ENHANCED - Topik dengan coverage multi-source (API+RSS)
Plus: Time Travel demonstration untuk perbandingan versi tabel
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, explode, split, lower, count as spark_count, 
    lag, coalesce, lit, when, desc, upper, trim,
    concat_ws, collect_list
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Indonesian stopwords
STOPWORDS_ID = [
    "yang", "dengan", "untuk", "dari", "akan", "pada", "telah",
    "dalam", "tidak", "juga", "oleh", "atau", "agar", "bisa",
    "lebih", "sudah", "saat", "kata", "bagi", "yaitu", "serta",
    "kami", "kita", "anda", "saya", "kamu", "mereka", "adalah",
    "karena", "bahwa", "sehingga", "tersebut", "hingga", "antara",
    "dan", "ke", "di", "ini", "itu", "tapi", "ini", "saja", "pun"
]

def init_spark():
    """Inisialisasi SparkSession dengan Delta Lake configuration"""
    print("=" * 80)
    print("GOLD LAYER: Initializing SparkSession with Delta Lake")
    print("=" * 80)
    
    builder = configure_spark_with_delta_pip(SparkSession.builder) \
        .appName("News Lakehouse - Gold Layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            
    spark = builder.getOrCreate()
    print("✓ SparkSession initialized")
    print(f"✓ Spark version: {spark.version}")
    return spark

def read_silver_layer(spark):
    """Baca data dari Silver layer"""
    print("\n" + "=" * 80)
    print("READING SILVER LAYER")
    print("=" * 80)
    
    try:
        silver_path = "./lakehouse_data/silver/news"
        df_silver = spark.read.format("delta").load(silver_path)
        record_count = df_silver.count()
        print(f"✓ Read Silver data: {record_count} records")
        return df_silver
    except Exception as e:
        print(f"✗ Error reading Silver layer: {e}")
        return None

def gold_table_1_word_freq(spark, df_silver):
    """
    GOLD TABLE 1: Word Frequency
    Reproduksi ETS lama: Top 15 kata dari judul
    Simpan top 50, print top 15
    """
    print("\n" + "=" * 80)
    print("GOLD TABLE 1: Word Frequency (Top 50 saved, Top 15 printed)")
    print("=" * 80)
    
    try:
        # Explode title_clean menjadi individual kata
        words_df = df_silver.withColumn(
            "word", explode(split(col("title_clean"), " "))
        )
        
        # Filter: panjang >= 4 karakter, hanya huruf [a-z]{4,}
        words_df = words_df.filter(
            col("word").rlike("^[a-z]{4,}$")
        )
        
        # Filter stopwords Indonesia
        words_df = words_df.filter(
            ~col("word").isin(STOPWORDS_ID)
        )
        
        # Group by word dan count
        word_freq_df = words_df.groupBy("word").agg(
            spark_count("*").alias("count")
        ).orderBy(desc("count"))
        
        # Simpan top 50
        gold_path = "./lakehouse_data/gold/word_freq"
        word_freq_df.limit(50).write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)
        
        print(f"✓ Saved top 50 words to: {gold_path}")
        
        # Print top 15
        print("\n📊 TOP 15 WORDS BY FREQUENCY:")
        word_freq_df.limit(15).show(15, truncate=False)
        
        return word_freq_df
    
    except Exception as e:
        print(f"✗ Error in gold_table_1: {e}")
        return None

def gold_table_2_news_per_source(spark, df_silver):
    """
    GOLD TABLE 2: News per Source
    Reproduksi ETS lama: Distribusi berita per sumber
    """
    print("\n" + "=" * 80)
    print("GOLD TABLE 2: News per Source Distribution")
    print("=" * 80)
    
    try:
        # Group by source_normalized dan count
        source_dist_df = df_silver.groupBy("source_normalized").agg(
            spark_count("*").alias("total_news")
        ).orderBy(desc("total_news"))
        
        # Simpan
        gold_path = "./lakehouse_data/gold/news_per_source"
        source_dist_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)
        
        print(f"✓ Saved to: {gold_path}")
        
        # Print semua sumber
        print("\n📊 NEWS DISTRIBUTION BY SOURCE:")
        source_dist_df.show(50, truncate=False)
        
        return source_dist_df
    
    except Exception as e:
        print(f"✗ Error in gold_table_2: {e}")
        return None

def gold_table_3_word_velocity(spark, df_silver):
    """
    GOLD TABLE 3: Word Velocity (ENHANCED)
    Deteksi kata dengan kecepatan naik tertinggi per jam
    Tujuan: Trending words based on momentum (bukan hanya total volume)
    """
    print("\n" + "=" * 80)
    print("GOLD TABLE 3: Word Velocity (Trending Words by Momentum)")
    print("=" * 80)
    
    try:
        # Step 1: Explode kata per jam
        print("\n→ Step 1: Exploding words by hour...")
        words_hourly = df_silver.withColumn(
            "word", explode(split(col("title_clean"), " "))
        ).filter(
            col("word").rlike("^[a-z]{4,}$")
        ).filter(
            ~col("word").isin(STOPWORDS_ID)
        ).groupBy("jam", "word").agg(
            spark_count("*").alias("freq_per_jam")
        )
        
        print(f"✓ Created hourly word frequency: {words_hourly.count()} rows")
        
        # Step 2: Window untuk lag per kata (previous hour)
        print("→ Step 2: Computing velocity with lag window...")
        window_word = Window.partitionBy("word").orderBy("jam")
        
        word_velocity_df = words_hourly \
            .withColumn("prev_freq", lag("freq_per_jam", 1).over(window_word)) \
            .withColumn("velocity",
                col("freq_per_jam") - coalesce(col("prev_freq"), lit(0))
            ).withColumn("velocity_pct",
                when(col("prev_freq") > 0,
                    (col("freq_per_jam") - col("prev_freq")) / col("prev_freq") * 100
                ).otherwise(lit(100.0))
            ).orderBy(desc("velocity"))
        
        print("✓ Computed velocity and velocity_pct")
        
        # Step 3: Simpan semua rows
        gold_path = "./lakehouse_data/gold/word_velocity"
        word_velocity_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)
        
        print(f"✓ Saved to: {gold_path}")
        
        # Step 4: Print top 10 trending words
        print("\n📊 TOP 10 WORDS WITH HIGHEST VELOCITY (Trending):")
        word_velocity_df.select(
            "word", "jam", "freq_per_jam", "prev_freq", "velocity", "velocity_pct"
        ).limit(10).show(10, truncate=False)
        
        return word_velocity_df
    
    except Exception as e:
        print(f"✗ Error in gold_table_3: {e}")
        return None

def gold_table_4_cross_source_topics(spark, df_silver):
    """
    GOLD TABLE 4: Cross-Source Topics (ENHANCED)
    Deteksi topik (kata) yang muncul di API dan RSS dalam waktu berdekatan.
    Mengidentifikasi breaking news dengan coverage konsisten di semua sumber.
    """
    print("\n" + "=" * 80)
    print("GOLD TABLE 4: Cross-Source Topics (Unified Coverage)")
    print("=" * 80)
    
    try:
        # Step 1: Explode kata per jam per source
        print("\n→ Step 1: Extracting words per source and hour...")
        words_by_source = df_silver.withColumn(
            "word", explode(split(col("title_clean"), " "))
        ).filter(
            col("word").rlike("^[a-z]{4,}$")
        ).filter(
            ~col("word").isin(STOPWORDS_ID)
        ).select("jam", "word", "_source").distinct()
        
        # Step 2: Pivot untuk cari kata yang ada di multiple sources per jam
        print("→ Step 2: Identifying cross-source word occurrences...")
        
        # Count berapa source yang mention setiap kata per jam
        words_by_source_agg = words_by_source.groupBy("jam", "word").agg(
            spark_count("_source").alias("num_sources"),
            concat_ws(",", collect_list("_source")).alias("sources")
        ).filter(col("num_sources") > 1)  # Hanya kata yang muncul di 2+ source
        
        # Step 3: Hitung total frekuensi kata cross-source di setiap jam
        print("→ Step 3: Computing cross-source frequency...")
        cross_source_freq = df_silver.withColumn(
            "word", explode(split(col("title_clean"), " "))
        ).filter(
            col("word").rlike("^[a-z]{4,}$")
        ).filter(
            ~col("word").isin(STOPWORDS_ID)
        ).select("jam", "word", "_source").join(
            words_by_source_agg,
            on=["jam", "word"],
            how="inner"
        ).groupBy("jam", "word", "sources").agg(
            spark_count("*").alias("cross_source_mentions")
        ).orderBy(desc("cross_source_mentions"), desc("jam"))
        
        # Step 4: Simpan ke Delta
        gold_path = "./lakehouse_data/gold/cross_source_topics"
        cross_source_freq.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)
        
        print(f"✓ Saved to: {gold_path}")
        
        # Step 5: Print top cross-source topics
        print("\n📊 TOP CROSS-SOURCE TOPICS (Mentioned in 2+ sources):")
        print("(Breaking news dengan coverage di API dan RSS bersamaan)\n")
        cross_source_freq.limit(15).show(15, truncate=False)
        
        return cross_source_freq
    
    except Exception as e:
        print(f"✗ Error in gold_table_4: {e}")
        return None

def time_travel_demo(spark):
    """
    TIME TRAVEL DEMONSTRATION
    Menunjukkan capability Delta Lake: versioning & time travel
    """
    print("\n" + "=" * 80)
    print("TIME TRAVEL DEMONSTRATION")
    print("=" * 80)
    
    try:
        silver_path = "./lakehouse_data/silver/news"
        deltaTable = DeltaTable.forPath(spark, silver_path)
        
        # Step 1: Tampilkan history
        print("\n→ Step 1: Table History")
        print("=" * 80)
        print("=== SILVER TABLE HISTORY ===")
        history_df = deltaTable.history()
        history_df.select("version", "timestamp", "operation", "operationParameters").show(10, truncate=False)
        
        # Step 2: Update NULL source_normalized ke "UNKNOWN"
        print("\n→ Step 2: Performing Update Operation")
        print("=" * 80)
        print("Updating: SET source_normalized = 'UNKNOWN' where NULL/empty")
        
        update_count = deltaTable.update(
            condition="source_normalized IS NULL OR source_normalized = ''",
            set={"source_normalized": lit("UNKNOWN")}
        )
        print(f"✓ Update completed")
        
        # Step 3: Bandingkan versi sekarang vs sebelum
        print("\n→ Step 3: Comparing Versions")
        print("=" * 80)
        
        print("\n=== SOURCE DISTRIBUTION - CURRENT VERSION ===")
        spark.read.format("delta").load(silver_path) \
            .groupBy("source_normalized").agg(spark_count("*").alias("count")) \
            .orderBy(desc("count")).show(20, truncate=False)
        
        print("\n=== SOURCE DISTRIBUTION - VERSION 0 (BEFORE UPDATE) ===")
        try:
            spark.read.format("delta") \
                .option("versionAsOf", 0).load(silver_path) \
                .groupBy("source_normalized").agg(spark_count("*").alias("count")) \
                .orderBy(desc("count")).show(20, truncate=False)
        except Exception as e:
            print(f"⚠ Cannot read version 0 (this is the first write): {e}")
        
        print("\n✓ TIME TRAVEL DEMONSTRATION COMPLETED")
    
    except Exception as e:
        print(f"✗ Error in time_travel_demo: {e}")

def main():
    # Initialize Spark
    spark = init_spark()
    
    try:
        # Read Silver
        df_silver = read_silver_layer(spark)
        
        if df_silver is None:
            print("✗ GOLD LAYER PROCESSING FAILED: No Silver data")
            return
        
        # Generate Gold Tables
        gold_table_1_word_freq(spark, df_silver)
        gold_table_2_news_per_source(spark, df_silver)
        gold_table_3_word_velocity(spark, df_silver)
        gold_table_4_cross_source_topics(spark, df_silver)
        
        # Time Travel Demonstration
        time_travel_demo(spark)
        
        # Final Summary
        print("\n" + "=" * 80)
        print("✓ GOLD LAYER GENERATION COMPLETED")
        print("=" * 80)
        print("\nGenerated Gold Tables:")
        print("  1. ./lakehouse_data/gold/word_freq")
        print("     - Top 50 kata dengan frekuensi tertinggi")
        print("     - Top 15 ditampilkan di atas")
        print("\n  2. ./lakehouse_data/gold/news_per_source")
        print("     - Distribusi berita per sumber media")
        print("\n  3. ./lakehouse_data/gold/word_velocity")
        print("     - Trending words based on momentum per jam")
        print("     - Top 10 ditampilkan di atas")
        print("\n  4. ./lakehouse_data/gold/cross_source_topics")
        print("     - Topik yang coverage-nya konsisten di API & RSS")
        print("     - Breaking news dengan sumber terpercaya (multi-source)")
        print("\n" + "=" * 80)
    
    except Exception as e:
        print(f"\n✗ CRITICAL ERROR: {e}")
        raise
    
    finally:
        spark.stop()
        print("\n✓ SparkSession stopped")

if __name__ == "__main__":
    main()
