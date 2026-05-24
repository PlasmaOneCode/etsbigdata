# README Lakehouse — NewsPulse (Kelompok 5)

**Mata Kuliah:** Big Data dan Data Lakehouse — ITS 2026  
**Tugas:** Lanjutan ETS — Upgrade Pipeline ke Delta Lake Lakehouse  
**Topik:** Analisis Tren Berita Nasional  

---

## Daftar Isi
1. [Prasyarat & Instalasi](#1-prasyarat--instalasi)
2. [Cara Menjalankan](#2-cara-menjalankan)
3. [Diagram Arsitektur](#3-diagram-arsitektur)
4. [Penjelasan Transformasi Silver](#4-penjelasan-transformasi-silver)
5. [Perbandingan Gold vs ETS Lama](#5-perbandingan-gold-vs-ets-lama)
6. [Screenshot Output](#6-screenshot-output)
7. [Refleksi: Delta Lake vs HDFS/CSV](#7-refleksi-delta-lake-vs-hdfscsv)
8. [Struktur Folder](#8-struktur-folder)

---

## 1. Prasyarat & Instalasi

### 1.1 Prasyarat Umum
- Python 3.8+
- Java JDK 11+ (wajib untuk PySpark)
- Docker Desktop (untuk Kafka + HDFS)
- Git

### 1.2 Khusus Windows — Install winutils (WAJIB)

PySpark di Windows membutuhkan file `winutils.exe` agar bisa berjalan secara lokal.
Tanpa ini, Spark akan crash dengan error `HADOOP_HOME and hadoop.home.dir are unset`.

**Step 1 — Download dua file berikut:**
```
https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe
https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll
```

**Step 2 — Buat folder dan taruh kedua file di sana:**
```
C:\hadoop\bin\winutils.exe
C:\hadoop\bin\hadoop.dll
```

**Step 3 — Set Environment Variable:**  
Buka *Start Menu → Edit the system environment variables → Environment Variables*

- Di **User variables**, klik **New**:
  ```
  Variable name : HADOOP_HOME
  Variable value: C:\hadoop
  ```
- Di **User variables**, klik **Path → Edit → New**, tambahkan:
  ```
  C:\hadoop\bin
  ```

**Step 4 — Restart terminal / VS Code.**

### 1.3 Install Python Dependencies

```bash
pip install delta-spark==3.1.0
pip install pyspark==3.5.0
```

Atau install semua sekaligus dari root repo:
```bash
pip install -r requirements.txt
pip install delta-spark==3.1.0
```

### 1.4 Verifikasi Instalasi

```bash
python -c "from delta import configure_spark_with_delta_pip; print('Delta OK')"
python -c "import pyspark; print('PySpark', pyspark.__version__)"
```

---

## 2. Cara Menjalankan

### 2.1 Persiapan — Jalankan Docker

Pastikan pipeline ETS sudah berjalan dan data sudah masuk ke HDFS:

```bash
# Start semua container
docker-compose up -d

# Verifikasi container aktif (harus ada: namenode, datanode, kafka-broker, zookeeper)
docker ps

# Jalankan producer untuk mengumpulkan data (~10 menit)
cd kafka
python producer_api.py   # Terminal 1
python producer_rss.py   # Terminal 2

# Jalankan consumer agar data masuk HDFS
cd hdfs
python consumer_to_hdfs.py   # Terminal 3
```

### 2.2 Jalankan Pipeline Lakehouse

Setelah data ada di HDFS, jalankan ketiga script secara berurutan dari root folder repo:

```bash
# Step 1 — Ingest HDFS → Bronze Layer (otomatis export dari Docker)
python lakehouse/01_bronze.py

# Step 2 — Cleaning Bronze → Silver Layer
python lakehouse/02_silver.py

# Step 3 — Agregasi Silver → Gold Layer + Time Travel Demo
python lakehouse/03_gold.py
```

### 2.3 Catatan Penting

| Kondisi | Yang Terjadi |
|---------|-------------|
| Docker aktif + HDFS berisi data | Bronze otomatis export data terbaru dari HDFS |
| Docker mati | Bronze fallback ke file lokal `news-api_2026-04-27_11-57.json` |
| Bronze belum ada | Silver akan gagal — jalankan Bronze dulu |
| Silver belum ada | Gold akan gagal — jalankan Silver dulu |

Script 01_bronze.py menggunakan mode **append** — aman dijalankan berkali-kali.  
Script 02 dan 03 menggunakan mode **overwrite** — selalu regenerate dari awal.

### 2.4 Output yang Dihasilkan

```
lakehouse_data/
├── bronze/
│   ├── news_api/        ← Data API mentah (Delta format)
│   └── news_rss/        ← Data RSS mentah (Delta format)
├── silver/
│   └── news/            ← Data bersih API+RSS (Delta format)
└── gold/
    ├── word_freq/        ← Top 50 kata (Repro ETS)
    ├── news_per_source/  ← Distribusi per sumber (Repro ETS)
    ├── word_velocity/    ← Trending words per jam (Enhanced)
    └── cross_source_topics/ ← Topik multi-source (Enhanced)
```

---

## 3. Diagram Arsitektur

### Sebelum — Pipeline ETS Lama

```
┌─────────────────────────────────────────────────────────────┐
│                  NEWS PIPELINE (ETS LAMA)                   │
└─────────────────────────────────────────────────────────────┘

  [GNews API]          [RSS Feed: Kompas, Tempo]
       │                          │
       └──────────┬───────────────┘
                  │
           [Kafka Broker]
            localhost:9092
                  │
       ┌──────────┴──────────┐
       │                     │
  [HDFS JSON]          [HDFS JSON]
  /data/news/api/      /data/news/rss/
       │                     │
       └──────────┬───────────┘
                  │
          [Spark analysis.py]
          (baca JSON mentah langsung)
                  │
       ┌──────────┼──────────┐
       │          │          │
  Word Freq   Source Dist  Hourly Vol
       │          │          │
       └──────────┴──────────┘
                  │
       [spark_results.json]
                  │
        [Flask Dashboard]
```

**Masalah ETS lama:**
- Data JSON mentah tanpa schema enforcement
- Duplikat tidak terdeteksi → analisis bias
- Tidak ada versioning → tidak bisa rollback
- Timestamp masih string → analisis temporal terbatas
- Tidak ada ACID → data bisa corrupt jika Spark crash

---

### Sesudah — Pipeline Dengan Delta Lake Lakehouse

```
┌─────────────────────────────────────────────────────────────┐
│             NEWS PIPELINE (DENGAN DELTA LAKE)               │
└─────────────────────────────────────────────────────────────┘

  [GNews API]          [RSS Feed: Kompas, Tempo]
       │                          │
       └──────────┬───────────────┘
                  │
           [Kafka Broker]
            localhost:9092
                  │
       ┌──────────┴──────────┐
       │                     │
  [HDFS JSON]          [HDFS JSON]
  /data/news/api/      /data/news/rss/
       │                     │
       └──────────┬───────────┘
                  │
    ┌─────────────────────────┐
    │      BRONZE LAYER       │
    │  Raw ingest + metadata  │
    │  Mode: APPEND           │
    │  ./bronze/news_api      │
    │  ./bronze/news_rss      │
    └────────────┬────────────┘
                 │
    ┌─────────────────────────┐
    │      SILVER LAYER       │
    │  5 transformasi cleaning│
    │  Mode: OVERWRITE        │
    │  ./silver/news          │
    └────────────┬────────────┘
                 │
    ┌─────────────────────────┐
    │       GOLD LAYER        │
    │  4 tabel analitik       │
    │  Mode: OVERWRITE        │
    │  ./gold/word_freq       │
    │  ./gold/news_per_source │
    │  ./gold/word_velocity   │
    │  ./gold/cross_source    │
    └────────────┬────────────┘
                 │
    ┌─────────────────────────┐
    │    DELTA TIME TRAVEL    │
    │  Versioning & audit log │
    │  Query versi manapun    │
    └────────────┬────────────┘
                 │
        [Flask Dashboard v2]
        (baca dari Gold Delta)
```

---

## 4. Penjelasan Transformasi Silver

`02_silver.py` melakukan 5 transformasi sebelum data disimpan ke Silver layer.

### Transformasi 1 — Hapus Duplikat
**Kode:** `dropDuplicates(["title", "url"])`

**Mengapa bisa ada duplikat?**
- Producer retry saat Kafka broker timeout → message dikirim 2x
- Berita viral dimuat oleh banyak sumber sekaligus (GNews + Kompas + Tempo)
- Consumer batch overlap saat restart

**Dampak jika tidak dihapus:**
- Kata dari berita populer ter-count 2–3x lebih tinggi dari seharusnya
- Top 15 words menjadi bias dan tidak merepresentasikan tren yang sebenarnya
- Contoh: "Indonesia juara" dari 3 sumber → kata "indonesia" ter-count 3x

---

### Transformasi 2 — Filter Null & Empty
**Kode:** `filter(col("title").isNotNull() & (col("title") != "") & col("timestamp").isNotNull())`

**Mengapa bisa ada data null/kosong?**
- JSON parsing gagal → field missing
- API timeout → response tidak lengkap
- RSS feed format tidak standar → field tidak ter-parse

**Dampak jika tidak difilter:**
- Berita tanpa judul tidak bisa dianalisis word frequency-nya
- Timestamp null menyebabkan error saat parsing → kolom `jam` bernilai null
- Data rusak masuk ke downstream analisis → hasil tidak valid

---

### Transformasi 3 — Parse & Ekstrak Timestamp
**Kode:**
```python
.withColumn("parsed_timestamp", to_timestamp(col("timestamp")))
.withColumn("jam", hour(col("parsed_timestamp")))
.withColumn("tanggal", to_date(col("parsed_timestamp")))
.withColumn("hari_dalam_minggu", dayofweek(col("parsed_timestamp")))
```

**Mengapa ini wajib?**

Di ETS lama, timestamp masih berupa string `"2026-04-26T16:50:17Z"`. Akibatnya:
```python
# ❌ ETS lama — tidak bisa lakukan ini:
df.groupBy(hour(col("timestamp")))  # Error! hour() perlu TIMESTAMP type

# ✓ Silver — setelah parsing:
df.groupBy("jam").count()  # Analisis per jam berjalan lancar
```

Tanpa transformasi ini, Word Velocity (analisis trending per jam) tidak bisa dibuat sama sekali karena Window Function membutuhkan tipe data temporal yang benar.

---

### Transformasi 4 — Normalisasi Teks Judul
**Kode:**
```python
.withColumn("title_clean", lower(trim(col("title"))))
.withColumn("title_word_count", size(split(col("title_clean"), " ")))
```

**Mengapa lowercase penting?**
```
Tanpa normalisasi:
"Indonesia" → count 1
"indonesia" → count 1  ← dihitung TERPISAH
"INDONESIA" → count 1  ← dihitung TERPISAH

Dengan normalisasi:
"indonesia" → count 3  ← dihitung BENAR
```

Tanpa ini, top words terpecah-pecah dan tidak mencerminkan frekuensi sesungguhnya.

---

### Transformasi 5 — Standarisasi Nama Sumber
**Kode:** `.withColumn("source_normalized", upper(trim(col("source"))))`

**Mengapa perlu?**
- "Kompas.com", "kompas.com", "KOMPAS.COM" → dihitung sebagai 3 sumber berbeda
- Menyebabkan distribusi per sumber tidak akurat
- Setelah standarisasi → semua menjadi "KOMPAS.COM" → dihitung sebagai 1 sumber

---

## 5. Perbandingan Gold vs Pengerjaan ETS lama

### Tabel Perbandingan Langsung

| Aspek | ETS Lama (Spark + JSON) | Gold Layer (Delta Lake) |
|-------|------------------------|------------------------|
| **Input data** | JSON mentah dari HDFS | Silver yang sudah bersih |
| **Schema** | Tidak di-enforce, field bisa hilang | Strict, validated di Silver |
| **Duplikat** | Tidak ditangani → bias | Sudah dihapus di Silver |
| **Timestamp** | String, tidak di-parse | `TimestampType`, sudah di-extract |
| **Analisis per jam** | Terbatas (string comparison) | Akurat (Window Function) |
| **Word Frequency** | Bisa (tapi bias karena duplikat) | Lebih akurat |
| **Source Distribution** | Bisa (tapi case-sensitive) | Lebih akurat (normalized) |
| **Trending Detection** | ❌ Tidak ada | ✓ Word Velocity (Enhanced) |
| **Cross-source Analysis** | ❌ Tidak ada | ✓ Cross-Source Topics (Enhanced) |
| **Time Travel** | ❌ Tidak ada | ✓ Query versi manapun |
| **ACID** | ❌ Tidak ada | ✓ Guaranteed |
| **Rollback** | ❌ Manual/tidak bisa | ✓ `versionAsOf` |

### Analisis Gold yang Tidak Bisa Dibuat di ETS

**Gold Table 3 — Word Velocity (Trending Detection):**
```
ETS lama tidak bisa karena:
→ timestamp belum di-parse → hour() tidak bisa dipakai
→ tidak ada Window Function yang bisa jalan dengan benar

Gold bisa karena:
→ Silver sudah punya kolom "jam" (integer 0–23)
→ Window.partitionBy("word").orderBy("jam") berjalan sempurna
→ lag() function bisa hitung selisih frekuensi antar jam
```

**Gold Table 4 — Cross-Source Topics:**
```
ETS lama tidak bisa karena:
→ API dan RSS dianalisis terpisah di analysis.py
→ Tidak ada join antara kedua sumber

Gold bisa karena:
→ Silver menggabungkan API + RSS dengan kolom "_source"
→ groupBy("jam", "word").agg(count("_source")) → deteksi multi-source coverage
```

---

## 6. Screenshot Output

### [Screenshot 1: Output Bronze Layer]

<img width="770" height="336" alt="image" src="https://github.com/user-attachments/assets/b0806796-be0c-4a33-b072-beb27c737bf5" />

### [Screenshot 2: Output Silver Layer — Statistik Transformasi]

<img width="1400" height="781" alt="image" src="https://github.com/user-attachments/assets/d2882374-637f-4eaa-8d98-48119d375d38" />

<img width="1270" height="811" alt="image" src="https://github.com/user-attachments/assets/c4d06ced-ec61-48f0-8abb-03aeea72c19a" />

<img width="753" height="625" alt="image" src="https://github.com/user-attachments/assets/97c57df5-74e9-4161-857d-07fab43ad019" />


### [Screenshot 3: Output Gold Layer — Top Words & Time Travel]

<img width="1397" height="797" alt="image" src="https://github.com/user-attachments/assets/55bbdf27-60d0-4a69-958d-5fff65a2806e" />

<img width="796" height="776" alt="image" src="https://github.com/user-attachments/assets/a2a19824-9528-4949-bd8b-b9e2f128cf82" />

<img width="882" height="822" alt="image" src="https://github.com/user-attachments/assets/9bf5e37e-c03c-4fc4-af14-b76cbd70e1d4" />

<img width="799" height="442" alt="image" src="https://github.com/user-attachments/assets/f69fcbc5-e9d7-4828-9348-f68b324f1ca1" />

## 7. Refleksi: Delta Lake vs HDFS/CSV

Berdasarkan pengalaman langsung mengerjakan tugas ini:

### 1. Schema Enforcement Menyelamatkan dari Silent Bug
Di ETS lama, data API memiliki kolom `category` yang tidak ada di RSS. Saat kedua data digabung, Spark tidak komplain — kolom yang hilang cukup diisi `null`. Akibatnya analisis berjalan tapi hasilnya salah tanpa ada error.

Dengan Delta Lake, perbedaan schema langsung terdeteksi saat append:
```
Schema mismatch detected when writing to the Delta table
```
Lebih baik gagal keras daripada diam-diam menghasilkan data yang salah.

### 2. ACID Membuat Pipeline Bisa Dijalankan Ulang dengan Aman
Karena Silver menggunakan mode `overwrite`, setiap kali dijalankan hasilnya selalu konsisten — tidak ada partial write yang merusak data. Di HDFS biasa, jika Spark crash di tengah write, file bisa corrupt dan harus direcovery manual.

### 3. Time Travel Terbukti Berguna untuk Audit
Saat melakukan demonstrasi update `source_normalized`, kita bisa langsung query versi sebelum dan sesudah update dalam satu script tanpa perlu backup manual. Ini sangat berguna untuk:
- Debugging: "data berubah jadi salah, kapan tepatnya?"
- Rollback: kembalikan ke versi sebelum kesalahan
- Audit: siapa mengubah apa dan kapan

### 4. Medallion Architecture Memaksa Pemisahan Tanggung Jawab yang Jelas
ETS lama mencampur ingestion, cleaning, dan analisis dalam satu file `analysis.py`. Dengan Medallion:
- Bronze: hanya bertanggung jawab simpan data mentah
- Silver: hanya bertanggung jawab cleaning
- Gold: hanya bertanggung jawab analisis

Jika ada bug di word frequency, langsung tahu itu masalah di Gold — bukan di Bronze atau Silver. Debugging jadi jauh lebih mudah.

---

## 8. Struktur Folder

```
etsbigdata/
├── ... (kode ETS lama, tidak diubah)
│
└── lakehouse/
    ├── README_lakehouse.md    ← File ini
    ├── 01_bronze.py           ← Ingest HDFS → Bronze Delta
    ├── 02_silver.py           ← Cleaning → Silver Delta (5 transformasi)
    └── 03_gold.py             ← Agregasi → Gold Delta (4 tabel + Time Travel)

lakehouse_data/               ← Di-generate otomatis saat pipeline dijalankan
├── bronze/
│   ├── news_api/             ← Delta format: parquet + _delta_log/
│   └── news_rss/
├── silver/
│   └── news/
└── gold/
    ├── word_freq/
    ├── news_per_source/
    ├── word_velocity/
    └── cross_source_topics/

news_data_local/              ← Cache export HDFS (auto-generated, jangan di-push ke git)
├── api/
└── rss/
```

> **Catatan:** Folder `lakehouse_data/` dan `news_data_local/` tidak perlu di-push ke GitHub karena di-generate ulang setiap kali pipeline dijalankan. Tambahkan keduanya ke `.gitignore`.

---

**NewsPulse — Kelompok 5 | Big Data ITS 2026**
