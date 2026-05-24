(Pengerjaan dilakukan menggunakan sharing file WhatsApp sehingga repo hanya memiliki sedikit kontributor 🙏)

# 🌍 NewsPulse — Sistem Big Data Pipeline Analisis Berita Nasional Indonesia

**Evaluasi Tengah Semester (ETS) — Praktik Kelompok**  
Mata Kuliah: Big Data dan Data Lakehouse  
Institusi: ITS (Institut Teknologi Sepuluh Nopember)  
Tanggal: April 2026

---

## 👥 Anggota Tim & Kontribusi

| No. | Nama | NRP | Kontribusi |
|-----|------|-----|-----------|
| 1 | Muhammad Fachry Shalahuddin Rusamsi | 5027241031 | Overall Architecture, Integration |
| 2 | Muhammad Huda Rabbani | 5027241098 | Documentation, Testing |
| 3 | Abiyyu Raihan Putra Wikanto | 5027241042 | API Producer (GNews), Dashboard |
| 4 | Daniswara Fausta Novanto | 5027241050 | HDFS Consumer, Storage Layer |
| 5 | Muhammad Khairul Yahya | 5027241092 | Kafka Setup, RSS Producer |

---

## 🎯 Topik & Justifikasi

### **Topik 5: NewsPulse — Analisis Tren Berita Nasional**

**Skenario Klien:**  
PR agency membutuhkan sistem monitoring otomatis untuk mendeteksi isu mana yang sedang trending di berbagai media nasional dan digital hari ini.

**Pertanyaan Bisnis yang Dijawab:**  
> *"Topik apa yang paling hangat hari ini di berbagai media, dan jam berapa biasanya berita dominan muncul?"*

**Justifikasi Pemilihan Topik:**
- Relevan dengan kondisi real-time media Indonesia
- Menggabungkan 2 sumber data berbeda (API + RSS feed)
- Analisis NLP sederhana namun bermakna (word frequency)
- Dashboard yang informatif untuk end-user non-teknis

---

## 🏗️ Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────────┐
│                   NEWSPULSE PIPELINE ARCHITECTURE               │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐         ┌──────────────────┐
│  GNews API       │         │  RSS Feeds       │
│  (10 min poll)   │         │  (5 min poll)    │
└────────┬─────────┘         └────────┬─────────┘
         │                            │
         ▼                            ▼
   ┌──────────────┐           ┌──────────────┐
   │ producer_    │           │ producer_    │
   │ api.py       │           │ rss.py       │
   └────────┬─────┘           └────────┬─────┘
            │                          │
            │     news-api    │     news-rss  │
            │    (Kafka Topic)│(Kafka Topic)  │
            └─────────┬───────┴──────────┬─────┘
                      │
                      ▼
            ╔════════════════════╗
            ║   APACHE KAFKA     ║
            ║  Ingestion Layer   ║
            ╚────────┬───────────╝
                     │
                     ▼
              ┌────────────────┐
              │ consumer_to_   │
              │ hdfs.py        │
              │ - Buffer 60s   │
              │ - Save to HDFS │
              │ - Local JSON   │
              └────────┬───────┘
                       │
         ┌─────────────┴──────────────┐
         ▼                            ▼
    ┌─────────────┐          ┌─────────────────┐
    │   HDFS      │          │ Dashboard/Data  │
    │  Storage    │          │ JSON Files      │
    │ /data/news/ │          │ (live view)     │
    └─────────────┘          └────────┬────────┘
         │                            │
         │            ┌───────────────┘
         │            │
         ▼            ▼
    ┌────────────┐ ┌──────────────────┐
    │  Spark     │ │ analysis.py      │
    │Analysis   │ │ - Top words      │
    │           │ │ - Source distrib │
    │           │ │ - Hourly volume  │
    └─────┬──────┘ │                  │
          │        └────────┬─────────┘
          │                 │
          └────────┬────────┘
                   │
                   ▼
          ┌──────────────────┐
          │ spark_results.   │
          │ json             │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │   Flask App      │
          │  (Dashboard)     │
          │ localhost:5000   │
          └──────────────────┘
```

### **Alur Data:**
1. **Ingestion**: 2 producer (API & RSS) → 2 Kafka topic
2. **Buffering**: Consumer membaca Kafka, buffer 60 detik
3. **Storage**: Tersimpan di HDFS + JSON lokal untuk dashboard
4. **Processing**: Spark analisis 3 dimensi dari data yang ada
5. **Serving**: Dashboard Flask menampilkan hasil real-time

---

## 🔧 Teknologi Stack

| Layer | Teknologi | Versi | Peran |
|-------|-----------|-------|-------|
| **Ingestion** | Apache Kafka | 7.3.0 | Message broker real-time |
| **Storage** | Hadoop HDFS | 3.2.1 | Distributed file storage |
| **Processing** | Apache Spark | 3.5.0 | Batch analytics |
| **Web** | Flask | 3.0.0 | Dashboard UI |
| **Data Source** | GNews API | v4 | Headline API |
| **Data Source** | RSS Feeds | - | Kompas, Tempo |

---

## 📋 Fitur & Analisis

### **3 Analisis Wajib (Spark):**

1. **🔥 Topik Trending (Kata Paling Sering)**
   - Extract judul berita → split menjadi kata
   - Filter stopwords (dan, yang, di, untuk, dengan, dll)
   - Hitung frekuensi → Top 15 kata
   - Output: Hashtag trending dengan jumlah kemunculan

2. **📊 Distribusi Berita per Sumber**
   - Group by media/sumber (GNews, Kompas, Tempo, dll)
   - Hitung jumlah artikel per sumber
   - Output: Tabel ranking sumber berita

3. **⏰ Volume Publikasi per Jam**
   - Extract jam dari timestamp artikel
   - Hitung jumlah artikel per jam
   - Output: Tren publikasi sepanjang hari

### **Dashboard Panels:**

| Panel | Data | Update |
|-------|------|--------|
| **🔥 Trending Topics** | Spark analysis (top words) | Real-time |
| **📢 Berita Terkini (API)** | 10 artikel terakhir dari GNews | 10 min |
| **📰 Feed Nasional (RSS)** | 10 artikel terakhir dari RSS | 5 min |
| **🏢 Distribusi per Sumber** | Jumlah berita per media (CNN, Tempo, dll) | Setiap Spark run |
| **⏰ Volume per Jam** | Jumlah berita tiap jam (00:00–23:00) | Setiap Spark run |

---

## 🚀 Cara Menjalankan Sistem

### **Prasyarat:**
- Docker & Docker Compose terinstall
- Python 3.8+
- Kafka-python, requests, feedparser, pyspark, flask

### **Step 1: Setup Environment**
```bash
# Clone repository
git clone <repo-url>
cd etsbigdata

# Install dependencies
pip install -r requirements.txt
```

### **Step 2: Jalankan Docker Containers**
```bash
# Start Kafka & Zookeeper
docker-compose up -d

# Verifikasi container berjalan
docker ps
# Seharusnya ada: zookeeper, kafka-broker, namenode, datanode
```

### **Step 3: Setup Kafka Topics (Sekali Saja) — RECOMMENDED**

**Gunakan Python Script (PALING MUDAH - Tidak perlu kafka-topics.sh):**
```bash
# Jalankan Python script untuk membuat topics
python setup_kafka_topics.py
```

Expected output:
```
[2026-04-27 10:15:30.123456] 🚀 Membuat Kafka Topics...
[2026-04-27 10:15:30.456789] 📍 Kafka Broker: localhost:9092

[2026-04-27 10:15:31.123456] ✅ Admin client berhasil terhubung ke Kafka

[2026-04-27 10:15:31.456789] 📝 Membuat 2 topics:

[2026-04-27 10:15:32.123456] ✅ Topic 'news-api' berhasil dibuat
[2026-04-27 10:15:32.456789] ✅ Topic 'news-rss' berhasil dibuat

[2026-04-27 10:15:33.123456] 🔍 Verifikasi topics yang ada:
[2026-04-27 10:15:33.456789] ✅ news-api
[2026-04-27 10:15:33.789012] ✅ news-rss

[2026-04-27 10:15:34.123456] ✨ Setup Kafka topics selesai!
```

✅ **Ini mengatasi masalah `kafka-topics.sh: command not found`!**

### **Step 4: Jalankan Producer & Consumer (3 Terminal Terpisah)**

**Terminal 1 - Producer API (GNews):**
```bash
cd kafka
python producer_api.py
# Diharapkan: ✅ Koneksi Kafka berhasil, 🚀 Producer API dimulai
```

**Terminal 2 - Producer RSS:**
```bash
cd kafka
python producer_rss.py
# Diharapkan: ✅ Koneksi Kafka berhasil, 🚀 Producer RSS dimulai
```

**Terminal 3 - Consumer to HDFS:**
```bash
cd hdfs
python consumer_to_hdfs.py
# Diharapkan: ✅ Koneksi Kafka berhasil, 🚀 Consumer dimulai
# ✅ Akan flush buffer data setiap 60 detik (even with low traffic)
# Flush juga terjadi saat shutdown
```

### **Step 5: Jalankan Spark Analysis**
```bash
# Di terminal baru
cd spark

# Pastikan Java/JDK sudah terpasang:
# java -version

# --- OPSI A: Jalankan SEKALI (single run) ---
python analysis.py

# --- OPSI B: Jalankan TERUS-MENERUS / Continuous Mode ---
# (Disarankan agar dashboard selalu mendapat data terbaru)
python analysis.py --continuous

# Ubah interval (default 300 detik = 5 menit):
python analysis.py --continuous --interval 120   # setiap 2 menit
```

> **Catatan:** Mode `--continuous` akan menjalankan ulang analisis Spark secara otomatis
> setiap N detik. Setiap iterasi membaca data terbaru dari HDFS dan memperbarui
> `dashboard/data/spark_results.json` sehingga dashboard selalu menampilkan data terkini.

### **Step 6: Jalankan Dashboard**
```bash
# Di terminal baru
cd dashboard

# Optional: Enable Flask debug mode (default is False for security)
# PowerShell: $env:FLASK_DEBUG='True'
# Linux/Mac: export FLASK_DEBUG='True'

python app.py

# Buka browser: http://localhost:5000
# Dashboard akan auto-refresh setiap 30 detik dengan data dari /api/data
```

## 📊 Output Sample

### **Sample Output - live_api.json:**
```json
[
  {
    "title": "Trio Persib Dipanggil Timnas Indonesia",
    "source": "Kompas.com",
    "url": "https://bola.kompas.com/...",
    "summary": "Persib umumkan tiga pemain...",
    "timestamp": "2026-04-26T16:35:00Z"
  }
]
```

### **Sample Output - spark_results.json:**
```json
{
  "timestamp": "2026-04-27T10:30:45.123456",
  "total_records_analyzed": 245,
  "top_words": [
    {"word": "indonesia", "count": 42},
    {"word": "berita", "count": 38},
    {"word": "timnas", "count": 35}
  ],
  "source_dist": [
    {"source": "Kompas.com", "total_news": 120},
    {"source": "Tempo", "total_news": 85}
  ],
  "hourly_vol": [
    {"jam_publikasi": 6, "count": 12},
    {"jam_publikasi": 7, "count": 18}
  ]
}
```

---

## ⚙️ Konfigurasi

### **GNews API Token:**
- Token disimpan di: `kafka/producer_api.py` baris 9
- Gratis untuk 100 request/hari
- Daftar di: https://gnews.io

### **RSS Feeds:**
- Kompas Nasional: `https://rss.kompas.com/feed/kompas.com/nasional`
- Tempo Nasional: `https://rss.tempo.co/nasional`

### **Kafka Broker:**
- Bootstrap Server: `localhost:9092`
- Zookeeper: `localhost:2181`

### **HDFS Namenode:**
- Web UI: `http://localhost:9870`
- Default FS: `hdfs://namenode:8020`

---

## 📚 Referensi

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Hadoop HDFS Guide](https://hadoop.apache.org/docs/stable/hadoop-hdfs/HdfsDesign.html)
- [Apache Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Flask Documentation](https://flask.palletsprojects.com/)
- [GNews API](https://gnews.io/docs)

---

## 📝 Lisensi & Catatan

Proyek ini dibuat untuk keperluan akademis — ETS Praktik Kelompok ITS 2026.

**Status:** Siap deployment untuk production-ready (dengan enhancement security & monitoring)

**Maintenance:** Perlu update API token GNews setiap bulan jika subscription berakhir

---

**Last Updated:** Mei 10, 2026  
**Status:** ✅ Fully Functional  
**Perubahan Terakhir:**
- ✅ Spark mode `--continuous`: analisis otomatis berulang (tidak hanya sekali)
- ✅ Panel dashboard baru: **Distribusi Berita per Sumber** (bar chart CSS)
- ✅ Panel dashboard baru: **Volume Publikasi per Jam** (bar chart 00:00–23:00)
