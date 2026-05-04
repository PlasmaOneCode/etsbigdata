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
# Jika masih error JAVA_HOME, set dulu (PowerShell):
# $env:JAVA_HOME="C:\Program Files\Java\jdk-17"
# $env:Path="$env:JAVA_HOME\bin;$env:Path"

# Script ini membaca data dari HDFS (/data/news/api dan /data/news/rss),
# lalu menyimpan ringkasan ke:
# 1) /data/news/hasil/ (HDFS)
# 2) dashboard/data/spark_results.json (untuk dashboard)

python analysis.py

# Atau gunakan Jupyter:
# jupyter notebook analysis.ipynb
```

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

### **Step 7: Verifikasi Data Flow**

**Check Kafka topics (tanpa shell script):**
```bash
# Buat file check_kafka.py untuk verifikasi topics
# Atau gunakan Python langsung:
python -c "
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'news-api', 'news-rss',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=5000
)

print('📊 Membaca data dari Kafka topics...\n')
count_api = 0
count_rss = 0

for message in consumer:
    if message.topic == 'news-api':
        count_api += 1
        print(f'[news-api] {message.value[\"title\"][:60]}...')
    elif message.topic == 'news-rss':
        count_rss += 1
        print(f'[news-rss] {message.value[\"title\"][:60]}...')
    
    if count_api + count_rss >= 10:
        break

consumer.close()
print(f'\n✅ Total: {count_api} dari API, {count_rss} dari RSS')
"
```

**Check HDFS:**
```bash
# Akses namenode container
docker exec -it namenode bash

# Lihat struktur folder
hdfs dfs -ls -R /data/news/

# Lihat isi file
hdfs dfs -cat /data/news/api/news-api_2026-04-27_*.json | head -20
```

**Check Dashboard Files:**
```bash
# Lihat file live data
cat dashboard/data/live_api.json | python -m json.tool
cat dashboard/data/live_rss.json | python -m json.tool
cat dashboard/data/spark_results.json | python -m json.tool
```

---

## 📊 Screenshot & Output Sample

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

## 🐛 Troubleshooting

### **Issue 0: kafka-topics.sh: command not found**
```
bash: kafka-topics.sh: command not found
```
**Penyebab:** Script `kafka-topics.sh` tidak tersedia di container Kafka yang digunakan (`confluentinc/cp-kafka:7.3.0`). Script shell ini tidak di-include dalam image tersebut.

**Solusi (Recommended):** ✅ Gunakan **Python script**:
```bash
# Gunakan setup_kafka_topics.py yang kami sediakan
python setup_kafka_topics.py
```

**Solusi Alternative (Jika script Python tidak bisa):** Buat topics menggunakan Kafka Python Client langsung di script producer/consumer.

---

### **Issue 1: Kafka tidak konek**
```
Error: Could not connect to bootstrap server 'localhost:9092'
```
**Solusi:**
```bash
# Verifikasi container Kafka berjalan
docker ps | grep kafka

# Cek log
docker logs kafka-broker

# Restart Kafka
docker restart kafka-broker
docker restart zookeeper
```

### **Issue 2: RSS feed kosong**
```
⚠️  File RSS tidak ada atau kosong
```
**Solusi:**
- Pastikan URL RSS valid dengan `curl https://rss.kompas.com/...`
- Check firewall/network blocking RSS domain
- Verify consumer sudah running minimal 60 detik

### **Issue 3: Spark analisis gagal**
```
❌ Error membaca data: ...
```
**Solusi:**
- Pastikan consumer sudah menulis file ke `dashboard/data/`
- Check file bukan kosong: `wc -c dashboard/data/live_api.json`
- Jalankan analisis setelah data ada minimal 100+ articles

### **Issue 4: Dashboard blank**
```
Memuat data... (tidak pernah selesai)
```
**Solusi:**
- Check browser console (F12) untuk error
- Verifikasi Flask app running: `netstat -an | grep 5000`
- Akses API endpoint: `curl http://localhost:5000/api/data`

---

## 📈 Performa & Optimasi

| Aspek | Target | Pencapaian |
|-------|--------|-----------|
| API polling | 10 min | ✅ Sesuai |
| RSS polling | 5 min | ✅ Sesuai |
| Consumer buffer | 60 sec | ✅ Sesuai |
| Dashboard refresh | 30 sec | ✅ Sesuai |
| HDFS file size | ~1-5MB per file | ✅ Optimal |

---

## 🎓 Tantangan & Solusi

### **Tantangan 1: Data Flow dari Windows ke Docker Container**

**Masalah:** Konsumer berjalan di Windows, tapi perlu menulis ke HDFS yang ada di Docker container.

**Solusi:** Menggunakan jembatan Docker (`docker cp` + `docker exec`) untuk transfer file:
1. Simpan file lokal di Windows
2. Copy ke container OS dengan `docker cp`
3. Upload ke HDFS dengan `hdfs dfs -put`
4. Hapus temporary file

**Kode:**
```python
# Copy ke container
docker cp local_file.json namenode:/local_file.json

# Upload ke HDFS
docker exec namenode hdfs dfs -put /local_file.json /data/news/api/
```

---

### **Tantangan 2: JSON Format Inconsistency antara API & RSS**

**Masalah:** GNews API dan RSS feed memiliki format timestamp berbeda, menyebabkan parsing error di Spark.

**Solusi:** Normalisasi format JSON di consumer:
- Pastikan semua field konsisten (title, source, url, summary, timestamp)
- Gunakan `timestamp.isoformat()` untuk format standard
- Handle missing fields dengan `.get()` dan default value

**Kode:**
```python
data = {
    'title': entry.get('title', ''),
    'source': source_name,
    'url': entry.get('url', ''),
    'summary': entry.get('summary', ''),
    'timestamp': entry.get('published', datetime.now().isoformat())
}
```

---

### **Tantangan 3: Kafka Consumer Offset Management**

**Masalah:** Konsumer restart → membaca ulang dari beginning, menyebabkan data duplikat

**Solusi:** Gunakan `group_id` yang konsisten di konsumer:
- Offset disimpan Kafka broker per group_id
- Consumer akan resume dari offset terakhir
- Untuk reset, gunakan: `kafka-consumer-groups.sh --reset-offsets`

**Kode:**
```python
consumer = KafkaConsumer(
    'news-api', 'news-rss',
    group_id='hdfs_writer_group',  # Konsisten!
    auto_offset_reset='earliest'
)
```

---

### **Tantangan 4: Performance Analisis dengan Dataset Besar**

**Masalah:** Spark memory error saat menganalisis 1000+ articles.

**Solusi:** Optimasi Spark config:
- Allocate lebih banyak memory ke driver
- Use DataFrame optimization (filter early, cache sparingly)
- Limit hasil (top 15 words, bukan semua)

**Kode:**
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

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

**Last Updated:** April 27, 2026  
**Status:** ✅ Fully Functional