# ETS Praktik: Big Data Pipeline End-to-End

|                         |                                                              |
| ----------------------- | ------------------------------------------------------------ |
| **Mata Kuliah**         | Big Data dan Data Lakehouse                                  |
| **Jenis Evaluasi**      | Evaluasi Tengah Semester (ETS) — Praktik Kelompok           |
| **Pertemuan Terkait**   | P4 (Hadoop/HDFS), P5 (Spark), P8 (Kafka)                   |
| **Kelompok**            | 4–5 orang, topik wajib berbeda antar kelompok               |
| **Durasi Pengerjaan**   | 2 minggu sejak tanggal rilis                                 |
| **Deliverable**         | GitHub Repository + Demo Live 10 menit                      |

---

## 🎯 Filosofi ETS: Problem-Based Learning

Ujian ini bukan tentang pengetahuan teoritis — tapi tentang **kemampuan membangun sistem nyata**.

Di dunia industri, seorang Data Engineer tidak pernah mengerjakan teknologi secara terpisah-pisah. Mereka membangun **pipeline** — aliran data yang mengalir dari sumber, melalui penyimpanan, ke pemrosesan, hingga ke tampilan yang bisa dimengerti manusia. Itulah yang akan kalian bangun di ETS ini.

Setiap kelompok mendapat **domain yang berbeda**, tapi semua membangun sistem dengan arsitektur yang sama. Kemampuan yang diuji bukan sekadar "bisa menjalankan Kafka" — tapi **bisa menyambungkan Kafka ke HDFS ke Spark ke Dashboard** untuk menjawab pertanyaan bisnis yang nyata.

---

## 📋 Latar Belakang

**DataPipeline.id** adalah startup konsultan data yang baru berdiri. Mereka mendapat beberapa kontrak sekaligus dari klien di berbagai industri — semuanya mengeluhkan masalah yang sama:

> *"Kami punya banyak data yang terus mengalir setiap menit — dari API, dari berita, dari sensor — tapi kami tidak tahu harus menyimpan di mana, bagaimana memprosesnya, dan bagaimana menampilkannya secara bermakna."*

CEO DataPipeline.id membentuk **8 tim Data Engineer** untuk mengerjakan 8 klien sekaligus. Setiap tim mendapat satu domain klien dan harus membangun fondasi sistem Big Data yang terdiri dari pipeline ingestion, penyimpanan terdistribusi, analisis batch, dan dashboard monitoring.

**Kelompok kalian adalah salah satu dari 8 tim tersebut.**

---

## 🏗️ Arsitektur Sistem (Berlaku untuk Semua Kelompok)

Semua kelompok membangun sistem dengan arsitektur yang sama. Yang berbeda hanya domain data dan sumber data yang digunakan.

```
╔═══════════════════════════════════════════════════════════════════╗
║              ARSITEKTUR PIPELINE BIG DATA                         ║
╠═══════════════════════════════════════════════════════════════════╣
║                                                                   ║
║  [SUMBER 1: API Real-time]         [SUMBER 2: RSS/Berita]         ║
║       │                                    │                      ║
║       ▼                                    ▼                      ║
║  ┌─────────┐                       ┌─────────┐                    ║
║  │Producer │                       │Producer │                    ║
║  │API.py   │                       │RSS.py   │                    ║
║  └────┬────┘                       └────┬────┘                    ║
║       │                                 │                         ║
║       ▼                                 ▼                         ║
║  ╔══════════════════════════════════════════╗                      ║
║  ║           APACHE KAFKA                   ║                      ║
║  ║  Topic: [tema]-api   Topic: [tema]-rss   ║                      ║
║  ╚══════════════════════╤═══════════════════╝                      ║
║                         │                                         ║
║                         ▼                                         ║
║                  ┌─────────────┐                                  ║
║                  │Consumer     │       ← membaca dari kedua topic  ║
║                  │to HDFS.py   │       ← menyimpan ke HDFS        ║
║                  └──────┬──────┘                                  ║
║                         │                                         ║
║                         ▼                                         ║
║  ╔══════════════════════════════════════════╗                      ║
║  ║         HADOOP HDFS                      ║                      ║
║  ║   /data/[tema]/api/   /data/[tema]/rss/  ║                      ║
║  ╚═════════════╤════════════════════════════╝                      ║
║                │                                                   ║
║                ▼                                                   ║
║         ┌─────────────┐                                            ║
║         │Apache Spark │   ← baca dari HDFS, analisis, simpan hasil ║
║         │analysis.py  │                                            ║
║         └──────┬───────┘                                           ║
║                │                                                   ║
║                ▼                                                   ║
║         ┌─────────────┐                                            ║
║         │  Dashboard  │   ← Flask, localhost:5000                  ║
║         │  (Flask)    │   ← baca hasil Spark + event terbaru       ║
║         └─────────────┘                                            ║
╚═══════════════════════════════════════════════════════════════════╝
```

**Mengapa arsitektur ini?**

| Teknologi | Peran dalam Pipeline | Alasan Dipilih |
|-----------|---------------------|----------------|
| **Apache Kafka** | *Ingestion Layer* — menerima dan menyangga data dari sumber manapun | Decouples sumber dari storage; producer tidak perlu tahu ke mana data pergi |
| **HDFS** | *Storage Layer* — menyimpan semua data secara terdistribusi | Fault-tolerant, skalabel, menjadi single source of truth |
| **Apache Spark** | *Processing Layer* — membaca HDFS, menganalisis, menghasilkan insight | Distributed processing, DataFrame + SQL API, hasil bisa disimpan kembali ke HDFS |
| **Dashboard** | *Serving Layer* — menampilkan hasil untuk pengambilan keputusan | Membuat data bermakna bagi non-engineer |

---

## 📦 Komponen yang Harus Dibangun

Semua kelompok wajib membangun **4 komponen** berikut, disesuaikan dengan domain masing-masing.

---

### Komponen 1 — Apache Kafka: Ingestion Layer

**Tujuan:** Kafka berfungsi sebagai pintu masuk data — semua data yang mengalir dari luar masuk melalui Kafka sebelum disimpan.

**Yang harus dibangun:**

- [ ] Setup Kafka menggunakan Docker Compose dari materi P8
- [ ] Buat **2 Kafka topic** sesuai domain:
  - Topic 1: data dari API real-time (nama: `[tema]-api`)
  - Topic 2: data dari RSS feed (nama: `[tema]-rss`)
- [ ] Buat **Producer 1** (`producer_api.py`): polling API eksternal setiap 60 detik, format data sebagai JSON, kirim ke topic API dengan **key** berdasarkan identifier data (misalnya simbol koin, kode kota, dst.)
- [ ] Buat **Producer 2** (`producer_rss.py`): polling RSS feed setiap 5 menit, parse feed menggunakan library `feedparser`, hindari duplikat dengan menyimpan ID yang sudah dikirim, kirim ke topic RSS

**Hint teknis Kafka:**
- Gunakan `kafka-python` library: `pip install kafka-python`
- Producer harus menggunakan `enable_idempotence=True` dan `acks="all"` agar tidak kehilangan data
- RSS: gunakan `feedparser.parse(url)` untuk parsing, field penting: `entry.title`, `entry.link`, `entry.summary`, `entry.published`
- Setiap event harus dalam format JSON yang konsisten (sama strukturnya), sertakan field `timestamp` di setiap event

**Cara memverifikasi Kafka berjalan:**
```bash
# Cek topic yang dibuat
docker exec -it kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092

# Cek event masuk ke topic
docker exec -it kafka-broker kafka-console-consumer.sh \
  --topic [tema]-api --from-beginning --bootstrap-server localhost:9092
```

---

### Komponen 2 — HDFS: Storage Layer

**Tujuan:** Consumer membaca dari Kafka dan menyimpan data ke HDFS. HDFS menjadi *single source of truth* — semua data tersimpan di sini sebelum diproses Spark.

**Yang harus dibangun:**

- [ ] Setup Hadoop menggunakan Docker Compose dari materi P4
- [ ] Buat struktur direktori di HDFS:
  ```
  /data/[tema]/api/    ← tempat file JSON dari topic API
  /data/[tema]/rss/    ← tempat file JSON dari topic RSS
  /data/[tema]/hasil/  ← tempat output Spark (dibuat saat Spark berjalan)
  ```
- [ ] Buat **Consumer** (`consumer_to_hdfs.py`): baca dari kedua topic Kafka, kumpulkan event dalam buffer, setiap 2–5 menit simpan buffer ke HDFS sebagai file JSON bernama timestamp

**Hint teknis HDFS:**
- Gunakan `KafkaConsumer` dengan `group_id` unik per consumer, `auto_offset_reset="earliest"`
- Strategi penyimpanan ke HDFS (pilih salah satu):
  - **Opsi A (Mudah):** Consumer simpan ke file lokal dulu, lalu jalankan perintah `hdfs dfs -put [file_lokal] [path_hdfs]` menggunakan `subprocess.run()`
  - **Opsi B (Lebih baik):** Gunakan library `hdfs` Python (`pip install hdfs`) untuk menulis langsung ke HDFS
- Nama file di HDFS: gunakan timestamp, misalnya `2026-04-20_14-30.json`
- Gunakan threading untuk membaca 2 topic secara paralel

**Cara memverifikasi HDFS berjalan:**
```bash
# Cek isi direktori
hdfs dfs -ls -R /data/[tema]/

# Cek ukuran file
hdfs dfs -du -h /data/[tema]/api/
```

---

### Komponen 3 — Apache Spark: Processing Layer

**Tujuan:** Spark membaca semua data yang sudah tersimpan di HDFS dan menghasilkan insight bermakna. Spark **tidak** membaca dari Kafka secara langsung — semua data sudah ada di HDFS.

**Yang harus dibangun:**

- [ ] Buat `spark/analysis.py` atau `spark/analysis.ipynb`
- [ ] Spark membaca dari **HDFS** (bukan file lokal)
- [ ] **3 analisis wajib** (berbeda-beda per domain — lihat bagian Pilihan Topik)
- [ ] Gunakan kombinasi **DataFrame API** dan **Spark SQL**
- [ ] Setiap analisis harus disertai **narasi interpretasi** (bukan hanya tabel output)
- [ ] Simpan hasil ke HDFS: `hdfs dfs -ls /data/[tema]/hasil/`
- [ ] Simpan juga ringkasan sebagai `dashboard/data/spark_results.json` untuk dashboard

**Hint teknis Spark:**
- Inisialisasi SparkSession dengan koneksi ke HDFS:
  ```
  SparkSession.builder.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
  ```
- Baca file JSON dari HDFS: `spark.read.option("multiLine", True).json("hdfs://namenode:8020/data/[tema]/api/")`
- Spark akan membaca **semua file** dalam folder sekaligus — tidak perlu loop per file
- Gunakan `createOrReplaceTempView()` agar bisa query dengan Spark SQL
- Konversi hasil ke Pandas (`toPandas()`) hanya untuk menyimpan ke JSON lokal untuk dashboard

---

### Komponen 4 — Dashboard: Serving Layer

**Tujuan:** Tampilan web sederhana yang menggabungkan output Spark (data historis yang sudah dianalisis) dengan data terbaru dari consumer (event terkini dari Kafka).

**Yang harus dibangun:**

- [ ] Flask web app (`dashboard/app.py`) yang berjalan di `localhost:5000`
- [ ] **3 panel minimum** di halaman utama:
  - Panel 1: Hasil analisis Spark (baca dari `spark_results.json`)
  - Panel 2: Data live terbaru (baca dari file JSON yang diupdate consumer)
  - Panel 3: Berita/feed terbaru (baca dari file JSON yang diupdate consumer)
- [ ] Auto-refresh halaman atau data setiap 30–60 detik

**Hint teknis Dashboard:**
- Flask minimal butuh `pip install flask`
- Buat endpoint `/api/data` yang return JSON dari ketiga file, frontend-nya fetch endpoint ini
- Consumer (Komponen 2) harus menyimpan **salinan lokal** di `dashboard/data/live_api.json` dan `dashboard/data/live_rss.json` agar dashboard bisa membacanya
- Auto-refresh: gunakan `setInterval(fetch, 30000)` di JavaScript halaman HTML
- Tidak harus cantik — yang penting **fungsional** dan data nyata terlihat

---

## 🎯 Pilihan Topik (1 Per Kelompok)

> **Aturan:** Setiap kelompok memilih satu topik. Dalam satu kelas, tidak boleh ada dua kelompok dengan topik yang sama. Daftarkan pilihan topik ke dosen maksimal **3 hari setelah ETS dirilis** (first come, first served).

---

### Topik 1 — 💰 CryptoWatch: Monitor Pasar Aset Digital

**Skenario klien:** Platform edukasi kripto yang ingin menampilkan dashboard real-time harga aset digital beserta konteks berita untuk investor pemula.

**Pertanyaan bisnis yang harus dijawab:**
> *"Pada jam berapa harga kripto paling volatile? Dan apakah berita yang muncul sejalan dengan pergerakan harga?"*

| | Detail |
|-|--------|
| **API Real-time** | CoinGecko Simple Price API — **gratis, tanpa API key** |
| **Endpoint hint** | `https://api.coingecko.com/api/v3/simple/price` dengan parameter `ids`, `vs_currencies`, `include_24hr_change` |
| **Data yang diambil** | Harga USD/IDR + % perubahan 24 jam untuk BTC, ETH, BNB (atau pilih 3 koin lain) |
| **Interval polling** | Setiap 60 detik (hormati rate limit: 30 req/menit gratis) |
| **RSS Feed** | `https://www.coindesk.com/arc/outboundfeeds/rss/` |
| **Backup RSS** | `https://cointelegraph.com/rss` |
| **Kafka Topic 1** | `crypto-api` — key: simbol koin (BTC, ETH, BNB) |
| **Kafka Topic 2** | `crypto-rss` — key: hash 8 karakter dari URL artikel |
| **HDFS Path** | `/data/crypto/api/` dan `/data/crypto/rss/` |

**3 Analisis Spark Wajib:**
1. **Statistik harga per koin:** rata-rata, tertinggi, terendah, standar deviasi `price_usd` — groupBy `symbol`
2. **Volatilitas per jam:** rata-rata nilai absolut `change_24h` per jam — gunakan Spark SQL dengan `HOUR(TO_TIMESTAMP(timestamp))`
3. **Volume berita per jam:** hitung jumlah artikel RSS yang masuk per jam — identifikasi jam paling aktif berita

**Fokus dashboard:** Tabel harga + persentase perubahan (merah/hijau) · Grafik rata-rata harga per jam · Daftar 5 berita terbaru

---

### Topik 2 — ⛅ WeatherPulse: Monitor Cuaca 6 Kota Besar Indonesia

**Skenario klien:** Perusahaan logistik yang ingin memutuskan apakah pengiriman jalur darat aman berdasarkan kondisi cuaca kota-kota yang dilalui.

**Pertanyaan bisnis yang harus dijawab:**
> *"Kota mana yang kondisi cuacanya paling ekstrem hari ini, dan kapan perkiraan terbaik untuk pengiriman?"*

| | Detail |
|-|--------|
| **API Real-time** | Open-Meteo Current Weather API — **gratis, tanpa API key** |
| **Endpoint hint** | `https://api.open-meteo.com/v1/forecast` dengan parameter `latitude`, `longitude`, `current` (temperature, humidity, wind_speed, weather_code), `timezone=Asia/Jakarta` |
| **Kota yang dipantau** | Jakarta (-6.21, 106.85), Surabaya (-7.25, 112.75), Semarang (-6.99, 110.42), Medan (-3.59, 98.67), Makassar (-5.14, 119.41), Denpasar (-8.67, 115.21) |
| **Interval polling** | Setiap 10 menit (cuaca tidak berubah terlalu cepat) |
| **RSS Feed** | `https://rss.tempo.co/tag/cuaca` |
| **Backup RSS** | `https://rss.kompas.com/feed/kompas.com/sains/environment` |
| **Kafka Topic 1** | `weather-api` — key: kode kota (JKT, SBY, SMG, MDN, MKS, DPS) |
| **Kafka Topic 2** | `weather-rss` — key: hash URL |
| **HDFS Path** | `/data/weather/api/` dan `/data/weather/rss/` |

**3 Analisis Spark Wajib:**
1. **Perbandingan suhu antar kota:** rata-rata, suhu tertinggi, terendah per kota — groupBy `kode_kota`
2. **Deteksi kondisi ekstrem:** filter event dengan `wind_speed > 40 km/h` OR `humidity > 90%` OR `temperature > 35°C` — berapa kali dan kota mana?
3. **Tren suhu per jam dalam sehari:** rata-rata suhu semua kota per jam — apakah ada pola diurnal?

**Fokus dashboard:** Tabel suhu semua kota saat ini (warna berdasarkan suhu) · Highlight kota ekstrem · Berita cuaca terbaru

---

### Topik 3 — 🌫️ AirQuality Alert: Indeks Kualitas Udara Jawa Timur

**Skenario klien:** Dinas Kesehatan Provinsi Jawa Timur yang ingin memantau AQI kota-kota besar dan mengirimkan peringatan saat kualitas udara memburuk.

**Pertanyaan bisnis yang harus dijawab:**
> *"Pada jam berapa kualitas udara paling buruk, dan apakah berita lingkungan mencerminkan kondisi tersebut?"*

| | Detail |
|-|--------|
| **API Real-time** | AQICN World Air Quality Index API — **gratis setelah daftar**, API key instan |
| **Endpoint hint** | `https://api.waqi.info/feed/[city]/?token=[your_key]` — ganti `[city]` dengan `surabaya`, `malang`, `sidoarjo`, dst. |
| **Alternatif tanpa key** | OpenAQ API: `https://api.openaq.org/v2/latest?city=Surabaya&limit=10` — gratis tanpa key |
| **Kota yang dipantau** | Surabaya, Malang, Sidoarjo, Gresik, Mojokerto (5 kota Gerbangkertasusila) |
| **Interval polling** | Setiap 15 menit |
| **RSS Feed** | `https://rss.tempo.co/tag/polusi` |
| **Backup RSS** | `https://rss.kompas.com/feed/kompas.com/sains/environment` |
| **Kafka Topic 1** | `airquality-api` — key: nama kota |
| **Kafka Topic 2** | `airquality-rss` — key: hash URL |
| **HDFS Path** | `/data/airquality/api/` dan `/data/airquality/rss/` |

**3 Analisis Spark Wajib:**
1. **Distribusi kategori AQI:** klasifikasikan setiap event — Baik (0–50), Sedang (51–100), Tidak Sehat (101–150), Berbahaya (>150) — hitung persentase per kota
2. **Rata-rata AQI per kota per jam:** identifikasi jam puncak polusi menggunakan Spark SQL
3. **Kota dengan AQI terburuk:** ranking kota dari rata-rata AQI tertinggi ke terendah, sertakan jumlah event "Tidak Sehat" atau lebih buruk

**Fokus dashboard:** Tabel AQI per kota dengan indikator warna (hijau/kuning/oranye/merah) · Kategorisasi kondisi · Berita lingkungan

---

### Topik 4 — 📈 SahamMeter: Monitor Saham IDX & Berita Pasar Modal

**Skenario klien:** Wealth management company yang membutuhkan monitoring harga saham blue-chip IDX beserta konteks berita untuk laporan harian kepada nasabah.

**Pertanyaan bisnis yang harus dijawab:**
> *"Saham blue-chip mana yang paling aktif bergerak hari ini, dan berita apa yang kemungkinan mendorongnya?"*

| | Detail |
|-|--------|
| **API Real-time** | Yahoo Finance via library `yfinance` — **gratis, tanpa API key, install: `pip install yfinance`** |
| **Cara fetch** | `yf.Ticker("BBCA.JK").fast_info` untuk harga terkini, atau `yf.download("BBCA.JK BBRI.JK", period="1d", interval="5m")` |
| **Saham yang dipantau** | BBCA.JK, BBRI.JK, TLKM.JK, ASII.JK, BMRI.JK |
| **Interval polling** | Setiap 5 menit (sesuai jam bursa: 09.00–15.30 WIB) |
| **RSS Feed** | `https://rss.bisnis.com/feed/rss2/financial-market` |
| **Backup RSS** | `https://www.cnnindonesia.com/ekonomi/rss` |
| **Kafka Topic 1** | `saham-api` — key: ticker (BBCA, BBRI, TLKM, dst.) |
| **Kafka Topic 2** | `saham-rss` — key: hash URL |
| **HDFS Path** | `/data/saham/api/` dan `/data/saham/rss/` |

**3 Analisis Spark Wajib:**
1. **Return per saham:** hitung `(harga_terkini - harga_awal) / harga_awal * 100` — saham mana yang naik/turun paling banyak?
2. **Volatilitas intraday:** standar deviasi harga per saham — saham mana paling fluktuatif hari ini?
3. **Frekuensi sebutan di berita:** hitung kemunculan nama perusahaan (BCA, BRI, Telkom, Astra, Mandiri) dalam field `judul` artikel RSS

**Fokus dashboard:** Tabel saham + % return dengan warna · Ranking volatilitas · Artikel terbaru pasar modal

---

### Topik 5 — 🌍 NewsPulse: Analisis Tren Berita Nasional

**Skenario klien:** PR agency yang perlu memantau isu apa yang sedang paling banyak dibicarakan media nasional dan digital hari ini.

**Pertanyaan bisnis yang harus dijawab:**
> *"Topik apa yang paling hangat hari ini di berbagai media, dan jam berapa biasanya berita dominan muncul?"*

| | Detail |
|-|--------|
| **API Real-time** | GNews API — **gratis tier (100 req/hari), daftar di gnews.io** |
| **Endpoint hint** | `https://gnews.io/api/v4/top-headlines?country=id&lang=id&max=10&token=[key]` |
| **Alternatif** | NewsAPI.org — gratis tier 100 req/hari, daftar di newsapi.org |
| **Data yang diambil** | Judul, sumber, URL, waktu publikasi, deskripsi singkat berita |
| **Interval polling** | Setiap 10 menit |
| **RSS Feed 1** | `https://rss.kompas.com/feed/kompas.com/nasional` |
| **RSS Feed 2** | `https://rss.tempo.co/nasional` — polling keduanya dalam 1 producer |
| **Kafka Topic 1** | `news-api` — key: kategori berita |
| **Kafka Topic 2** | `news-rss` — key: hash URL, untuk menghindari duplikat antar dua RSS |
| **HDFS Path** | `/data/news/api/` dan `/data/news/rss/` |

**3 Analisis Spark Wajib:**
1. **Kata paling sering muncul di judul berita:** gunakan `split()` dan `explode()` di Spark SQL, filter stopwords sederhana (dan, yang, di, ke, dari, untuk, dengan)
2. **Distribusi berita per sumber:** berapa artikel dari Kompas vs Tempo vs GNews? — groupBy `sumber`
3. **Volume publikasi per jam:** pada jam berapa paling banyak berita dipublikasikan? — groupBy `HOUR(waktu_terbit)`

**Fokus dashboard:** Tabel kata trending (top 15) · Bar chart volume per sumber · Feed berita terbaru semua sumber

---

### Topik 6 — 🌋 GempaRadar: Monitor Aktivitas Seismik Wilayah Indonesia

**Skenario klien:** BPBD Provinsi yang membutuhkan sistem informasi gempa real-time untuk koordinasi respons kebencanaan.

**Pertanyaan bisnis yang harus dijawab:**
> *"Di wilayah mana aktivitas gempa paling tinggi dalam periode ini, dan seberapa sering gempa signifikan (M>4) terjadi?"*

| | Detail |
|-|--------|
| **API Real-time** | USGS Earthquake FDSN API — **gratis, tanpa API key** |
| **Endpoint hint** | `https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minlatitude=-11&maxlatitude=6&minlongitude=95&maxlongitude=141&minmagnitude=2&orderby=time&limit=20` |
| **Filter wilayah** | Koordinat bounding box mencakup seluruh wilayah Indonesia |
| **Interval polling** | Setiap 5 menit |
| **RSS Feed** | `https://www.bmkg.go.id/rss/gempa_m50.xml` |
| **Backup RSS** | `https://rss.tempo.co/tag/gempa-bumi` |
| **Kafka Topic 1** | `gempa-api` — key: ID gempa dari USGS (field `id` di GeoJSON) |
| **Kafka Topic 2** | `gempa-rss` — key: hash URL |
| **HDFS Path** | `/data/gempa/api/` dan `/data/gempa/rss/` |

**Hint parsing USGS GeoJSON:**
- Response berupa GeoJSON — ambil dari `response["features"]`, setiap item punya `properties.mag`, `properties.place`, `properties.time`, dan `geometry.coordinates` (lon, lat, depth)

**3 Analisis Spark Wajib:**
1. **Distribusi magnitudo:** kategorikan M < 3 (mikro), M 3–4 (minor), M 4–5 (sedang), M > 5 (kuat) — hitung jumlah tiap kategori
2. **Wilayah paling aktif:** groupBy `place` (ambil substring setelah "of " jika ada) — top 10 lokasi paling sering muncul
3. **Distribusi kedalaman:** rata-rata dan persebaran `depth` — apakah lebih banyak gempa dangkal (<70 km) atau dalam?

**Fokus dashboard:** Tabel gempa terbaru (mag, lokasi, waktu) · Statistik distribusi magnitudo · Berita gempa terbaru

---

### Topik 7 — 💻 GitTrend: Monitor Repositori Open Source Populer

**Skenario klien:** Newsletter teknologi bulanan yang membutuhkan kurasi otomatis repositori GitHub trending untuk edisi mingguan mereka.

**Pertanyaan bisnis yang harus dijawab:**
> *"Bahasa pemrograman apa yang paling tren minggu ini, dan tema proyek apa yang paling banyak digemari developer?"*

| | Detail |
|-|--------|
| **API Real-time** | GitHub REST API — **gratis tanpa auth: 10 req/menit; dengan Personal Access Token: 5000 req/jam** |
| **Endpoint hint** | `https://api.github.com/search/repositories?q=created:>[YYYYMMDD]&sort=stars&order=desc&per_page=30` — ganti tanggal secara dinamis (kemarin) |
| **Header** | Sertakan `Accept: application/vnd.github.v3+json`, dan opsional `Authorization: token [PAT]` untuk rate limit lebih longgar |
| **Data yang diambil** | `full_name`, `description`, `language`, `stargazers_count`, `topics`, `html_url` |
| **Interval polling** | Setiap 30 menit (data trending tidak berubah cepat) |
| **RSS Feed** | `https://techcrunch.com/feed/` |
| **Backup RSS** | `https://tekno.kompas.com/rss/` |
| **Kafka Topic 1** | `github-api` — key: `full_name` repositori |
| **Kafka Topic 2** | `github-rss` — key: hash URL |
| **HDFS Path** | `/data/github/api/` dan `/data/github/rss/` |

**3 Analisis Spark Wajib:**
1. **Distribusi bahasa pemrograman:** bahasa apa yang paling banyak digunakan repositori trending? — groupBy `language`, count, urutkan descending
2. **Top 10 repositori berdasarkan bintang:** tampilkan `full_name`, `language`, `stargazers_count`, potongan `description` — orderBy stars descending
3. **Kata paling sering di deskripsi/topik repo:** split deskripsi, hitung frekuensi kata (filter kata pendek <4 huruf) — identifikasi tema utama

**Fokus dashboard:** Leaderboard repo (nama + ⭐) · Pie/bar chart distribusi bahasa · Berita teknologi terbaru

---

### Topik 8 — 🛒 HargaPangan: Monitor Harga Komoditas Bahan Pokok

**Skenario klien:** Tim riset Bulog yang membutuhkan sistem early warning untuk memantau fluktuasi harga bahan pokok dan menghubungkannya dengan berita ekonomi.

**Pertanyaan bisnis yang harus dijawab:**
> *"Komoditas mana yang paling bergejolak harganya hari ini, dan apakah ada berita ekonomi yang menjelaskan penyebabnya?"*

| | Detail |
|-|--------|
| **API Real-time** | Pilih salah satu: **a)** Panel Harga Badanpangan: `https://panelharga.badanpangan.go.id/` (perlu inspect endpoint via DevTools) **b)** World Bank Commodity API: `https://api.worldbank.org/v2/en/indicator/PNRG_CS?downloadformat=json` **c)** Simulator realistis berbasis data historis (jika kedua API di atas tidak bisa diakses — **wajib didokumentasikan di README**) |
| **Komoditas** | Beras, Jagung, Kedelai, Gula, Minyak Goreng, Cabai, Bawang Merah, Telur |
| **Interval polling** | Setiap 30 menit |
| **RSS Feed** | `https://rss.bisnis.com/feed/rss2/ekonomi` |
| **Backup RSS** | `https://rss.kompas.com/feed/kompas.com/money` |
| **Kafka Topic 1** | `pangan-api` — key: nama komoditas |
| **Kafka Topic 2** | `pangan-rss` — key: hash URL |
| **HDFS Path** | `/data/pangan/api/` dan `/data/pangan/rss/` |

**3 Analisis Spark Wajib:**
1. **Volatilitas harga per komoditas:** hitung `(max_price - min_price) / avg_price * 100` sebagai indeks volatilitas relatif — ranking komoditas dari paling bergejolak
2. **Rata-rata harga per komoditas per periode:** tren harga dari waktu ke waktu (groupBy komoditas + jam/hari)
3. **Sebutan komoditas di berita:** hitung kemunculan nama komoditas ("beras", "cabai", "minyak") di judul artikel RSS — korelasi antara frekuensi berita dan perubahan harga

**Fokus dashboard:** Tabel harga + indikator naik/turun · Ranking volatilitas · Berita ekonomi terbaru

---

## 📁 Struktur Repository

```
[kelompok-X]-ets-bigdata/
├── README.md                    ← WAJIB: dokumentasi lengkap
├── docker-compose-hadoop.yml    ← dari materi P4
├── hadoop.env                   ← dari materi P4
├── docker-compose-kafka.yml     ← dari materi P8
│
├── kafka/
│   ├── producer_api.py          ← producer untuk API real-time
│   ├── producer_rss.py          ← producer untuk RSS feed
│   └── consumer_to_hdfs.py      ← consumer yang simpan ke HDFS
│
├── spark/
│   └── analysis.ipynb           ← notebook Spark (atau .py)
│
└── dashboard/
    ├── app.py                   ← Flask app
    ├── templates/
    │   └── index.html
    ├── static/
    │   └── style.css            ← opsional
    └── data/                    ← folder ini di .gitignore
        ├── spark_results.json
        ├── live_api.json
        └── live_rss.json
```

**README.md wajib berisi:**
- Nama anggota kelompok + kontribusi masing-masing
- Topik yang dipilih + justifikasi singkat (mengapa menarik)
- Diagram arsitektur (boleh gambar tangan yang difoto / draw.io)
- Cara menjalankan sistem step-by-step
- Screenshot: HDFS Web UI + Kafka consumer output + Dashboard berjalan
- Tantangan terbesar yang dihadapi dan cara mengatasinya

---

## 🗓️ Timeline (2 Minggu)

| Hari | Target |
|------|--------|
| 1–2 | Daftar topik ke dosen, setup Docker (Hadoop + Kafka), buat 2 topic Kafka |
| 3–4 | `producer_api.py` berjalan — event masuk ke Kafka |
| 5–6 | `producer_rss.py` berjalan — artikel RSS masuk ke Kafka |
| **7** | **Checkpoint Minggu 1:** Kafka menerima dari kedua sumber, verifikasi via console consumer |
| 8–9 | `consumer_to_hdfs.py` berjalan — file JSON muncul di HDFS |
| 10–11 | Spark `analysis.ipynb` — 3 analisis berjalan dari HDFS |
| 12 | `dashboard/app.py` — dashboard menampilkan data nyata |
| 13 | Testing end-to-end, perbaikan, finalisasi README |
| **14** | **Deadline:** Submit link GitHub ke LMS, persiapan demo |

---

## 📋 Deliverable

| # | Item | Ketentuan |
|---|------|-----------|
| 1 | **GitHub Repository** | Public, struktur folder sesuai, semua kode ada |
| 2 | **README.md** | Nama anggota, cara menjalankan, screenshot, refleksi tantangan |
| 3 | **Demo Live** | 10 menit (7 menit demo + 3 menit tanya jawab), sistem berjalan live saat demo |

> ⚠️ **Sistem harus bisa dijalankan** dari instruksi di README oleh orang lain. Demo menggunakan laptop kelompok sendiri.

---

## ⚖️ Rubrik Penilaian

> **Total: 100 poin + bonus 10 poin**

### Dimensi 1 — Apache Kafka (30 poin)

| Skor | Deskripsi |
|------|-----------|
| 27–30 | 2 topic aktif; producer API berjalan dengan interval yang benar dan event JSON-nya valid dan konsisten; producer RSS berjalan, menghindari duplikat, mengekstrak field bermakna; consumer group terdaftar; `--describe` menunjukkan LAG |
| 21–26 | Minimal 1 producer berjalan dengan baik; event valid; consumer ada tapi mungkin ada isu kecil |
| 14–20 | Kafka berjalan dan topic dibuat, tapi hanya dibuktikan via CLI; producer Python ada tapi output belum konsisten |
| 7–13 | Kafka berjalan, tapi producer minimal atau tidak berjalan otomatis |
| 0–6 | Kafka tidak berjalan atau tidak ada producer Python |

### Dimensi 2 — HDFS (25 poin)

| Skor | Deskripsi |
|------|-----------|
| 22–25 | File JSON tersimpan di `/data/[tema]/api/` dan `/data/[tema]/rss/`; `hdfs dfs -ls -R /data/[tema]/` membuktikan; Web UI screenshot di README; file dinamai dengan timestamp |
| 17–21 | Minimal 1 direktori berisi file; Hadoop berjalan dengan semua container; dokumentasi cukup |
| 11–16 | Hadoop berjalan, direktori ada, tapi consumer tidak menyimpan ke HDFS (hanya lokal) |
| 5–10 | Hadoop berjalan tapi tidak ada data yang tersimpan dari consumer |
| 0–4 | Hadoop tidak berjalan |

### Dimensi 3 — Apache Spark (30 poin)

| Skor | Deskripsi |
|------|-----------|
| 27–30 | 3 analisis wajib berjalan; Spark membaca dari HDFS (bukan file lokal); menggunakan DataFrame API dan Spark SQL; hasil tersimpan ke HDFS dan `spark_results.json`; setiap analisis ada narasi interpretasi bisnis |
| 21–26 | 2–3 analisis berjalan; HDFS digunakan sebagai sumber; interpretasi ada tapi ringkas |
| 14–20 | Spark berjalan tapi membaca dari file lokal, bukan HDFS; analisis ada |
| 7–13 | Hanya 1 analisis atau menggunakan Pandas/Python tanpa Spark |
| 0–6 | Spark tidak berjalan |

### Dimensi 4 — Dashboard (15 poin)

| Skor | Deskripsi |
|------|-----------|
| 13–15 | Flask di localhost:5000; 3 panel menampilkan data nyata dari Spark + Kafka; auto-refresh berjalan; terasa seperti sistem monitoring yang hidup |
| 9–12 | Dashboard berjalan; minimal 2 panel menampilkan data nyata |
| 5–8 | Dashboard ada tapi data statis atau hardcoded; tidak terhubung ke hasil pipeline |
| 0–4 | Tidak ada dashboard atau tidak bisa diakses |

### Rekap

| Dimensi | Bobot |
|---------|-------|
| 1. Apache Kafka | 30% |
| 2. HDFS | 25% |
| 3. Apache Spark | 30% |
| 4. Dashboard | 15% |
| **Total** | **100%** |

**Bonus (kumulatif, maks 10 poin):**
- **+5 poin:** Tambahkan satu analisis menggunakan **Spark MLlib** — misal prediksi tren (Linear Regression) atau clustering (K-Means) dari data historis HDFS
- **+3 poin:** Dashboard menampilkan **grafik/chart** berbasis data Spark (Chart.js atau Plotly.js)
- **+2 poin:** Consumer menyimpan ke HDFS menggunakan **library Python langsung** (bukan subprocess)

---

## ✅ Checklist Sebelum Demo

```
KAFKA:
[ ] docker compose (Kafka) berjalan — kafka-broker aktif
[ ] kafka-topics.sh --list menampilkan 2 topic [tema]-api dan [tema]-rss
[ ] producer_api.py berjalan dan output event terlihat di terminal
[ ] producer_rss.py berjalan dan output artikel terlihat di terminal
[ ] consumer_to_hdfs.py berjalan
[ ] kafka-consumer-groups.sh --describe menampilkan consumer group

HDFS:
[ ] docker compose (Hadoop) berjalan — 4 container aktif
[ ] hdfs dfs -ls /data/[tema]/api/ menampilkan file JSON
[ ] hdfs dfs -ls /data/[tema]/rss/ menampilkan file JSON
[ ] Screenshot HDFS Web UI (localhost:9870) ada di README

SPARK:
[ ] Analisis 1 berjalan tanpa error dari HDFS
[ ] Analisis 2 berjalan (Spark SQL)
[ ] Analisis 3 berjalan
[ ] hdfs dfs -ls /data/[tema]/hasil/ menampilkan output Spark
[ ] dashboard/data/spark_results.json ada

DASHBOARD:
[ ] python dashboard/app.py berjalan
[ ] localhost:5000 bisa dibuka di browser
[ ] Panel data Spark menampilkan data nyata (bukan placeholder)
[ ] Panel data live menampilkan event terbaru
[ ] Panel berita menampilkan artikel terbaru
[ ] Auto-refresh terbukti berjalan

REPOSITORY:
[ ] GitHub repo public
[ ] Semua file kode ada (tidak ada file yang "lupa di-push")
[ ] README berisi nama anggota + kontribusi + cara menjalankan + screenshot
[ ] Link repository sudah dikirim ke LMS sebelum deadline
```

---

## ❓ FAQ

**Q: API saya kena rate limit atau perlu berbayar. Apa yang harus dilakukan?**
> Buat **simulator data** yang menghasilkan angka realistis berdasarkan distribusi statistik (mean ± std) dari dataset publik yang relevan. Simulator ini tetap harus berjalan sebagai producer dan mengirim ke Kafka. Dokumentasikan pendekatan ini di README — nilai tidak dikurangi jika didokumentasikan dengan baik.

**Q: Boleh pakai Google Colab untuk Spark?**
> Boleh, tapi Spark harus membaca dari HDFS yang berjalan di Docker. Jika koneksi Colab ke HDFS lokal sulit, Spark boleh membaca file lokal sebagai alternatif, tapi catat keterbatasan ini di README. Nilai Dimensi 3 maks dikurangi 5 poin untuk kasus ini.

**Q: Bagaimana cara menyimpan ke HDFS dari Python?**
> Dua cara: **(a)** Simpan ke file lokal dulu, lalu jalankan `subprocess.run(["hdfs", "dfs", "-put", file_lokal, hdfs_path])` dari Python. **(b)** Gunakan library `hdfs` Python (`pip install hdfs`) dan `InsecureClient("http://localhost:9870")`. Cara (a) lebih mudah diimplementasikan.

**Q: Pembagian tugas yang disarankan?**

| Anggota | Tanggung Jawab |
|---------|----------------|
| Anggota 1 | Setup Docker (Hadoop & Kafka), buat topic, troubleshooting infrastruktur |
| Anggota 2 | `producer_api.py` — integrasi API eksternal |
| Anggota 3 | `producer_rss.py` + `consumer_to_hdfs.py` |
| Anggota 4 | `spark/analysis.ipynb` — 3 analisis wajib |
| Anggota 5 | `dashboard/app.py` + `index.html` |

> Untuk kelompok 4 orang: gabungkan Anggota 3 & 5, atau 2 & 3.

**Q: Setiap anggota harus punya kontribusi kode?**
> Ya. Tandai bagian kode yang dikerjakan dengan komentar `# [NamaAnggota]: ...` di awal setiap fungsi/blok utama. Ini juga memudahkan saat tanya jawab demo.

**Q: Kapan batas waktu mendaftarkan pilihan topik?**
> Maksimal **3 hari kerja setelah ETS dirilis**, melalui LMS atau langsung ke dosen. First come, first served — segera daftarkan sebelum topik pilihan habis.
