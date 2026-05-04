import json
import time
import requests
import os
from kafka import KafkaProducer
from datetime import datetime
import sys

# Konfigurasi
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'news-api'
GNEWS_TOKEN = os.getenv('GNEWS_TOKEN', '1033e1bad9dc6b4de9a3651a109b14f2')
URL = f"https://gnews.io/api/v4/top-headlines?country=id&lang=id&max=10&token={GNEWS_TOKEN}"

# Timeout untuk requests
REQUEST_TIMEOUT = 10

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks="all",
        retries=3
    )
    print(f"[{datetime.now()}] ✅ Koneksi Kafka berhasil")
except Exception as e:
    print(f"[{datetime.now()}] ❌ Gagal koneksi Kafka: {e}")
    sys.exit(1)

def fetch_and_send():
    try:
        print(f"[{datetime.now()}] 🔄 Fetching data dari GNews API...")
        response = requests.get(URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        articles = data.get('articles', [])
        
        if not articles:
            print(f"[{datetime.now()}] ⚠️  Tidak ada artikel dari API")
            return
        
        total_sent = 0
        for article in articles:
            try:
                # Format data konsisten
                data_item = {
                    'title': article.get('title', ''),
                    'source': article.get('source', {}).get('name', 'GNews'),
                    'category': article.get('topic', 'umum'),
                    'url': article.get('url', ''),
                    'summary': article.get('description', ''),
                    'timestamp': article.get('publishedAt', datetime.now().isoformat())
                }
                
                # Validasi data
                if not data_item['title'] or not data_item['url']:
                    continue
                
                # Key berdasarkan kategori berita (sesuai ketentuan ETS)
                key = data_item.get('category') or 'umum'
                producer.send(TOPIC, key=key, value=data_item)
                total_sent += 1
                
            except Exception as e:
                print(f"[{datetime.now()}] ⚠️  Error processing article: {e}")
                continue
        
        if total_sent > 0:
            producer.flush()
            print(f"[{datetime.now()}] ✅ Terkirim {total_sent} artikel dari GNews ke Kafka")
        else:
            print(f"[{datetime.now()}] ⚠️  Tidak ada artikel valid untuk dikirim")
            
    except requests.exceptions.Timeout:
        print(f"[{datetime.now()}] ❌ Request timeout dari GNews API")
    except requests.exceptions.ConnectionError:
        print(f"[{datetime.now()}] ❌ Koneksi error dengan GNews API")
    except requests.exceptions.HTTPError as e:
        print(f"[{datetime.now()}] ❌ HTTP error dari GNews API: {e.response.status_code}")
        if e.response.status_code == 401:
            print(f"[{datetime.now()}] ⚠️  Token GNews tidak valid atau sudah expired")
    except json.JSONDecodeError as e:
        print(f"[{datetime.now()}] ❌ Error parsing JSON dari API: {e}")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error tidak terduga: {e}")

if __name__ == "__main__":
    print(f"[{datetime.now()}] 🚀 Producer API (GNews) dimulai")
    print(f"[{datetime.now()}] 🔑 Menggunakan token: {GNEWS_TOKEN[:20]}...")
    
    while True:
        try:
            fetch_and_send()
        except KeyboardInterrupt:
            print(f"\n[{datetime.now()}] ⏹️  Producer API dihentikan")
            break
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Error dalam main loop: {e}")
        
        time.sleep(600)  # Polling setiap 10 menit