import json
import time
import feedparser
import hashlib
from kafka import KafkaProducer
from datetime import datetime
import sys

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'news-rss'
RSS_FEEDS = [
    {
        "name": "Kompas",
        "urls": [
            "https://rss.kompas.com/feed/kompas.com/nasional"
        ]
    },
    {
        "name": "Tempo",
        "urls": [
            "https://rss.tempo.co/nasional"
        ]
    },
    {
        "name": "CNN Indonesia",
        "urls": [
            "https://www.cnnindonesia.com/nasional/rss"
        ]
    }
]

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks="all",
        retries=3
    )
    print(f"[{datetime.now()}] ✅ Koneksi Kafka berhasil")
except Exception as e:
    print(f"[{datetime.now()}] ❌ Gagal koneksi Kafka: {e}")
    sys.exit(1)

sent_urls = set() # Untuk menghindari duplikat dalam satu session

def fetch_and_send():
    total_sent = 0
    for feed_config in RSS_FEEDS:
        source_name = feed_config["name"]
        source_sent = 0
        feed_loaded = False

        for feed_url in feed_config["urls"]:
            try:
                print(f"[{datetime.now()}] Parsing feed ({source_name}): {feed_url}")
                feed = feedparser.parse(feed_url)

                if feed.bozo:  # Peringatan parsing error
                    print(f"[{datetime.now()}] ⚠️  Warning parsing feed {feed_url}: {feed.bozo_exception}")

                if not feed.entries:
                    print(f"[{datetime.now()}] ⚠️  Tidak ada entries di feed: {feed_url}")
                    continue

                feed_loaded = True
                for entry in feed.entries:
                    try:
                        article_url = entry.get('link', '')
                        if not article_url or article_url in sent_urls:
                            continue

                        data = {
                            'title': entry.get('title', ''),
                            'source': source_name,
                            'url': article_url,
                            'summary': entry.get('summary', ''),
                            'timestamp': entry.get('published', datetime.now().isoformat())
                        }

                        url_hash = hashlib.md5(article_url.encode('utf-8')).hexdigest()[:8]
                        producer.send(TOPIC, key=url_hash, value=data)
                        sent_urls.add(article_url)
                        source_sent += 1
                        total_sent += 1
                    except Exception as e:
                        print(f"[{datetime.now()}] ❌ Error processing entry: {e}")
                        continue

                # Jika satu URL source berhasil dibaca, tidak perlu fallback URL lain.
                break

            except Exception as e:
                print(f"[{datetime.now()}] ❌ Error fetching feed {feed_url}: {e}")
                continue

        if not feed_loaded:
            print(f"[{datetime.now()}] ⚠️  Semua endpoint RSS gagal untuk source {source_name}")
        else:
            print(f"[{datetime.now()}] ✅ Terkirim {source_sent} artikel RSS dari {source_name}")

    if total_sent > 0:
        producer.flush()
        print(f"[{datetime.now()}] ✅ Total {total_sent} artikel RSS terkirim ke Kafka")
    else:
        print(f"[{datetime.now()}] ℹ️  Tidak ada artikel baru")

if __name__ == "__main__":
    print(f"[{datetime.now()}] 🚀 Producer RSS dimulai")
    while True:
        try:
            fetch_and_send()
        except KeyboardInterrupt:
            print(f"\n[{datetime.now()}] ⏹️  Producer RSS dihentikan")
            break
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Error in main loop: {e}")
        
        time.sleep(300) # Polling setiap 5 menit