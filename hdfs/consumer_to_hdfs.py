import json
import time
import os
import subprocess
from kafka import KafkaConsumer
from datetime import datetime
import sys

# Konfigurasi
KAFKA_BROKER = 'localhost:9092'
DASHBOARD_DIR = "../dashboard/data"

def ensure_dashboard_dir():
    """Memastikan folder dashboard/data ada"""
    os.makedirs(DASHBOARD_DIR, exist_ok=True)

def save_to_hdfs(data_list, topic_name):
    if not data_list:
        return
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    local_file = f"{topic_name}_{timestamp}.json"
    
    try:
        # 1. Simpan ke file lokal sementara di Windows
        print(f"[{datetime.now()}] 💾 Menyimpan {len(data_list)} items ke file lokal: {local_file}")
        with open(local_file, 'w', encoding='utf-8') as f:
            for item in data_list:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        # 2. Simpan salinan untuk dashboard live (akumulasi artikel lama + baru)
        MAX_LIVE_ITEMS = 50  # Maksimal artikel disimpan di live JSON
        dashboard_file = os.path.join(DASHBOARD_DIR, f"live_{topic_name.split('-')[1]}.json")
        print(f"[{datetime.now()}] 📊 Mengakumulasi live data ke: {dashboard_file}")
        os.makedirs(os.path.dirname(dashboard_file), exist_ok=True)

        # Baca data lama jika ada
        existing_items = []
        if os.path.exists(dashboard_file):
            try:
                with open(dashboard_file, 'r', encoding='utf-8') as f_old:
                    existing_items = json.load(f_old)
                    if not isinstance(existing_items, list):
                        existing_items = []
            except Exception:
                existing_items = []

        # Gabungkan data lama + baru, deduplikasi by URL (URL baru menang)
        seen_urls = {}
        for item in existing_items + data_list:
            url_key = item.get('url') or item.get('title', '')
            seen_urls[url_key] = item

        # Urutkan by timestamp terbaru, ambil MAX_LIVE_ITEMS
        merged = sorted(
            seen_urls.values(),
            key=lambda x: x.get('timestamp', ''),
            reverse=True
        )[:MAX_LIVE_ITEMS]

        with open(dashboard_file, 'w', encoding='utf-8') as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        
        print(f"[{datetime.now()}] ✅ Live data tersimpan: {dashboard_file}")
        
        # 3. Pindah ke HDFS menggunakan jembatan Docker
        hdfs_dir = f"/data/news/{'api' if 'api' in topic_name else 'rss'}"
        
        try:
            # a. Copy file dari laptop Windows ke dalam OS container namenode
            cmd_copy = ["docker", "cp", local_file, f"namenode:/{local_file}"]
            result = subprocess.run(cmd_copy, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"[{datetime.now()}] ⚠️  Warning docker cp: {result.stderr}")
            
            # b. Buat folder di HDFS (kalau belum ada)
            cmd_mkdir = ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir]
            subprocess.run(cmd_mkdir, capture_output=True, check=False)
            
            # c. Masukkan file dari OS container ke dalam sistem HDFS
            cmd_put = ["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", f"/{local_file}", f"{hdfs_dir}/{local_file}"]
            result = subprocess.run(cmd_put, capture_output=True, text=True, check=False)
            
            if result.returncode == 0:
                print(f"[{datetime.now()}] ✅ HDFS: Berhasil menyimpan {len(data_list)} items ke {hdfs_dir}/{local_file}")
            else:
                print(f"[{datetime.now()}] ⚠️  HDFS error (non-critical): {result.stderr}")
            
            # d. Bersihkan file sampah di dalam OS container
            subprocess.run(["docker", "exec", "namenode", "rm", f"/{local_file}"], capture_output=True, check=False)
            
        except Exception as hdfs_error:
            print(f"[{datetime.now()}] ⚠️  HDFS error (lanjut): {hdfs_error}")
        
        # 4. Hapus file lokal di Windows
        if os.path.exists(local_file):
            os.remove(local_file)
            
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error dalam save_to_hdfs: {e}")

def start_consumer():
    """Mulai consumer yang membaca dari 2 topic Kafka dengan flush periodik"""
    ensure_dashboard_dir()

    buffer_api = []
    buffer_rss = []
    last_save_time = time.time()

    print(f"[{datetime.now()}] 🚀 Consumer dimulai, membaca 2 topic Kafka: news-api, news-rss")
    print(f"[{datetime.now()}] ⏱️  Akan menyimpan setiap 60 detik (even with low traffic)...")

    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                'news-api', 'news-rss',
                bootstrap_servers=KAFKA_BROKER,
                group_id='hdfs_writer_group',
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=30000,
                consumer_timeout_ms=10000  # Timeout every 10s to check flush timer
            )
            print(f"[{datetime.now()}] ✅ Koneksi Kafka berhasil")

            for message in consumer:
                try:
                    if message.topic == 'news-api':
                        buffer_api.append(message.value)
                        if len(buffer_api) % 5 == 0:
                            print(f"[{datetime.now()}] 📥 API buffer: {len(buffer_api)} items")
                    elif message.topic == 'news-rss':
                        buffer_rss.append(message.value)
                        if len(buffer_rss) % 5 == 0:
                            print(f"[{datetime.now()}] 📥 RSS buffer: {len(buffer_rss)} items")

                except json.JSONDecodeError as e:
                    print(f"[{datetime.now()}] ⚠️  Error decode message: {e}")
                    continue
                except Exception as e:
                    print(f"[{datetime.now()}] ⚠️  Error processing message: {e}")
                    continue
                
                # Check flush timer every iteration (not just when messages arrive)
                current_time = time.time()
                if current_time - last_save_time >= 60:
                    if buffer_api:
                        save_to_hdfs(buffer_api, 'news-api')
                    if buffer_rss:
                        save_to_hdfs(buffer_rss, 'news-rss')

                    buffer_api.clear()
                    buffer_rss.clear()
                    last_save_time = current_time

        except KeyboardInterrupt:
            print(f"\n[{datetime.now()}] ⏹️  Consumer dihentikan oleh user")
            # Flush remaining buffered data on shutdown
            if buffer_api:
                print(f"[{datetime.now()}] 💾 Flushing {len(buffer_api)} buffered API items on shutdown...")
                save_to_hdfs(buffer_api, 'news-api')
            if buffer_rss:
                print(f"[{datetime.now()}] 💾 Flushing {len(buffer_rss)} buffered RSS items on shutdown...")
                save_to_hdfs(buffer_rss, 'news-rss')
            break
        except ValueError as e:
            # Workaround isu kafka-python di Python 3.14: "Invalid file descriptor: -1"
            if "Invalid file descriptor" in str(e):
                print(f"[{datetime.now()}] ⚠️  Koneksi consumer terputus, mencoba reconnect 3 detik lagi...")
                time.sleep(3)
                continue
            print(f"[{datetime.now()}] ❌ Error consumer: {e}")
            time.sleep(3)
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Error consumer: {e}")
            time.sleep(3)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass

    # Simpan buffer yang tersisa
    if buffer_api:
        save_to_hdfs(buffer_api, 'news-api')
    if buffer_rss:
        save_to_hdfs(buffer_rss, 'news-rss')
    print(f"[{datetime.now()}] ✅ Consumer ditutup")

if __name__ == "__main__":
    start_consumer()