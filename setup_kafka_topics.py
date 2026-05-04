"""
Script untuk membuat Kafka topics secara programmatic
Menghindari masalah: kafka-topics.sh: command not found
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from datetime import datetime
import sys

KAFKA_BROKER = 'localhost:9092'
TOPICS = [
    {'name': 'news-api', 'partitions': 1, 'replication_factor': 1},
    {'name': 'news-rss', 'partitions': 1, 'replication_factor': 1}
]

def create_topics():
    """Membuat Kafka topics secara programmatic"""
    print(f"[{datetime.now()}] 🚀 Membuat Kafka Topics...")
    print(f"[{datetime.now()}] 📍 Kafka Broker: {KAFKA_BROKER}\n")
    
    try:
        # Buat admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id='topic_creator'
        )
        print(f"[{datetime.now()}] ✅ Admin client berhasil terhubung ke Kafka\n")
        
        # Ambil topics yang sudah ada agar tidak melempar TopicAlreadyExistsError
        existing_topics = set(admin_client.list_topics())

        # Siapkan list topics untuk dibuat
        topic_list = []
        for topic_config in TOPICS:
            if topic_config['name'] in existing_topics:
                print(f"[{datetime.now()}] ℹ️  Topic '{topic_config['name']}' sudah ada (skip)")
                continue

            new_topic = NewTopic(
                name=topic_config['name'],
                num_partitions=topic_config['partitions'],
                replication_factor=topic_config['replication_factor']
            )
            topic_list.append(new_topic)
        
        # Buat topics
        print(f"[{datetime.now()}] 📝 Membuat {len(topic_list)} topics:\n")

        if not topic_list:
            print(f"[{datetime.now()}] ✅ Semua topic target sudah tersedia, tidak ada yang perlu dibuat")
            create_response = None
        else:
            create_response = admin_client.create_topics(new_topics=topic_list, validate_only=False)

        # create_topics() bisa mengembalikan bentuk response yang berbeda
        # tergantung versi kafka-python / broker.
        if create_response is None:
            pass
        elif hasattr(create_response, "topic_errors"):
            for topic_name, error_code, error_message in create_response.topic_errors:
                if error_code == 0:
                    print(f"[{datetime.now()}] ✅ Topic '{topic_name}' berhasil dibuat")
                elif error_code == 36:
                    print(f"[{datetime.now()}] ℹ️  Topic '{topic_name}' sudah ada (skip)")
                else:
                    message = error_message or f"kode error Kafka {error_code}"
                    print(f"[{datetime.now()}] ❌ Error membuat topic '{topic_name}': {message}")
        elif isinstance(create_response, dict):
            # Kompatibilitas untuk versi lama yang mengembalikan futures map
            for topic_name, future in create_response.items():
                try:
                    future.result()
                    print(f"[{datetime.now()}] ✅ Topic '{topic_name}' berhasil dibuat")
                except TopicAlreadyExistsError:
                    print(f"[{datetime.now()}] ℹ️  Topic '{topic_name}' sudah ada (skip)")
                except Exception as e:
                    print(f"[{datetime.now()}] ❌ Error membuat topic '{topic_name}': {e}")
        else:
            print(f"[{datetime.now()}] ⚠️  Format response create_topics tidak dikenali: {type(create_response)}")
        
        # Verifikasi topics
        print(f"\n[{datetime.now()}] 🔍 Verifikasi topics yang ada:")
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKER)
        topics = consumer.topics()
        
        for topic in sorted(topics):
            if topic.startswith('news-'):
                print(f"[{datetime.now()}] ✅ {topic}")
        
        consumer.close()
        admin_client.close()
        
        print(f"\n[{datetime.now()}] ✨ Setup Kafka topics selesai!")
        return True
        
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = create_topics()
    sys.exit(0 if success else 1)
