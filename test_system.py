#!/usr/bin/env python
"""
Script untuk testing dan verifikasi semua komponen sistem NewsPulse
"""

import sys
import subprocess
from datetime import datetime
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
import json

def print_header(text):
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}\n")

def test_docker_containers():
    """Cek apakah semua container Docker berjalan"""
    print_header("1. Cek Docker Containers")
    
    required_containers = ['kafka-broker', 'zookeeper', 'namenode', 'datanode']
    
    try:
        result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], 
                              capture_output=True, text=True)
        running_containers = result.stdout.strip().split('\n')
        
        for container in required_containers:
            if container in running_containers:
                print(f"✅ {container} - Running")
            else:
                print(f"❌ {container} - NOT RUNNING")
                print(f"   ℹ️  Start dengan: docker-compose up -d")
        
    except Exception as e:
        print(f"❌ Error checking containers: {e}")

def test_kafka_topics():
    """Cek apakah Kafka topics sudah dibuat (menggunakan AdminClient tanpa kafka-topics.sh)"""
    print_header("2. Cek Kafka Topics")
    
    required_topics = ['news-api', 'news-rss']
    
    try:
        # Gunakan KafkaAdminClient untuk mengecek topics
        admin_client = KafkaAdminClient(
            bootstrap_servers='localhost:9092',
            client_id='test-admin'
        )
        
        # Dapatkan metadata topics
        cluster_metadata = admin_client.list_topics()
        topics = list(cluster_metadata.keys())
        
        for topic in required_topics:
            if topic in topics:
                print(f"✅ Topic '{topic}' - Created")
            else:
                print(f"❌ Topic '{topic}' - NOT CREATED")
                print(f"   ℹ️  Create dengan: python setup_kafka_topics.py")
        
        admin_client.close()
        
    except Exception as e:
        print(f"❌ Error checking topics: {e}")
        print(f"   ℹ️  Pastikan Kafka broker berjalan di localhost:9092")

def test_hdfs():
    """Cek apakah HDFS struktur ada"""
    print_header("3. Cek HDFS Structure")
    
    try:
        result = subprocess.run([
            'docker', 'exec', 'namenode',
            'hdfs', 'dfs', '-ls', '-R', '/data'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ HDFS /data directory exists")
            print("\nContent:")
            for line in result.stdout.strip().split('\n')[:5]:
                print(f"   {line}")
            if len(result.stdout.strip().split('\n')) > 5:
                print(f"   ... ({len(result.stdout.strip().split('\n')) - 5} more lines)")
        else:
            print("❌ HDFS /data directory not found (will be created on first data write)")
    
    except Exception as e:
        print(f"❌ Error checking HDFS: {e}")

def test_kafka_data():
    """Cek apakah ada data di Kafka topics"""
    print_header("4. Cek Data dalam Kafka Topics")
    
    for topic in ['news-api', 'news-rss']:
        try:
            result = subprocess.run([
                'docker', 'exec', 'kafka-broker',
                'kafka-console-consumer.sh',
                '--topic', topic,
                '--bootstrap-server', 'localhost:9092',
                '--from-beginning',
                '--max-messages', '1',
                '--consumer-property', 'fetch.min.bytes=1',
                '--consumer-property', 'fetch.max.wait.ms=100'
            ], capture_output=True, text=True, timeout=5)
            
            if result.stdout.strip():
                print(f"✅ Topic '{topic}' - Ada data")
                # Print preview (max 100 chars)
                preview = result.stdout.strip()[:100]
                print(f"   Preview: {preview}...")
            else:
                print(f"⚠️  Topic '{topic}' - Belum ada data (tunggu producer)")
        
        except subprocess.TimeoutExpired:
            print(f"⚠️  Topic '{topic}' - Timeout (belum ada data)")
        except Exception as e:
            print(f"⚠️  Topic '{topic}' - Error: {str(e)[:50]}")

def test_local_files():
    """Cek apakah local JSON files sudah ada"""
    print_header("5. Cek Local Data Files (Dashboard)")
    
    import os
    
    files = {
        'dashboard/data/live_api.json': 'Live API data',
        'dashboard/data/live_rss.json': 'Live RSS data',
        'dashboard/data/spark_results.json': 'Spark analysis results'
    }
    
    for filepath, description in files.items():
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            if size > 0:
                print(f"✅ {filepath} ({size} bytes) - {description}")
            else:
                print(f"⚠️  {filepath} (0 bytes - empty) - {description}")
        else:
            print(f"❌ {filepath} - NOT FOUND")

def print_instructions():
    """Print setup instructions"""
    print_header("SETUP CHECKLIST")
    
    print("""
1. Start Docker Containers (if not running):
   docker-compose up -d

2. Create Kafka Topics (one-time, using Python script):
   python setup_kafka_topics.py
   
   (This uses kafka-python AdminClient, no shell script dependency)

3. Start Producer API (Terminal 1):
   cd kafka && python producer_api.py

4. Start Producer RSS (Terminal 2):
   cd kafka && python producer_rss.py

5. Start Consumer to HDFS (Terminal 3):
   cd hdfs && python consumer_to_hdfs.py
   (Wait 60 seconds for first data write)

6. Run Spark Analysis (Terminal 4):
   cd spark && python analysis.py

7. Start Dashboard (Terminal 5):
   cd dashboard && python app.py
   Then open: http://localhost:5000

8. Monitor Kafka (using Python, no shell script dependency):
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
for message in consumer:
    print(f'[{message.topic}] {message.value.get(\"title\", \"\")[:60]}...')
"
""")

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"  NewsPulse System - Diagnostics & Testing")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    test_docker_containers()
    test_kafka_topics()
    test_hdfs()
    test_kafka_data()
    test_local_files()
    print_instructions()
    
    print(f"\n{'='*60}")
    print(f"  ✅ Diagnostics Complete")
    print(f"{'='*60}\n")
