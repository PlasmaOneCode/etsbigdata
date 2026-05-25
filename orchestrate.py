import subprocess
import sys
import time
import logging

# Konfigurasi log agar Anda bisa memantau hasilnya di terminal dan file log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("lakehouse_pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Daftar script yang harus dijalankan secara berurutan
pipeline_stages = [
    ("Bronze Ingestion", "lakehouse/01_bronze.py"),
    ("Silver Cleaning", "lakehouse/02_silver.py"),
    ("Gold Aggregation", "lakehouse/03_gold.py")
]

def run_pipeline():
    logging.info("=" * 60)
    logging.info("MEMULAI PIPELINE LAKEHOUSE")
    logging.info("=" * 60)
    
    for stage_name, script_path in pipeline_stages:
        logging.info(f"-> Memulai Tahap: {stage_name} ({script_path})")
        start_time = time.time()
        
        # Menjalankan script menggunakan interpreter python yang sedang aktif
        # subprocess.run secara default akan menunggu script selesai sebelum lanjut
        result = subprocess.run([sys.executable, script_path])
        
        elapsed_time = time.time() - start_time
        
        # Mengecek exit code dari script
        if result.returncode == 0:
            logging.info(f"✓ {stage_name} SELESAI dalam {elapsed_time:.2f} detik.\n")
        else:
            logging.error(f"✗ {stage_name} GAGAL (Exit Code: {result.returncode}).")
            logging.error("Menghentikan seluruh pipeline untuk mencegah korupsi data downstream!")
            logging.info("=" * 60)
            return False
            
    logging.info("=" * 60)
    logging.info("✓ SELURUH PIPELINE LAKEHOUSE BERHASIL DISELESAIKAN")
    logging.info("=" * 60)
    return True

if __name__ == "__main__":
    # --- Mode Standby Daemon (Looping Internal) ---
    # Sangat berguna jika dijalankan langsung di lokal PC tanpa cron eksternal
    try:
        while True:
            success = run_pipeline()
            next_run = 120  # interval dalam menit
            logging.info(f"Menunggu {next_run} menit untuk eksekusi berikutnya...")
            time.sleep(next_run * 60)
    except KeyboardInterrupt:
        logging.info("Orchestrator dihentikan oleh pengguna.")