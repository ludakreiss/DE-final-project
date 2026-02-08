from src.pipeline.ingestion import load_raw_data
from src.pipeline.cleaning import clean_data
from src.pipeline.streaming import replay_hourly
from src.constants import PARQUET_PATH

import threading
import time
import subprocess
import sys

def start_metrics_consumer():
    subprocess.Popen([sys.executable, "-m", "src.metrics.metrics_consumer"])

def start_metrics_engine():
    subprocess.Popen([sys.executable, "-m", "src.metrics.metrics"])

def start_ui():
    time.sleep(3)
    subprocess.Popen([sys.executable, "-m", "streamlit", "run", "src/ui/dashboard.py"])

def main():
    print("Starting Redset Full System...")

    threading.Thread(target=start_metrics_consumer, daemon=True).start()
    threading.Thread(target=start_metrics_engine, daemon=True).start()
    threading.Thread(target=start_ui, daemon=True).start()

    time.sleep(5)

    # 4) Load raw data
    df = load_raw_data(PARQUET_PATH)
    print(f"Loaded {len(df)} raw rows")

    # 5) Clean data
    df = clean_data(df)
    print(f"{len(df)} rows remain after cleaning")

    # 6) Stream data to Kafka
    replay_hourly(
        df,
        hours=24,
        sleep_seconds=60
    )


if __name__ == "__main__":
    main()