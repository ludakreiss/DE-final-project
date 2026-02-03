from src.ingestion import load_raw_data
from src.cleaning import clean_data
from src.streaming import replay_hourly

import threading
import os
import time

def start_metrics_consumer():
    os.system("python metrics/metrics_consumer.py")

def start_metrics_engine():
    os.system("python metrics/metrics.py")

def start_ui():
    time.sleep(3)
    os.system("streamlit run ui/dashboard.py")

def main():
    print("Starting Redset Full System...")

    threading.Thread(target=start_metrics_consumer, daemon=True).start()
    threading.Thread(target=start_metrics_engine, daemon=True).start()
    threading.Thread(target=start_ui, daemon=True).start()

    time.sleep(5)

    # 4) Load raw data
    df = load_raw_data("data/raw/sample_0.01.parquet")
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