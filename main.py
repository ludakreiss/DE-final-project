from src.ingestion import load_raw_data
from src.cleaning import clean_data
from src.streaming import replay_hourly

import threading
import os
import time


# Start Metrics Consumer
def start_metrics():
    os.system("python metrics/metrics_consumer.py")


# Start UI
def start_ui():
    time.sleep(3)  # give consumer time to start
    os.system("streamlit run ui/dashboard.py")


def main():
    print("Starting Redset Full System...")

    # 1) Start metrics consumer in background
    threading.Thread(target=start_metrics, daemon=True).start()

    # 2) Start UI in background
    threading.Thread(target=start_ui, daemon=True).start()

    # 3) Give them a moment to boot
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