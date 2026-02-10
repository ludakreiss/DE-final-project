from src.pipeline.ingestion import load_raw_data
from src.pipeline.cleaning import clean_data
from src.pipeline.streaming import replay_hourly
from src.constants import PARQUET_PATH

import threading
import time
import subprocess
import sys


# Start the metrics consumer process in a separate Python process
def start_metrics_consumer():
    subprocess.Popen([sys.executable, "-m", "src.metrics.metrics_consumer"])


# Start the metrics engine process in a separate Python process
def start_metrics_engine():
    subprocess.Popen([sys.executable, "-m", "src.metrics.metrics"])


# Start the Streamlit UI after a short delay to allow backend services to initialize
def start_ui():
    time.sleep(3)
    subprocess.Popen([sys.executable, "-m", "streamlit", "run", "src/ui/dashboard.py"])


def main():
    print("Starting Redset Full System...")

    # Start background services using daemon threads
    threading.Thread(target=start_metrics_consumer, daemon=True).start()
    threading.Thread(target=start_metrics_engine, daemon=True).start()
    threading.Thread(target=start_ui, daemon=True).start()

    # Wait for services to be ready before sending data
    time.sleep(5)

    # Load raw data from the parquet file
    df = load_raw_data(PARQUET_PATH)
    print(f"Loaded {len(df)} raw rows")

    # Clean the raw data
    df = clean_data(df)
    print(f"{len(df)} rows remain after cleaning")

    # Stream the cleaned data to Kafka in hourly batches
    replay_hourly(
        df,
        hours=24,
        sleep_seconds=60
    )


if __name__ == "__main__":
    main()