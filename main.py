from src.ingestion import load_raw_data
from src.cleaning import clean_data
from src.streaming import replay_hourly


def main():
    print("Starting Redset streaming pipeline")

    #1.Load raw data
    df = load_raw_data("data/raw/sample_0.01.parquet")
    print(f"Loaded {len(df)} raw rows")

    #2.Clean data
    df = clean_data(df)
    print(f"{len(df)} rows remain after cleaning")

    #3.Stream data (1 hour -> 1 minute)
    replay_hourly(
        df,
        hours=24,
        sleep_seconds=1  #use 60 for final demo
    )


if __name__ == "__main__":
    main()
