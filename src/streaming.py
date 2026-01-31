import json
import time
from datetime import timedelta
import pandas as pd
from confluent_kafka import Producer


def replay_hourly(
    df: pd.DataFrame,
    hours: int = 24,
    sleep_seconds: int = 60,
    kafka_broker: str = "localhost:9092",
    topic: str = "redset-replay",
):

    producer = Producer({"bootstrap.servers": kafka_broker})

    start_ts = df["arrival_timestamp"].min()
    end_ts = start_ts + timedelta(hours=hours)

    df = df[
        (df["arrival_timestamp"] >= start_ts) &
        (df["arrival_timestamp"] < end_ts)
    ]

    df = df.copy()
    df.loc[:, "hour_bucket"] = df["arrival_timestamp"].dt.floor("h")

    hour_groups = df.groupby("hour_bucket")

    print(f"Starting replay for {len(hour_groups)} hours")

    for hour_index, (hour, hour_df) in enumerate(hour_groups):
        print(f"Replaying hour {hour_index}: {hour}")

        # ✅ send rows normally (Kafka safe)
        for _, row in hour_df.iterrows():
            producer.produce(
                topic,
                value=json.dumps(row.to_dict(), default=str)
            )

        producer.flush()

        if hour_index < hours - 1:
            time.sleep(sleep_seconds)

    print("Streaming finished.")