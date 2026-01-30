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
    """
    Replay query workload hour-by-hour.
    One hour of real data is replayed in `sleep_seconds`.
    """

    producer = Producer({"bootstrap.servers": kafka_broker})

    # select first 24 hours
    start_ts = df["arrival_timestamp"].min()
    end_ts = start_ts + timedelta(hours=hours)

    df = df[
        (df["arrival_timestamp"] >= start_ts) &
        (df["arrival_timestamp"] < end_ts)
    ]

    # bucket queries by hour
    df["hour_bucket"] = df["arrival_timestamp"].dt.floor("h")
    hour_groups = df.groupby("hour_bucket")

    print(f"Starting replay for {len(hour_groups)} hours")

    for hour_index, (hour, hour_df) in enumerate(hour_groups):
        print(
            f"Replaying hour {hour_index}: {hour} "
            f"({len(hour_df)} queries)"
        )

        # send all queries from this hour
    for _, row in hour_df.iterrows():
        event = row.to_dict()

        json_event = json.dumps(event, default=str)

        producer.produce(
            topic,
            value=json_event
        )

    if hour_index < hours - 1:
        time.sleep(sleep_seconds)
    producer.flush()
    print("Streaming finished.")
