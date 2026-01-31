from confluent_kafka import Consumer
import json
import pandas as pd
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_PATH = os.path.join(BASE_DIR, "data", "consumed", "cleaned_consumed.parquet")


def consume_data():

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "redset-group",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe(["redset-replay"])

    os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)

    topic_data = []
    last_write = time.time()

    print("Metrics consumer started...")

    try:
        while True:
            msg = consumer.poll(0.1)

            if msg is not None and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))
                topic_data.append(data)

            # ✅ write once per minute (1 hour batch)
            if time.time() - last_write >= 60 and topic_data:
                hour_df = pd.DataFrame(topic_data)

                if os.path.exists(PARQUET_PATH):
                    existing = pd.read_parquet(PARQUET_PATH)
                    hour_df = pd.concat([existing, hour_df], ignore_index=True)

                hour_df.to_parquet(PARQUET_PATH, index=False)
                print(f"Wrote {len(hour_df)} rows")

                topic_data = []
                last_write = time.time()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_data()