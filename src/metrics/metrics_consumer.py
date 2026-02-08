from confluent_kafka import Consumer
import json
import pandas as pd
import os
import time
from src.constants import CLEANED_PATH, KAFKA_BROKER, KAFKA_TOPIC, KAFKA_GROUP_ID




def consume_data():

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([KAFKA_TOPIC])  # changed from redset-replay

    os.makedirs(os.path.dirname(CLEANED_PATH), exist_ok=True)

    topic_data = []
    last_write = time.time()

    print("Metrics consumer started...")

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                # print("No message yet") # used for debugging
                pass


            if msg is not None and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))
                topic_data.append(data)

            # write once per minute (1 hour batch)
            if time.time() - last_write >= 60 and topic_data:
                hour_df = pd.DataFrame(topic_data)

                if os.path.exists(CLEANED_PATH):



                    existing = pd.read_parquet(CLEANED_PATH)



                    hour_df = pd.concat([existing, hour_df], ignore_index=True)

                hour_df.to_parquet(CLEANED_PATH, index=False)
                print(f"Wrote {len(hour_df)} rows")
                print(f"Buffered messages: {len(topic_data)}")


                topic_data = []
                last_write = time.time()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_data()