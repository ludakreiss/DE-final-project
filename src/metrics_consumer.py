from confluent_kafka import Consumer
import pandas as pd


def consume_data(kafka_broker: str = "localhost:9092",
                 topic: str = "redset-replay"):

    """
    takes cleaned data from the kafka topic: redset-replay
    """
    # print("This is mertics_consumer.py")

    consumer_config = {
        "bootstrap.servers": kafka_broker,
        "group.id": "redset-group",
        "auto.offset.reset": "earliest"
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    # list for saving the event values
    topic_data = []

    # reading json data in a loop

    try:
        while True:
            event = consumer.poll(1.0)  # waiting for 1sec to receive json event

            if event is None:
                continue

            if event.error():
                print(f"Kafka error: {event.error()}")
                continue

            value = event.value()  # .decode("utf-8") (only if encoded)
            print("Received message:", value)
            topic_data.append(value)

    except Exception as e:
        print("The exception is:", e)

    topic_df = pd.DataFrame(topic_data)
    topic_df.to_parquet("data/consumed/cleaned_consumed.parquet")
