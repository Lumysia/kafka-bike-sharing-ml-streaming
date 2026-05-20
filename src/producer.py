import json
import os
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

from src.config import DATA_PATH, RAW_TOPIC, confluent_config
from src.train_model import FEATURES, ensure_dataset


def on_delivery(err, msg):
    if err:
        print(f"Delivery failed: {err}")
        return
    print(f"sent row={msg.key().decode()} partition={msg.partition()} offset={msg.offset()}")


def build_event(row):
    event = {feature: float(row[feature]) for feature in FEATURES}
    integer_fields = [
        "season",
        "yr",
        "mnth",
        "hr",
        "holiday",
        "weekday",
        "workingday",
        "weathersit",
    ]
    for integer_field in integer_fields:
        event[integer_field] = int(event[integer_field])
    event["instant"] = int(row["instant"])
    event["datetime"] = row["dteday"]
    event["actual_count"] = int(row["cnt"])
    return event


def main():
    ensure_dataset()
    if not Path(DATA_PATH).exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_PATH}. Run uv run python -m src.train_model first."
        )

    df = pd.read_csv(DATA_PATH)
    stream_limit = int(os.getenv("STREAM_LIMIT", "0"))
    stream_delay = float(os.getenv("STREAM_DELAY", "1"))
    if stream_limit > 0:
        df = df.head(stream_limit)

    producer = Producer(confluent_config())

    print(
        f"Streaming {len(df)} Bike Sharing rows to Kafka topic "
        f"'{RAW_TOPIC}' with {stream_delay}s delay"
    )
    for _, row in df.iterrows():
        event = build_event(row)
        producer.produce(
            topic=RAW_TOPIC,
            key=str(event["instant"]),
            value=json.dumps(event),
            callback=on_delivery,
        )
        producer.poll(0)
        time.sleep(stream_delay)

    producer.flush()


if __name__ == "__main__":
    main()
