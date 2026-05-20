import json
import os

from confluent_kafka import Consumer

from src.config import PREDICTIONS_TOPIC, confluent_config


def main():
    consumer_limit = int(os.getenv("CONSUMER_LIMIT", "0"))
    consumer_group = os.getenv("CONSUMER_GROUP", "bike-sharing-output-consumer")
    consumer = Consumer(
        {
            **confluent_config(),
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([PREDICTIONS_TOPIC])

    print(f"Listening for predictions on topic '{PREDICTIONS_TOPIC}'", flush=True)
    print("instant | hour | predicted | actual | abs_error", flush=True)
    print("-" * 52, flush=True)

    received = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                continue

            event = json.loads(msg.value())
            print(
                f"{event['instant']:>7} | {event['hour']:>4} | "
                f"{event['predicted_count']:>9} | {event['actual_count']:>6} | "
                f"{event['absolute_error']:>9}",
                flush=True,
            )
            received += 1
            if consumer_limit > 0 and received >= consumer_limit:
                break
    except KeyboardInterrupt:
        print("Stopping output consumer", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
