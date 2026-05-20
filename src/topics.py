from confluent_kafka.admin import AdminClient, NewTopic

from src.config import RAW_TOPIC, PREDICTIONS_TOPIC, confluent_config


def create_topics():
    admin = AdminClient(confluent_config())
    topics = [
        NewTopic(RAW_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(PREDICTIONS_TOPIC, num_partitions=1, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic: {topic}")
        except Exception as exc:
            if "already exists" in str(exc).lower():
                print(f"Topic already exists: {topic}")
            else:
                raise


if __name__ == "__main__":
    create_topics()
