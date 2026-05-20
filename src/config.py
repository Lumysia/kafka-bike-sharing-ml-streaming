import os


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-data")
PREDICTIONS_TOPIC = os.getenv("PREDICTIONS_TOPIC", "predictions")
MODEL_PATH = os.getenv("MODEL_PATH", "models/bike_sharing_model.joblib")
DATA_PATH = os.getenv("DATA_PATH", "data/hour.csv")


def confluent_config():
    api_key = os.getenv("KAFKA_API_KEY")
    api_secret = os.getenv("KAFKA_API_SECRET")
    if not api_key or not api_secret:
        return {"bootstrap.servers": BOOTSTRAP_SERVERS}

    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    }
