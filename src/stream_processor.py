import os
from datetime import datetime, timezone

import faust
import joblib
import pandas as pd

from src.config import MODEL_PATH, PREDICTIONS_TOPIC, RAW_TOPIC

BROKER_URL = os.getenv("FAUST_BROKER_URL", "kafka://localhost:9092")

app = faust.App(
    "bike-sharing-stream-processor",
    broker=BROKER_URL,
    store="memory://",
    topic_partitions=1,
)

raw_topic = app.topic(RAW_TOPIC, value_serializer="json")
predictions_topic = app.topic(PREDICTIONS_TOPIC, value_serializer="json")

model_package = joblib.load(MODEL_PATH)
model = model_package["model"]
features = model_package["features"]


@app.agent(raw_topic)
async def predict_rentals(events):
    async for event in events:
        feature_row = pd.DataFrame([{feature: event[feature] for feature in features}])
        predicted_count = max(0, int(round(model.predict(feature_row)[0])))
        actual_count = event.get("actual_count")

        output = {
            "instant": event.get("instant"),
            "datetime": event.get("datetime"),
            "hour": event.get("hr"),
            "predicted_count": predicted_count,
            "actual_count": actual_count,
            "absolute_error": (
                abs(predicted_count - actual_count) if actual_count is not None else None
            ),
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "model": "RandomForestRegressor",
        }
        await predictions_topic.send(key=str(output["instant"]), value=output)
        print(
            f"processed row={output['instant']} predicted={predicted_count} "
            f"actual={actual_count} error={output['absolute_error']}"
        )
