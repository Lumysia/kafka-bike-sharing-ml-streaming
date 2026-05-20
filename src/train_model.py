from pathlib import Path
from urllib.request import urlretrieve
from zipfile import ZipFile

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

DATASET_URL = "https://archive.ics.uci.edu/static/public/275/bike+sharing+dataset.zip"
DATA_DIR = Path("data")
MODEL_DIR = Path("models")
ZIP_PATH = DATA_DIR / "Bike-Sharing-Dataset.zip"
CSV_PATH = DATA_DIR / "hour.csv"
MODEL_PATH = MODEL_DIR / "bike_sharing_model.joblib"
METRICS_PATH = MODEL_DIR / "metrics.txt"

FEATURES = [
    "season",
    "yr",
    "mnth",
    "hr",
    "holiday",
    "weekday",
    "workingday",
    "weathersit",
    "temp",
    "atemp",
    "hum",
    "windspeed",
]
TARGET = "cnt"


def ensure_dataset():
    DATA_DIR.mkdir(exist_ok=True)
    if CSV_PATH.exists():
        return

    print("Downloading UCI Bike Sharing dataset...")
    urlretrieve(DATASET_URL, ZIP_PATH)
    with ZipFile(ZIP_PATH) as archive:
        archive.extract("hour.csv", DATA_DIR)


def main():
    ensure_dataset()
    MODEL_DIR.mkdir(exist_ok=True)

    df = pd.read_csv(CSV_PATH)
    x = df[FEATURES]
    y = df[TARGET]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42
    )

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "regressor",
                RandomForestRegressor(
                    n_estimators=120,
                    max_depth=18,
                    min_samples_leaf=2,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    model.fit(x_train, y_train)
    predictions = model.predict(x_test)

    mae = mean_absolute_error(y_test, predictions)
    mse = mean_squared_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    package = {
        "model": model,
        "features": FEATURES,
        "target": TARGET,
        "dataset": "UCI Bike Sharing Dataset - hourly rental count",
        "metrics": {"mae": mae, "mse": mse, "r2": r2},
    }
    joblib.dump(package, MODEL_PATH)

    metrics_text = (
        "Bike Sharing rental count regression model\n"
        f"Dataset rows: {len(df)}\n"
        f"Train rows: {len(x_train)}\n"
        f"Test rows: {len(x_test)}\n"
        f"MAE: {mae:.3f}\n"
        f"MSE: {mse:.3f}\n"
        f"RMSE: {mse ** 0.5:.3f}\n"
        f"R2: {r2:.4f}\n"
    )
    METRICS_PATH.write_text(metrics_text)
    print(metrics_text)
    print(f"Saved model to {MODEL_PATH}")


if __name__ == "__main__":
    main()
