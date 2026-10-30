"""
Layer 4: Machine Learning - Model Training Workflow.
Generates historical synthetic server telemetry, trains the Isolation Forest model,
evaluates detection metrics, and serializes the model artifact via Joblib.
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple, Dict, Any

# Ensure root directory is on python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import classification_report, f1_score, precision_score, recall_score, accuracy_score

from producer.generator import ServerMetricGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ml_train")

FEATURE_COLUMNS = ["cpu_usage", "memory_usage", "network_traffic", "response_time"]


def generate_historical_dataset(
    num_samples: int = 4000,
    contamination: float = 0.05,
    seed: int = 42
) -> pd.DataFrame:
    """
    Generate synthetic historical server telemetry dataset with labeled anomalies.
    """
    logger.info(f"Generating {num_samples} historical metrics samples (contamination={contamination})...")
    generator = ServerMetricGenerator(
        server_ids=["server-01", "server-02", "server-03", "server-04"],
        anomaly_probability=contamination,
        seed=seed
    )

    records = []
    for _ in range(num_samples):
        event = generator.generate_metric()
        records.append({
            "timestamp": event["timestamp"],
            "server_id": event["server_id"],
            "cpu_usage": event["cpu_usage"],
            "memory_usage": event["memory_usage"],
            "network_traffic": event["network_traffic"],
            "response_time": event["response_time"],
            "is_anomaly_ground_truth": event["_injected_anomaly"],
            "anomaly_type": event["_anomaly_type"]
        })

    df = pd.DataFrame(records)
    logger.info(
        f"Dataset generated: {len(df)} rows. "
        f"Anomalies: {df['is_anomaly_ground_truth'].sum()} ({df['is_anomaly_ground_truth'].mean():.2%})"
    )
    return df


def train_and_save_model(
    num_samples: int = 4000,
    contamination: float = 0.05,
    n_estimators: int = 100,
    random_state: int = 42,
    output_path: str = "models/isolation_forest.joblib"
) -> Tuple[IsolationForest, Dict[str, Any]]:
    """
    Trains an Isolation Forest model and saves it as a joblib artifact.
    """
    out_file = Path(output_path).resolve()
    out_file.parent.mkdir(parents=True, exist_ok=True)

    # 1. Generate synthetic training dataset
    df = generate_historical_dataset(num_samples=num_samples, contamination=contamination, seed=random_state)
    X = df[FEATURE_COLUMNS].values
    y_true = df["is_anomaly_ground_truth"].astype(bool).values

    # 2. Configure and train Isolation Forest
    logger.info(
        f"Fitting IsolationForest(n_estimators={n_estimators}, contamination={contamination}, "
        f"random_state={random_state})..."
    )
    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        max_samples="auto",
        random_state=random_state,
        n_jobs=-1
    )
    model.fit(X)

    # 3. Model evaluation against ground truth
    raw_preds = model.predict(X)
    y_pred = (raw_preds == -1)  # -1 is outlier in Isolation Forest

    acc = float(accuracy_score(y_true, y_pred))
    prec = float(precision_score(y_true, y_pred, zero_division=0))
    rec = float(recall_score(y_true, y_pred, zero_division=0))
    f1 = float(f1_score(y_true, y_pred, zero_division=0))

    logger.info("=" * 60)
    logger.info("MODEL TRAINING EVALUATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Accuracy:  {acc:.4f}")
    logger.info(f"Precision: {prec:.4f}")
    logger.info(f"Recall:    {rec:.4f}")
    logger.info(f"F1-Score:  {f1:.4f}")
    logger.info("=" * 60)
    logger.info("\nDetailed Classification Report:\n" + classification_report(y_true, y_pred, target_names=["Normal", "Anomaly"]))

    # 4. Serialize model using Joblib
    joblib.dump(model, out_file)
    logger.info(f"Model successfully saved to: {out_file}")

    # 5. Save detailed metadata
    metadata = {
        "algorithm": "IsolationForest",
        "library": "scikit-learn",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "num_training_samples": num_samples,
        "features": FEATURE_COLUMNS,
        "hyperparameters": {
            "contamination": contamination,
            "n_estimators": n_estimators,
            "random_state": random_state,
            "max_samples": "auto"
        },
        "evaluation_metrics": {
            "accuracy": round(acc, 4),
            "precision": round(prec, 4),
            "recall": round(rec, 4),
            "f1_score": round(f1, 4)
        },
        "feature_summary": {
            col: {
                "mean": round(float(df[col].mean()), 2),
                "std": round(float(df[col].std()), 2),
                "min": round(float(df[col].min()), 2),
                "max": round(float(df[col].max()), 2)
            }
            for col in FEATURE_COLUMNS
        }
    }

    meta_file = out_file.with_suffix(".json")
    with open(meta_file, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Model metadata saved to: {meta_file}")

    return model, metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Isolation Forest model for server anomaly detection")
    parser.add_argument("--samples", type=int, default=4000, help="Number of historical metric samples to generate")
    parser.add_argument("--contamination", type=float, default=0.05, help="Contamination rate (expected anomaly fraction)")
    parser.add_argument("--estimators", type=int, default=100, help="Number of isolation trees")
    parser.add_argument("--output", type=str, default="models/isolation_forest.joblib", help="Output path for joblib artifact")
    args = parser.parse_args()

    train_and_save_model(
        num_samples=args.samples,
        contamination=args.contamination,
        n_estimators=args.estimators,
        output_path=args.output
    )
