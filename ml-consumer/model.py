"""
Layer 4: Machine Learning - Isolation Forest Anomaly Detection (ml-consumer service).
Loads pre-trained model artifact and performs inference without retraining per event.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import numpy as np
import joblib
from sklearn.ensemble import IsolationForest

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = ["cpu_usage", "memory_usage", "network_traffic", "response_time"]
DEFAULT_MODEL_PATH = "/app/models/isolation_forest.joblib"


class AnomalyDetector:
    """
    Isolation Forest Anomaly Detector for the ML Consumer container service.
    """

    def __init__(self, model_path: Optional[str] = None):
        self.model_path = Path(model_path or os.getenv("MODEL_PATH", DEFAULT_MODEL_PATH))
        self.feature_columns = FEATURE_COLUMNS
        self.model = None
        self.is_loaded = False
        self.load_or_train_model()

    def load_or_train_model(self):
        """Loads model if artifact exists, otherwise trains a baseline model."""
        if self.model_path.exists():
            try:
                self.model = joblib.load(self.model_path)
                self.is_loaded = True
                logger.info(f"Loaded trained Isolation Forest from {self.model_path}")
                return
            except Exception as e:
                logger.warning(f"Could not load existing model: {e}")

        logger.info("Training initial Isolation Forest model artifact...")
        self.model_path.parent.mkdir(parents=True, exist_ok=True)

        # Baseline synthetic training data
        np.random.seed(42)
        n_samples = 3000
        cpu = np.clip(np.random.normal(35, 10, n_samples), 5, 80)
        mem = np.clip(np.random.normal(55, 8, n_samples), 20, 80)
        net = np.clip(np.random.normal(25, 10, n_samples), 1, 95)
        resp = np.clip(np.random.normal(120, 35, n_samples), 15, 350)
        X = np.column_stack([cpu, mem, net, resp])

        model = IsolationForest(
            n_estimators=100,
            contamination=0.05,
            max_samples="auto",
            random_state=42,
            n_jobs=-1
        )
        model.fit(X)
        self.model = model
        self.is_loaded = True

        try:
            joblib.dump(model, self.model_path)
            logger.info(f"Saved initial model artifact to {self.model_path}")
        except Exception as e:
            logger.warning(f"Could not persist model artifact: {e}")

    def predict(self, event: Dict[str, Any]) -> Tuple[bool, float]:
        """Score event using Isolation Forest. Returns (is_anomaly, anomaly_score)."""
        if not self.is_loaded:
            self.load_or_train_model()

        try:
            features = [float(event[col]) for col in self.feature_columns]
        except KeyError as e:
            logger.error(f"Missing metric {e} in event")
            return False, 0.0

        X = np.array([features], dtype=np.float64)
        pred = self.model.predict(X)[0]
        raw_score = float(self.model.decision_function(X)[0])

        is_anomaly = bool(pred == -1)
        norm_score = float(1.0 / (1.0 + np.exp(raw_score * 8.0)))
        return is_anomaly, round(norm_score, 4)
