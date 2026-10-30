"""
Layer 4: Machine Learning - Isolation Forest Anomaly Detector.
Loads the trained Scikit-learn Isolation Forest model and performs live inference.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

# Ensure root directory is on python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import numpy as np
import joblib

logger = logging.getLogger(__name__)

# Default feature order
FEATURE_COLUMNS = ["cpu_usage", "memory_usage", "network_traffic", "response_time"]


class ServerAnomalyDetector:
    """
    Inference engine for server health anomaly detection using Isolation Forest.
    Loads a pre-trained joblib model artifact once at initialization.
    Does NOT retrain on live streaming events.
    """

    def __init__(
        self,
        model_path: Optional[str] = None,
        feature_columns: Optional[List[str]] = None,
        auto_train_if_missing: bool = True
    ):
        """
        Args:
            model_path: Path to serialized .joblib model artifact.
            feature_columns: List of feature names in strict order.
            auto_train_if_missing: If True and model does not exist, trains an initial model.
        """
        self.model_path = Path(model_path) if model_path else Path(__file__).resolve().parent.parent / "models" / "isolation_forest.joblib"
        self.feature_columns = feature_columns or FEATURE_COLUMNS
        self.model = None
        self.is_loaded = False
        self.metadata = {}

        self.load_model(auto_train_if_missing=auto_train_if_missing)

    def load_model(self, auto_train_if_missing: bool = True) -> bool:
        """
        Loads the trained Isolation Forest model from disk.
        """
        if self.model_path.exists():
            try:
                self.model = joblib.load(self.model_path)
                self.is_loaded = True
                logger.info(f"Loaded trained Isolation Forest model from: {self.model_path}")
                
                # Check for metadata
                meta_path = self.model_path.with_suffix(".json")
                if meta_path.exists():
                    try:
                        with open(meta_path, "r") as f:
                            self.metadata = json.load(f)
                    except Exception:
                        pass
                return True
            except Exception as e:
                logger.error(f"Failed to load model from {self.model_path}: {e}")

        if auto_train_if_missing:
            logger.warning(f"Model file {self.model_path} not found. Automatically training initial model...")
            from ml.train import train_and_save_model
            self.model, self.metadata = train_and_save_model(output_path=str(self.model_path))
            self.is_loaded = True
            return True

        return False

    def predict(self, event: Dict[str, Any]) -> Tuple[bool, float]:
        """
        Score a single server telemetry event.

        Args:
            event: Dict containing metric keys ('cpu_usage', 'memory_usage', 'network_traffic', 'response_time').

        Returns:
            Tuple of (is_anomaly: bool, anomaly_score: float)
            - is_anomaly: True if classified as anomaly (-1 in Isolation Forest), False otherwise.
            - anomaly_score: Continuous score in [0.0, 1.0], where higher indicates greater abnormality.
        """
        if not self.is_loaded or self.model is None:
            raise RuntimeError("AnomalyDetector model is not loaded. Cannot perform inference.")

        # Extract features in correct order
        try:
            features = [float(event[col]) for col in self.feature_columns]
        except KeyError as e:
            raise ValueError(f"Missing required ML feature '{e}' in event: {event}")

        X = np.array([features], dtype=np.float64)

        # Scikit-learn IsolationForest:
        # predict() -> 1 for normal (inlier), -1 for anomaly (outlier)
        # decision_function() -> negative for anomalies, positive for normal
        raw_pred = self.model.predict(X)[0]
        raw_score = float(self.model.decision_function(X)[0])

        is_anomaly = bool(raw_pred == -1)

        # Normalize decision function score to a readable [0.0, 1.0] anomaly confidence scale:
        # Logistic sigmoid transformation centered at 0: 1 / (1 + exp(raw_score * 8.0))
        # When raw_score is strongly negative (< 0), norm_score approaches 1.0 (Definite Anomaly)
        # When raw_score is positive (> 0), norm_score approaches 0.0 (Normal)
        try:
            normalized_anomaly_score = 1.0 / (1.0 + np.exp(raw_score * 8.0))
        except OverflowError:
            normalized_anomaly_score = 1.0 if raw_score < 0 else 0.0

        return is_anomaly, round(float(normalized_anomaly_score), 4)

    def batch_predict(self, events: List[Dict[str, Any]]) -> List[Tuple[bool, float]]:
        """
        Score a batch of events efficiently.
        """
        if not self.is_loaded or self.model is None:
            raise RuntimeError("AnomalyDetector model is not loaded.")

        X = np.array([[float(e[col]) for col in self.feature_columns] for e in events], dtype=np.float64)
        raw_preds = self.model.predict(X)
        raw_scores = self.model.decision_function(X)

        results = []
        for pred, score in zip(raw_preds, raw_scores):
            is_anomaly = bool(pred == -1)
            norm_score = float(1.0 / (1.0 + np.exp(score * 8.0)))
            results.append((is_anomaly, round(norm_score, 4)))
        return results
