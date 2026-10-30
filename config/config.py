"""
Configuration module for Real-Time Server Health Monitoring and Anomaly Detection.
Centralizes all settings, environment variables, and default parameters.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Base directory of the repository
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file if it exists
load_dotenv(dotenv_path=BASE_DIR / ".env")

# ------------------------------------------------------------------------------
# Apache Kafka Settings
# ------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "server-metrics")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "server-health-processor-group")

# ------------------------------------------------------------------------------
# Database Settings (PostgreSQL with SQLite fallback support)
# ------------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "localhost"))
DB_PORT = int(os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")))
DB_NAME = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "server_health_db"))
DB_USER = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "postgres"))
DB_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres"))
DB_TYPE = os.getenv("DB_TYPE", "postgresql").lower()
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", str(BASE_DIR / "server_health.db"))

# ------------------------------------------------------------------------------
# Producer & Generator Settings
# ------------------------------------------------------------------------------
PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1.0"))
ANOMALY_PROBABILITY = float(os.getenv("ANOMALY_PROBABILITY", "0.08"))
SERVER_IDS = [s.strip() for s in os.getenv("SERVER_IDS", "server-01,server-02,server-03").split(",") if s.strip()]

# ------------------------------------------------------------------------------
# Stream Processor Settings
# ------------------------------------------------------------------------------
SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "30"))

# ------------------------------------------------------------------------------
# Machine Learning (Isolation Forest) Settings
# ------------------------------------------------------------------------------
MODEL_PATH = os.getenv("MODEL_PATH", str(BASE_DIR / "models" / "isolation_forest.joblib"))
MODEL_METADATA_PATH = os.getenv("MODEL_METADATA_PATH", str(BASE_DIR / "models" / "model_metadata.json"))
CONTAMINATION = float(os.getenv("CONTAMINATION", "0.05"))
N_ESTIMATORS = int(os.getenv("N_ESTIMATORS", "100"))
RANDOM_STATE = int(os.getenv("RANDOM_STATE", "42"))

# Feature column names required for ML model training and inference
ML_FEATURE_COLUMNS = ["cpu_usage", "memory_usage", "network_traffic", "response_time"]

# ------------------------------------------------------------------------------
# Dashboard Settings
# ------------------------------------------------------------------------------
DASHBOARD_REFRESH_INTERVAL = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "2"))
DASHBOARD_HISTORY_LIMIT = int(os.getenv("DASHBOARD_HISTORY_LIMIT", "100"))

