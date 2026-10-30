"""
Layer 3: Stream Processing - Kafka Consumer and Anomaly Processor (ml-consumer service).
Maintains 30-event sliding window, calculates rolling averages, invokes Isolation Forest model,
and updates PostgreSQL server_metrics and server_summary tables.
"""

import os
import sys
import json
import time
import logging
from collections import deque
from datetime import datetime, timezone
import psycopg2
from confluent_kafka import Consumer, KafkaError
from model import AnomalyDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ml_consumer] %(message)s"
)
logger = logging.getLogger("ml_consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "server-metrics")
KAFKA_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "ml-consumer-group")

POSTGRES_HOST = os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "db"))
POSTGRES_PORT = int(os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")))
POSTGRES_DB = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "server_health_db"))
POSTGRES_USER = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "postgres"))
POSTGRES_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres"))

SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "30"))


class SlidingTracker:
    def __init__(self, maxlen: int = 30):
        self.maxlen = maxlen
        self.window = deque(maxlen=maxlen)
        self.peak_cpu = 0.0
        self.peak_memory = 0.0
        self.total_events = 0
        self.total_anomalies = 0

    def add(self, event, is_anomaly):
        self.window.append(event)
        self.total_events += 1
        if is_anomaly:
            self.total_anomalies += 1
        cpu = float(event["cpu_usage"])
        mem = float(event["memory_usage"])
        if cpu > self.peak_cpu:
            self.peak_cpu = cpu
        if mem > self.peak_memory:
            self.peak_memory = mem

    def rolling_averages(self):
        if not self.window:
            return {"cpu": 0.0, "mem": 0.0, "net": 0.0, "resp": 0.0}
        n = len(self.window)
        return {
            "cpu": round(sum(e["cpu_usage"] for e in self.window) / n, 2),
            "mem": round(sum(e["memory_usage"] for e in self.window) / n, 2),
            "net": round(sum(e["network_traffic"] for e in self.window) / n, 2),
            "resp": round(sum(e["response_time"] for e in self.window) / n, 2)
        }

    def health_status(self, event, is_anomaly):
        cpu = event["cpu_usage"]
        mem = event["memory_usage"]
        resp = event["response_time"]
        if is_anomaly or cpu >= 90.0 or mem >= 90.0 or resp >= 1000.0:
            return "CRITICAL"
        elif cpu >= 75.0 or mem >= 75.0 or resp >= 400.0:
            return "WARNING"
        return "HEALTHY"


class ServerStreamConsumer:
    def __init__(self):
        self.detector = AnomalyDetector()
        self.trackers = {}
        self.conn = None
        self.consumer = None

    def connect_db(self, max_retries: int = 30):
        """Connect to PostgreSQL with retry logic."""
        for attempt in range(1, max_retries + 1):
            try:
                self.conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    connect_timeout=5
                )
                self.conn.autocommit = True
                self.create_schema()
                logger.info("Connected to PostgreSQL successfully.")
                return True
            except Exception as e:
                logger.warning(f"Database connection attempt {attempt}/{max_retries} failed: {e}")
                time.sleep(2)
        return False

    def create_schema(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS server_metrics (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    server_id VARCHAR(50) NOT NULL,
                    cpu_usage FLOAT NOT NULL,
                    memory_usage FLOAT NOT NULL,
                    network_traffic FLOAT NOT NULL,
                    response_time FLOAT NOT NULL,
                    cpu_rolling_avg FLOAT,
                    memory_rolling_avg FLOAT,
                    network_rolling_avg FLOAT,
                    response_time_rolling_avg FLOAT,
                    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
                    anomaly_score FLOAT NOT NULL DEFAULT 0.0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_server_metrics_timestamp ON server_metrics(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_server_metrics_server_id ON server_metrics(server_id);
                CREATE INDEX IF NOT EXISTS idx_server_metrics_is_anomaly ON server_metrics(is_anomaly);

                CREATE TABLE IF NOT EXISTS server_summary (
                    id SERIAL PRIMARY KEY,
                    server_id VARCHAR(50) UNIQUE NOT NULL,
                    current_cpu FLOAT NOT NULL DEFAULT 0.0,
                    current_memory FLOAT NOT NULL DEFAULT 0.0,
                    current_network FLOAT NOT NULL DEFAULT 0.0,
                    current_response_time FLOAT NOT NULL DEFAULT 0.0,
                    peak_cpu FLOAT NOT NULL DEFAULT 0.0,
                    peak_memory FLOAT NOT NULL DEFAULT 0.0,
                    total_events INTEGER NOT NULL DEFAULT 0,
                    total_anomalies INTEGER NOT NULL DEFAULT 0,
                    health_status VARCHAR(20) NOT NULL DEFAULT 'HEALTHY',
                    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)

    def _get_tracker(self, server_id: str) -> SlidingTracker:
        if server_id not in self.trackers:
            self.trackers[server_id] = SlidingTracker(maxlen=SLIDING_WINDOW_SIZE)
        return self.trackers[server_id]

    def setup_consumer(self, max_retries: int = 30):
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000
        }
        for attempt in range(1, max_retries + 1):
            try:
                self.consumer = Consumer(conf)
                self.consumer.subscribe([KAFKA_TOPIC])
                logger.info(f"Kafka consumer subscribed to '{KAFKA_TOPIC}'.")
                return True
            except Exception as e:
                logger.warning(f"Consumer connection attempt {attempt}/{max_retries} failed: {e}")
                time.sleep(2)
        return False

    def insert_metrics(self, event, roll, is_anomaly, score, health_status):
        try:
            with self.conn.cursor() as cur:
                # 1. Insert detailed metric
                cur.execute("""
                    INSERT INTO server_metrics (
                        timestamp, server_id, cpu_usage, memory_usage,
                        network_traffic, response_time, cpu_rolling_avg,
                        memory_rolling_avg, network_rolling_avg, response_time_rolling_avg,
                        is_anomaly, anomaly_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    event["timestamp"], event["server_id"],
                    event["cpu_usage"], event["memory_usage"],
                    event["network_traffic"], event["response_time"],
                    roll["cpu"], roll["mem"], roll["net"], roll["resp"],
                    is_anomaly, score
                ))

                # 2. Upsert summary
                tracker = self._get_tracker(event["server_id"])
                cur.execute("""
                    INSERT INTO server_summary (
                        server_id, current_cpu, current_memory, current_network,
                        current_response_time, peak_cpu, peak_memory, total_events,
                        total_anomalies, health_status, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT(server_id) DO UPDATE SET
                        current_cpu = EXCLUDED.current_cpu,
                        current_memory = EXCLUDED.current_memory,
                        current_network = EXCLUDED.current_network,
                        current_response_time = EXCLUDED.current_response_time,
                        peak_cpu = GREATEST(server_summary.peak_cpu, EXCLUDED.peak_cpu),
                        peak_memory = GREATEST(server_summary.peak_memory, EXCLUDED.peak_memory),
                        total_events = EXCLUDED.total_events,
                        total_anomalies = EXCLUDED.total_anomalies,
                        health_status = EXCLUDED.health_status,
                        last_updated = NOW()
                """, (
                    event["server_id"], event["cpu_usage"], event["memory_usage"],
                    event["network_traffic"], event["response_time"],
                    tracker.peak_cpu, tracker.peak_memory,
                    tracker.total_events, tracker.total_anomalies,
                    health_status
                ))
        except Exception as e:
            logger.error(f"Error persisting to DB: {e}")
            if self.conn and self.conn.closed:
                self.connect_db(max_retries=5)

    def process_message(self, msg):
        try:
            event = json.loads(msg.value().decode("utf-8"))
            server_id = event["server_id"]
            tracker = self._get_tracker(server_id)

            # ML Anomaly Scoring
            is_anomaly, score = self.detector.predict(event)

            # Sliding Window Update
            tracker.add(event, is_anomaly)
            roll = tracker.rolling_averages()
            health = tracker.health_status(event, is_anomaly)

            # Persist
            self.insert_metrics(event, roll, is_anomaly, score, health)

            if is_anomaly:
                logger.warning(
                    f"🚨 [ANOMALY DETECTED] Server: {server_id} (Score: {score:.3f}) | "
                    f"CPU: {event['cpu_usage']}% | MEM: {event['memory_usage']}% | "
                    f"NET: {event['network_traffic']}MB/s | RESP: {event['response_time']}ms"
                )
            elif tracker.total_events % 20 == 0:
                logger.info(f"Processed {tracker.total_events} events for {server_id}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        if not self.connect_db():
            logger.error("Database connection failed. Exiting.")
            return

        if not self.setup_consumer():
            logger.error("Kafka consumer setup failed. Exiting.")
            return

        logger.info("Stream Processor consumer running...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                self.process_message(msg)
                self.consumer.commit(msg)
        except KeyboardInterrupt:
            logger.info("Consumer stopping...")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.conn:
                self.conn.close()


if __name__ == "__main__":
    consumer = ServerStreamConsumer()
    consumer.run()
