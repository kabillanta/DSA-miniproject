"""
Layer 3: Stream Processing - Python Stream Processor.
Consumes real-time Kafka server telemetry, validates incoming events,
maintains a sliding window of the last 30 events, computes rolling averages,
tracks peak metrics, scores anomalies via Isolation Forest, and persists results to PostgreSQL.
"""

import os
import sys
import json
import time
import logging
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# Ensure root directory is on python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP,
    SLIDING_WINDOW_SIZE, MODEL_PATH
)
from producer.generator import ServerMetricGenerator, MetricValidationError
from ml.model import ServerAnomalyDetector
from database.db import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("stream_processor")


class SlidingWindowTracker:
    """
    Maintains a FIFO sliding window of the most recent N events (default 30).
    Computes rolling averages and tracks peak historical utilization.
    """

    def __init__(self, maxlen: int = 30):
        self.maxlen = maxlen
        self.window = deque(maxlen=maxlen)
        self.peak_cpu = 0.0
        self.peak_memory = 0.0
        self.total_events = 0
        self.total_anomalies = 0

    def add_event(self, event: Dict[str, Any], is_anomaly: bool):
        """Add event to window and update tracked counters."""
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

    def compute_rolling_averages(self) -> Dict[str, float]:
        """
        Calculates moving averages over the sliding window for all 4 key metrics.
        """
        if not self.window:
            return {
                "cpu_rolling_avg": 0.0,
                "memory_rolling_avg": 0.0,
                "network_rolling_avg": 0.0,
                "response_time_rolling_avg": 0.0
            }

        n = len(self.window)
        return {
            "cpu_rolling_avg": round(sum(e["cpu_usage"] for e in self.window) / n, 2),
            "memory_rolling_avg": round(sum(e["memory_usage"] for e in self.window) / n, 2),
            "network_rolling_avg": round(sum(e["network_traffic"] for e in self.window) / n, 2),
            "response_time_rolling_avg": round(sum(e["response_time"] for e in self.window) / n, 2)
        }

    def determine_health_status(self, current_event: Dict[str, Any], is_anomaly: bool) -> str:
        """
        Evaluates health status based on current anomaly detection and rolling load.
        Status: 'CRITICAL', 'WARNING', or 'HEALTHY'.
        """
        cpu = current_event["cpu_usage"]
        mem = current_event["memory_usage"]
        resp = current_event["response_time"]

        if is_anomaly or cpu >= 90.0 or mem >= 90.0 or resp >= 1000.0:
            return "CRITICAL"
        elif cpu >= 75.0 or mem >= 75.0 or resp >= 400.0:
            return "WARNING"
        return "HEALTHY"


class MetricStreamProcessor:
    """
    Continuous stream processor connecting Kafka, ML inference, and PostgreSQL.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
        window_size: int = SLIDING_WINDOW_SIZE,
        model_path: Optional[str] = None,
        db_manager: Optional[DatabaseManager] = None
    ):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or KAFKA_TOPIC
        self.consumer_group = consumer_group or KAFKA_CONSUMER_GROUP
        self.window_size = window_size

        # Layer 4: Load ML model ONCE at startup (no retraining during live inference)
        logger.info("Initializing ML Anomaly Detector...")
        self.detector = ServerAnomalyDetector(model_path=model_path)

        # Layer 5: Initialize Database Manager
        logger.info("Initializing Database connection...")
        self.db = db_manager or DatabaseManager()

        # Sliding window state trackers per server and globally
        self.server_trackers: Dict[str, SlidingWindowTracker] = {}
        self.global_tracker = SlidingWindowTracker(maxlen=self.window_size)

        self.consumer = None
        self._running = False

    def _get_tracker(self, server_id: str) -> SlidingWindowTracker:
        """Retrieve or create a sliding window tracker for a specific server."""
        if server_id not in self.server_trackers:
            self.server_trackers[server_id] = SlidingWindowTracker(maxlen=self.window_size)
        return self.server_trackers[server_id]

    def process_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single incoming telemetry event:
        1. Validates and preprocesses incoming metric.
        2. Scores anomaly via trained Isolation Forest model.
        3. Updates sliding window (30 events) and calculates rolling averages.
        4. Tracks peak CPU and memory metrics.
        5. Persists processed record to database.
        6. Updates server_summary table.
        """
        try:
            # Step 1: Validate and sanitize
            event = ServerMetricGenerator.validate_metric(raw_event)
        except MetricValidationError as e:
            logger.warning(f"Discarding invalid metric event: {e}")
            return None

        server_id = event["server_id"]
        tracker = self._get_tracker(server_id)

        # Step 2: ML Inference (Isolation Forest)
        is_anomaly, anomaly_score = self.detector.predict(event)

        # Step 3: Maintain sliding window & compute rolling averages
        tracker.add_event(event, is_anomaly)
        self.global_tracker.add_event(event, is_anomaly)
        rolling_avgs = tracker.compute_rolling_averages()

        # Step 4: Health status determination
        health_status = tracker.determine_health_status(event, is_anomaly)

        # Construct enriched processed record
        processed_event = {
            "timestamp": event["timestamp"],
            "server_id": server_id,
            "cpu_usage": event["cpu_usage"],
            "memory_usage": event["memory_usage"],
            "network_traffic": event["network_traffic"],
            "response_time": event["response_time"],
            "cpu_rolling_avg": rolling_avgs["cpu_rolling_avg"],
            "memory_rolling_avg": rolling_avgs["memory_rolling_avg"],
            "network_rolling_avg": rolling_avgs["network_rolling_avg"],
            "response_time_rolling_avg": rolling_avgs["response_time_rolling_avg"],
            "is_anomaly": is_anomaly,
            "anomaly_score": anomaly_score,
            "health_status": health_status
        }

        # Step 5 & 6: Persist to database (server_metrics & server_summary)
        self.db.insert_metric(processed_event)
        self.db.update_summary(
            server_id=server_id,
            current_cpu=event["cpu_usage"],
            current_memory=event["memory_usage"],
            current_network=event["network_traffic"],
            current_response_time=event["response_time"],
            peak_cpu=tracker.peak_cpu,
            peak_memory=tracker.peak_memory,
            total_events=tracker.total_events,
            total_anomalies=tracker.total_anomalies,
            health_status=health_status
        )

        # Log alerts loudly when anomalies are detected
        if is_anomaly:
            logger.warning(
                f"🚨 [ANOMALY DETECTED] Server: {server_id} | Score: {anomaly_score:.3f} | "
                f"CPU: {event['cpu_usage']}% (avg {rolling_avgs['cpu_rolling_avg']}%) | "
                f"MEM: {event['memory_usage']}% (avg {rolling_avgs['memory_rolling_avg']}%) | "
                f"NET: {event['network_traffic']}MB/s | RESP: {event['response_time']}ms"
            )
        else:
            if tracker.total_events % 20 == 0:
                logger.info(
                    f"Processed {tracker.total_events} events for {server_id} | "
                    f"CPU: {event['cpu_usage']}% (roll {rolling_avgs['cpu_rolling_avg']}%) | "
                    f"Status: {health_status}"
                )

        return processed_event

    def setup_consumer(self, max_retries: int = 10, retry_delay: float = 3.0) -> bool:
        """
        Initializes Kafka Consumer with connection retry logic.
        """
        from confluent_kafka import Consumer
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000
        }

        for attempt in range(1, max_retries + 1):
            try:
                self.consumer = Consumer(conf)
                self.consumer.subscribe([self.topic])
                logger.info(f"Subscribed Kafka consumer to topic '{self.topic}' (bootstrap: {self.bootstrap_servers})")
                return True
            except Exception as e:
                logger.warning(f"Kafka consumer connection attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
        return False

    def run(self):
        """
        Continuous stream processing loop.
        """
        from confluent_kafka import KafkaError

        if not self.setup_consumer():
            logger.error("Failed to connect to Kafka. Processor exiting.")
            return

        self._running = True
        logger.info("Stream Processor started and continuously processing events...")

        try:
            while self._running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    continue

                try:
                    raw_data = json.loads(msg.value().decode("utf-8"))
                    self.process_event(raw_data)
                    self.consumer.commit(msg)
                except json.JSONDecodeError as e:
                    logger.error(f"Malformed JSON in Kafka message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Processor stopping due to interrupt signal...")
        finally:
            self.stop()

    def stop(self):
        """Clean shutdown."""
        self._running = False
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed.")
            except Exception:
                pass
        self.db.close()


if __name__ == "__main__":
    processor = MetricStreamProcessor()
    processor.run()

