"""
Layer 2: Streaming Tool - Apache Kafka Producer.
Publishes continuous synthetic server telemetry to the Kafka 'server-metrics' topic.
Supports automatic topic creation, connection retry, and manual anomaly injection for demos.
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from pathlib import Path
from typing import Optional, List

# Ensure root directory is on python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PRODUCER_INTERVAL_SECONDS,
    ANOMALY_PROBABILITY, SERVER_IDS
)
from producer.generator import ServerMetricGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [kafka_producer] %(message)s"
)
logger = logging.getLogger("kafka_producer")


class KafkaMetricProducer:
    """
    Kafka Producer for high-throughput streaming of server metrics.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        server_ids: Optional[List[str]] = None,
        anomaly_probability: float = ANOMALY_PROBABILITY,
        interval: float = PRODUCER_INTERVAL_SECONDS
    ):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or KAFKA_TOPIC
        self.server_ids = server_ids or SERVER_IDS
        self.anomaly_probability = anomaly_probability
        self.interval = interval

        self.generator = ServerMetricGenerator(
            server_ids=self.server_ids,
            anomaly_probability=self.anomaly_probability
        )

        self.producer = None
        self._running = False
        self._delivered_count = 0
        self._failed_count = 0

    def create_topic_if_not_exists(self, num_partitions: int = 1, replication_factor: int = 1) -> bool:
        """Create Kafka topic using AdminClient if it doesn't already exist."""
        try:
            admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            metadata = admin_client.list_topics(timeout=8)
            if self.topic in metadata.topics:
                logger.info(f"Kafka topic '{self.topic}' already exists.")
                return True

            logger.info(f"Topic '{self.topic}' not found. Creating topic...")
            new_topic = NewTopic(self.topic, num_partitions=num_partitions, replication_factor=replication_factor)
            fs = admin_client.create_topics([new_topic])
            for topic_name, future in fs.items():
                future.result()
                logger.info(f"Successfully created topic '{topic_name}'.")
            return True
        except Exception as e:
            logger.warning(f"Topic creation check/attempt encountered: {e}")
            return False

    def connect(self, max_retries: int = 10, retry_delay: float = 2.0) -> bool:
        """Initialize Producer instance and verify Kafka broker readiness."""
        producer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "server-metrics-producer",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 500,
            "request.timeout.ms": 15000,
            "message.timeout.ms": 15000,
            "compression.type": "snappy"
        }

        for attempt in range(1, max_retries + 1):
            try:
                self.producer = Producer(producer_conf)
                # Test connectivity
                self.producer.list_topics(timeout=4)
                logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
                self.create_topic_if_not_exists()
                return True
            except Exception as e:
                logger.warning(f"Kafka broker connection attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
        return False

    def _delivery_callback(self, err, msg):
        """Callback executed upon message delivery or failure."""
        if err:
            self._failed_count += 1
            logger.error(f"Message delivery failed to topic {msg.topic()}: {err}")
        else:
            self._delivered_count += 1

    def send_metric(self, force_anomaly: bool = False, force_anomaly_type: Optional[str] = None) -> dict:
        """
        Generate and publish a single metric message to Kafka.
        """
        event = self.generator.generate_metric(
            force_anomaly=force_anomaly,
            force_anomaly_type=force_anomaly_type
        )
        payload = json.dumps(event).encode("utf-8")
        partition_key = event["server_id"].encode("utf-8")

        if self.producer:
            self.producer.produce(
                topic=self.topic,
                key=partition_key,
                value=payload,
                callback=self._delivery_callback
            )
            self.producer.poll(0)

        if event.get("_injected_anomaly"):
            logger.warning(
                f"🚨 Produced ANOMALOUS metric [{event['_anomaly_type']}] -> Server: {event['server_id']} | "
                f"CPU: {event['cpu_usage']}% | MEM: {event['memory_usage']}% | "
                f"NET: {event['network_traffic']}MB/s | RESP: {event['response_time']}ms"
            )
        else:
            logger.info(
                f"Produced normal metric -> Server: {event['server_id']} | "
                f"CPU: {event['cpu_usage']}% | MEM: {event['memory_usage']}% | "
                f"NET: {event['network_traffic']}MB/s | RESP: {event['response_time']}ms"
            )
        return event

    def run(self, count: Optional[int] = None):
        """
        Main production loop.
        """
        if not self.connect():
            logger.error(f"Cannot connect to Kafka at {self.bootstrap_servers}. Producer exiting.")
            return

        self._running = True
        logger.info(f"Starting continuous telemetry stream to '{self.topic}' (interval: {self.interval}s)...")

        def handle_sigint(sig, frame):
            logger.info("Received termination signal. Stopping producer...")
            self._running = False

        signal.signal(signal.SIGINT, handle_sigint)

        sent = 0
        try:
            while self._running:
                self.send_metric()
                sent += 1
                if count and sent >= count:
                    logger.info(f"Reached requested count of {count} events.")
                    break
                time.sleep(self.interval)
        finally:
            self.stop()

    def stop(self):
        """Flushes buffered messages and shuts down cleanly."""
        self._running = False
        if self.producer:
            logger.info("Flushing pending messages...")
            self.producer.flush(timeout=5)
            logger.info(f"Producer shut down. Total delivered: {self._delivered_count}, Failed: {self._failed_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apache Kafka Server Metrics Producer")
    parser.add_argument("--interval", type=float, default=PRODUCER_INTERVAL_SECONDS, help="Interval in seconds between metrics")
    parser.add_argument("--anomaly-prob", type=float, default=ANOMALY_PROBABILITY, help="Probability of injecting an anomaly")
    parser.add_argument("--bootstrap", type=str, default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default=KAFKA_TOPIC, help="Kafka topic name")
    parser.add_argument("--count", type=int, default=None, help="Number of messages to produce (default: infinite)")
    parser.add_argument("--inject-anomaly", action="store_true", help="Immediately produce a forced anomaly")
    parser.add_argument("--anomaly-type", type=str, default=None, help="Specific anomaly type to force")
    args = parser.parse_args()

    producer = KafkaMetricProducer(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        anomaly_probability=args.anomaly_prob,
        interval=args.interval
    )

    if args.inject_anomaly:
        if producer.connect():
            logger.info("Injecting immediate manual anomaly...")
            producer.send_metric(force_anomaly=True, force_anomaly_type=args.anomaly_type)
            producer.stop()
    else:
        producer.run(count=args.count)

