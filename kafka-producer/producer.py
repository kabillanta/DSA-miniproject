"""
Layer 2: Streaming Tool - Apache Kafka Producer (Docker Container Service).
Publishes simulated server telemetry to the Kafka 'server-metrics' topic.
"""

import os
import sys
import json
import time
import logging
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from generator import ServerMetricGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [producer] %(message)s"
)
logger = logging.getLogger("producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "server-metrics")
PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1.0"))
ANOMALY_PROBABILITY = float(os.getenv("ANOMALY_PROBABILITY", "0.08"))
SERVER_IDS = [s.strip() for s in os.getenv("SERVER_IDS", "server-01,server-02,server-03").split(",") if s.strip()]


def create_topic_if_not_exists(bootstrap_servers, topic_name):
    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin_client.list_topics(timeout=10)
        if topic_name in metadata.topics:
            logger.info(f"Topic '{topic_name}' already exists.")
            return True
        topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([topic])
        for t, f in fs.items():
            f.result()
            logger.info(f"Topic '{t}' created successfully.")
        return True
    except Exception as e:
        logger.warning(f"Topic verification: {e}")
        return False


def delivery_report(err, msg):
    if err:
        logger.error(f"Message delivery failed: {err}")


def main():
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "kafka-metrics-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "request.timeout.ms": 30000,
        "message.timeout.ms": 30000
    }

    # Retry loop
    connected = False
    for attempt in range(1, 30):
        try:
            producer = Producer(producer_conf)
            producer.list_topics(timeout=5)
            connected = True
            logger.info("Kafka is ready.")
            break
        except Exception as e:
            logger.warning(f"Waiting for Kafka... attempt {attempt}/30: {e}")
            time.sleep(2)

    if not connected:
        logger.error("Failed to connect to Kafka. Exiting.")
        sys.exit(1)

    create_topic_if_not_exists(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    generator = ServerMetricGenerator(server_ids=SERVER_IDS, anomaly_probability=ANOMALY_PROBABILITY)

    logger.info(f"Starting server metrics producer on topic '{KAFKA_TOPIC}'...")
    msg_count = 0
    try:
        while True:
            event = generator.generate_metric()
            payload = json.dumps(event)
            producer.produce(
                KAFKA_TOPIC,
                key=event["server_id"],
                value=payload,
                callback=delivery_report
            )
            producer.poll(0.05)

            msg_count += 1
            if event.get("_injected_anomaly"):
                logger.warning(
                    f"🚨 Produced ANOMALY: {event['server_id']} | "
                    f"CPU: {event['cpu_usage']}% | MEM: {event['memory_usage']}% | "
                    f"NET: {event['network_traffic']}MB/s | RESP: {event['response_time']}ms"
                )
            elif msg_count % 15 == 0:
                logger.info(f"Produced {msg_count} metrics. Latest: {event['server_id']} CPU: {event['cpu_usage']}%")

            time.sleep(PRODUCER_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.flush(timeout=5)


if __name__ == "__main__":
    main()
