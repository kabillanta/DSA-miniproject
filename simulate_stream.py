"""
Real-Time Telemetry Simulation & End-to-End Pipeline Runner.
Runs the complete ingestion and stream processing loop locally,
feeding live events directly into the serving database for the Streamlit dashboard.
"""

import time
import argparse
import logging
from producer.generator import ServerMetricGenerator
from processor.stream_processor import MetricStreamProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("simulator")


def run_simulation(interval: float = 1.0, count: int = None, anomaly_prob: float = 0.08):
    logger.info("Initializing Local End-to-End Pipeline Simulation...")
    processor = MetricStreamProcessor()
    generator = ServerMetricGenerator(
        server_ids=["server-01", "server-02", "server-03"],
        anomaly_probability=anomaly_prob
    )

    logger.info(f"Pipeline active! Streaming telemetry every {interval}s (Anomaly Prob: {anomaly_prob:.1%})...")
    logger.info("Open the Streamlit Dashboard (streamlit run dashboard/app.py) to view real-time updates.")

    sent = 0
    try:
        while True:
            raw_event = generator.generate_metric()
            processed = processor.process_event(raw_event)
            sent += 1

            if count and sent >= count:
                logger.info(f"Simulation completed after {count} events.")
                break

            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Simulation halted by user.")
    finally:
        processor.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run local telemetry stream simulation")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between metric events")
    parser.add_argument("--count", type=int, default=None, help="Number of events (default: infinite)")
    parser.add_argument("--anomaly-prob", type=float, default=0.08, help="Anomaly probability (0.0 - 1.0)")
    args = parser.parse_args()

    run_simulation(interval=args.interval, count=args.count, anomaly_prob=args.anomaly_prob)

