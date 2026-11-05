"""
Layer 1: Data Source - Synthetic Server Metric Generator.
Generates realistic baseline metrics and injects simulated server anomalies.
"""

import math
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple


class MetricValidationError(Exception):
    """Raised when generated or received metric data is invalid."""
    pass


class ServerMetricGenerator:
    """
    Generates synthetic server telemetry for CPU, Memory, Network, and Response Time.
    Simulates realistic baseline continuous dynamics with diurnal-like patterns,
    stochastic noise, and configurable artificial anomaly injections.
    """

    def __init__(
        self,
        server_ids: Optional[List[str]] = None,
        anomaly_probability: float = 0.08,
        seed: Optional[int] = None
    ):
        """
        Args:
            server_ids: List of server identifiers (e.g., ['server-01', 'server-02']).
            anomaly_probability: Probability [0.0, 1.0] of injecting an anomaly.
            seed: Optional random seed for reproducible metric streams.
        """
        if seed is not None:
            random.seed(seed)

        self.server_ids = server_ids or ["server-01", "server-02", "server-03"]
        self.anomaly_probability = max(0.0, min(1.0, anomaly_probability))
        
        # State tracking per server to simulate smooth temporal trends
        self._states = {
            sid: {
                "cpu_base": random.uniform(25.0, 45.0),
                "mem_base": random.uniform(40.0, 60.0),
                "net_base": random.uniform(15.0, 35.0),
                "resp_base": random.uniform(80.0, 160.0),
                "step": 0
            }
            for sid in self.server_ids
        }

    def _next_baseline(self, server_id: str) -> Dict[str, float]:
        """
        Simulate continuous normal operating behavior with random walk + sine wave.
        """
        st = self._states[server_id]
        st["step"] += 1
        t = st["step"]

        # Gentle periodic diurnal variation
        diurnal = math.sin(t * 0.05) * 5.0

        # Normal Gaussian fluctuations around baseline
        cpu = st["cpu_base"] + diurnal + random.gauss(0, 3.5)
        mem = st["mem_base"] + (diurnal * 0.5) + random.gauss(0, 2.0)
        net = st["net_base"] + (diurnal * 1.5) + random.gauss(0, 4.0)
        resp = st["resp_base"] + (diurnal * 4.0) + random.gauss(0, 15.0)

        # Realistic normal operating bounds
        cpu = max(5.0, min(80.0, cpu))
        mem = max(20.0, min(80.0, mem))
        net = max(1.0, min(95.0, net))
        resp = max(15.0, min(350.0, resp))

        return {
            "cpu_usage": round(cpu, 2),
            "memory_usage": round(mem, 2),
            "network_traffic": round(net, 2),
            "response_time": round(resp, 2),
        }

    def _inject_anomaly(self, baseline: Dict[str, float]) -> Tuple[Dict[str, float], str]:
        """
        Inject an abnormal performance spike into the baseline metrics.
        Returns the modified metric dict and a descriptive anomaly label.
        """
        anomaly_type = random.choice([
            "cpu_spike",
            "memory_exhaustion",
            "network_flood",
            "latency_spike",
            "cascading_failure"
        ])
        metrics = dict(baseline)

        if anomaly_type == "cpu_spike":
            # Sudden high CPU load: e.g. runaway process or cryptographic loop
            metrics["cpu_usage"] = round(random.uniform(91.0, 99.8), 2)
            metrics["response_time"] = round(metrics["response_time"] * random.uniform(1.8, 3.2), 2)

        elif anomaly_type == "memory_exhaustion":
            # Memory leak hitting saturation
            metrics["memory_usage"] = round(random.uniform(93.0, 99.5), 2)
            metrics["response_time"] = round(metrics["response_time"] * random.uniform(2.0, 4.0), 2)

        elif anomaly_type == "network_flood":
            # Network spike / DDoS burst
            metrics["network_traffic"] = round(random.uniform(180.0, 650.0), 2)
            metrics["cpu_usage"] = round(min(98.0, metrics["cpu_usage"] + random.uniform(25.0, 45.0)), 2)
            metrics["response_time"] = round(random.uniform(400.0, 1200.0), 2)

        elif anomaly_type == "latency_spike":
            # Heavy database lock or third-party service timeout
            metrics["response_time"] = round(random.uniform(850.0, 3800.0), 2)
            metrics["cpu_usage"] = round(min(95.0, metrics["cpu_usage"] + random.uniform(15.0, 30.0)), 2)

        elif anomaly_type == "cascading_failure":
            # Total system degradation
            metrics["cpu_usage"] = round(random.uniform(92.0, 99.9), 2)
            metrics["memory_usage"] = round(random.uniform(92.0, 99.5), 2)
            metrics["network_traffic"] = round(random.uniform(200.0, 750.0), 2)
            metrics["response_time"] = round(random.uniform(1500.0, 4500.0), 2)

        return metrics, anomaly_type

    def generate_metric(
        self,
        server_id: Optional[str] = None,
        force_anomaly: bool = False,
        force_anomaly_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a single validated server metric event.

        Args:
            server_id: Target server id or random choice if omitted.
            force_anomaly: If True, forces an artificial anomaly injection.
            force_anomaly_type: Specific anomaly type override.

        Returns:
            Dict containing timestamp, server_id, and 4 metric floats.
        """
        sid = server_id if server_id in self.server_ids else random.choice(self.server_ids)
        baseline = self._next_baseline(sid)

        is_anomalous = force_anomaly or (random.random() < self.anomaly_probability)
        anomaly_label = None

        if is_anomalous:
            if force_anomaly_type:
                # Force specific injection
                metrics, anomaly_label = baseline, force_anomaly_type
                if force_anomaly_type == "cpu_spike":
                    metrics["cpu_usage"] = round(random.uniform(92.0, 99.5), 2)
                elif force_anomaly_type == "memory_exhaustion":
                    metrics["memory_usage"] = round(random.uniform(93.0, 99.5), 2)
                elif force_anomaly_type == "network_flood":
                    metrics["network_traffic"] = round(random.uniform(220.0, 500.0), 2)
                elif force_anomaly_type == "latency_spike":
                    metrics["response_time"] = round(random.uniform(1200.0, 3500.0), 2)
                else:
                    metrics, anomaly_label = self._inject_anomaly(baseline)
            else:
                metrics, anomaly_label = self._inject_anomaly(baseline)
        else:
            metrics = baseline

        # Format local system timestamp in Indian Standard Time (IST)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        event = {
            "timestamp": now_ts,
            "server_id": sid,
            "cpu_usage": float(metrics["cpu_usage"]),
            "memory_usage": float(metrics["memory_usage"]),
            "network_traffic": float(metrics["network_traffic"]),
            "response_time": float(metrics["response_time"]),
            "_injected_anomaly": is_anomalous,
            "_anomaly_type": anomaly_label
        }

        # Validate before returning
        return self.validate_metric(event)

    @staticmethod
    def validate_metric(event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates schema, types, and metric ranges.
        Handles missing/corrupt values by applying bounds and type conversion.
        """
        required_fields = ["timestamp", "server_id", "cpu_usage", "memory_usage", "network_traffic", "response_time"]
        for field in required_fields:
            if field not in event or event[field] is None:
                raise MetricValidationError(f"Missing required field: '{field}' in metric event: {event}")

        # Validate timestamp
        try:
            # Check ISO string or datetime
            if isinstance(event["timestamp"], str):
                datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        except Exception as e:
            raise MetricValidationError(f"Invalid timestamp format '{event['timestamp']}': {e}")

        # Validate server_id
        if not isinstance(event["server_id"], str) or not event["server_id"].strip():
            raise MetricValidationError(f"Invalid server_id: {event['server_id']}")

        # Validate numeric bounds
        try:
            cpu = float(event["cpu_usage"])
            mem = float(event["memory_usage"])
            net = float(event["network_traffic"])
            resp = float(event["response_time"])
        except (ValueError, TypeError) as e:
            raise MetricValidationError(f"Non-numeric metric values in event: {e}")

        # Check for NaN or Inf
        for name, val in [("cpu_usage", cpu), ("memory_usage", mem), ("network_traffic", net), ("response_time", resp)]:
            if math.isnan(val) or math.isinf(val):
                raise MetricValidationError(f"Metric '{name}' cannot be NaN or Inf (received: {val})")

        # Sanitize reasonable ranges (clamp non-sensical negatives or >100% percentages)
        cpu = max(0.0, min(100.0, cpu))
        mem = max(0.0, min(100.0, mem))
        net = max(0.0, net)
        resp = max(0.0, resp)

        cleaned_event = dict(event)
        cleaned_event["cpu_usage"] = round(cpu, 2)
        cleaned_event["memory_usage"] = round(mem, 2)
        cleaned_event["network_traffic"] = round(net, 2)
        cleaned_event["response_time"] = round(resp, 2)
        return cleaned_event

