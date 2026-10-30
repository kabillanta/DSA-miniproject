"""
Layer 1: Data Source - Synthetic Server Metric Generator for Docker container.
"""
import math
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple


class MetricValidationError(Exception):
    pass


class ServerMetricGenerator:
    def __init__(
        self,
        server_ids: Optional[List[str]] = None,
        anomaly_probability: float = 0.08,
        seed: Optional[int] = None
    ):
        if seed is not None:
            random.seed(seed)
        self.server_ids = server_ids or ["server-01", "server-02", "server-03"]
        self.anomaly_probability = max(0.0, min(1.0, anomaly_probability))
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
        st = self._states[server_id]
        st["step"] += 1
        t = st["step"]
        diurnal = math.sin(t * 0.05) * 5.0
        cpu = st["cpu_base"] + diurnal + random.gauss(0, 3.5)
        mem = st["mem_base"] + (diurnal * 0.5) + random.gauss(0, 2.0)
        net = st["net_base"] + (diurnal * 1.5) + random.gauss(0, 4.0)
        resp = st["resp_base"] + (diurnal * 4.0) + random.gauss(0, 15.0)

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
        anomaly_type = random.choice([
            "cpu_spike", "memory_exhaustion", "network_flood", "latency_spike", "cascading_failure"
        ])
        metrics = dict(baseline)

        if anomaly_type == "cpu_spike":
            metrics["cpu_usage"] = round(random.uniform(91.0, 99.8), 2)
            metrics["response_time"] = round(metrics["response_time"] * random.uniform(1.8, 3.2), 2)
        elif anomaly_type == "memory_exhaustion":
            metrics["memory_usage"] = round(random.uniform(93.0, 99.5), 2)
            metrics["response_time"] = round(metrics["response_time"] * random.uniform(2.0, 4.0), 2)
        elif anomaly_type == "network_flood":
            metrics["network_traffic"] = round(random.uniform(180.0, 650.0), 2)
            metrics["cpu_usage"] = round(min(98.0, metrics["cpu_usage"] + random.uniform(25.0, 45.0)), 2)
            metrics["response_time"] = round(random.uniform(400.0, 1200.0), 2)
        elif anomaly_type == "latency_spike":
            metrics["response_time"] = round(random.uniform(850.0, 3800.0), 2)
            metrics["cpu_usage"] = round(min(95.0, metrics["cpu_usage"] + random.uniform(15.0, 30.0)), 2)
        elif anomaly_type == "cascading_failure":
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
        sid = server_id if server_id in self.server_ids else random.choice(self.server_ids)
        baseline = self._next_baseline(sid)

        is_anomalous = force_anomaly or (random.random() < self.anomaly_probability)
        anomaly_label = None

        if is_anomalous:
            metrics, anomaly_label = self._inject_anomaly(baseline)
        else:
            metrics = baseline

        now_utc = datetime.now(timezone.utc).isoformat()
        event = {
            "timestamp": now_utc,
            "server_id": sid,
            "cpu_usage": float(metrics["cpu_usage"]),
            "memory_usage": float(metrics["memory_usage"]),
            "network_traffic": float(metrics["network_traffic"]),
            "response_time": float(metrics["response_time"]),
            "_injected_anomaly": is_anomalous,
            "_anomaly_type": anomaly_label
        }
        return self.validate_metric(event)

    @staticmethod
    def validate_metric(event: Dict[str, Any]) -> Dict[str, Any]:
        required = ["timestamp", "server_id", "cpu_usage", "memory_usage", "network_traffic", "response_time"]
        for f in required:
            if f not in event or event[f] is None:
                raise MetricValidationError(f"Missing field '{f}'")
        cpu = max(0.0, min(100.0, float(event["cpu_usage"])))
        mem = max(0.0, min(100.0, float(event["memory_usage"])))
        net = max(0.0, float(event["network_traffic"]))
        resp = max(0.0, float(event["response_time"]))

        cleaned = dict(event)
        cleaned["cpu_usage"] = round(cpu, 2)
        cleaned["memory_usage"] = round(mem, 2)
        cleaned["network_traffic"] = round(net, 2)
        cleaned["response_time"] = round(resp, 2)
        return cleaned

