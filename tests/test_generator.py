"""
Unit tests for Layer 1: Data Source - ServerMetricGenerator.
"""

import pytest
from producer.generator import ServerMetricGenerator, MetricValidationError


def test_generator_initialization():
    gen = ServerMetricGenerator(server_ids=["srv-1", "srv-2"], anomaly_probability=0.1)
    assert gen.server_ids == ["srv-1", "srv-2"]
    assert gen.anomaly_probability == 0.1


def test_baseline_metric_ranges():
    gen = ServerMetricGenerator(server_ids=["srv-1"], anomaly_probability=0.0, seed=123)
    for _ in range(50):
        metric = gen.generate_metric()
        assert metric["server_id"] == "srv-1"
        assert 0.0 <= metric["cpu_usage"] <= 100.0
        assert 0.0 <= metric["memory_usage"] <= 100.0
        assert metric["network_traffic"] >= 0.0
        assert metric["response_time"] >= 0.0
        assert metric["_injected_anomaly"] is False


def test_forced_anomaly_generation():
    gen = ServerMetricGenerator(server_ids=["srv-1"], seed=42)
    metric = gen.generate_metric(force_anomaly=True)
    assert metric["_injected_anomaly"] is True
    assert metric["_anomaly_type"] in [
        "cpu_spike", "memory_exhaustion", "network_flood", "latency_spike", "cascading_failure"
    ]


def test_validation_missing_field():
    with pytest.raises(MetricValidationError):
        ServerMetricGenerator.validate_metric({
            "timestamp": "2026-10-30T00:00:00Z",
            "server_id": "srv-1",
            "cpu_usage": 50.0
            # missing memory, network, response
        })


def test_validation_out_of_bounds_clamping():
    event = {
        "timestamp": "2026-10-30T00:00:00Z",
        "server_id": "srv-1",
        "cpu_usage": 150.0,  # Should be clamped to 100.0
        "memory_usage": -10.0, # Should be clamped to 0.0
        "network_traffic": 25.0,
        "response_time": 100.0
    }
    cleaned = ServerMetricGenerator.validate_metric(event)
    assert cleaned["cpu_usage"] == 100.0
    assert cleaned["memory_usage"] == 0.0

