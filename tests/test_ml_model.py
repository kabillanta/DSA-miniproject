"""
Unit tests for Layer 4: Machine Learning - Isolation Forest Anomaly Detector.
"""

import pytest
from ml.model import ServerAnomalyDetector


@pytest.fixture(scope="module")
def detector():
    return ServerAnomalyDetector()


def test_model_loaded(detector):
    assert detector.is_loaded is True
    assert detector.model is not None


def test_predict_normal_event(detector):
    normal_event = {
        "cpu_usage": 35.0,
        "memory_usage": 52.0,
        "network_traffic": 22.0,
        "response_time": 115.0
    }
    is_anomaly, score = detector.predict(normal_event)
    assert is_anomaly is False
    assert 0.0 <= score <= 1.0
    assert score < 0.50  # Below anomaly threshold


def test_predict_anomalous_event(detector):
    spike_event = {
        "cpu_usage": 99.5,
        "memory_usage": 98.2,
        "network_traffic": 550.0,
        "response_time": 3500.0
    }
    is_anomaly, score = detector.predict(spike_event)
    assert is_anomaly is True
    assert score > 0.50  # Above anomaly threshold


def test_batch_prediction(detector):
    events = [
        {"cpu_usage": 30.0, "memory_usage": 50.0, "network_traffic": 20.0, "response_time": 100.0},
        {"cpu_usage": 98.0, "memory_usage": 99.0, "network_traffic": 600.0, "response_time": 4000.0}
    ]
    results = detector.batch_predict(events)
    assert len(results) == 2
    assert results[0][0] is False
    assert results[1][0] is True

