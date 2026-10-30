"""
Unit tests for Layer 3: Stream Processing - SlidingWindowTracker & MetricStreamProcessor.
"""

import pytest
from processor.stream_processor import SlidingWindowTracker, MetricStreamProcessor
from database.db import DatabaseManager


def test_sliding_window_tracker():
    tracker = SlidingWindowTracker(maxlen=30)
    for i in range(40):
        tracker.add_event({
            "cpu_usage": 50.0,
            "memory_usage": 60.0,
            "network_traffic": 20.0,
            "response_time": 100.0
        }, is_anomaly=False)

    assert len(tracker.window) == 30
    assert tracker.total_events == 40
    assert tracker.total_anomalies == 0
    assert tracker.peak_cpu == 50.0

    roll = tracker.compute_rolling_averages()
    assert roll["cpu_rolling_avg"] == 50.0
    assert roll["memory_rolling_avg"] == 60.0
    assert roll["network_rolling_avg"] == 20.0
    assert roll["response_time_rolling_avg"] == 100.0


def test_health_status_transitions():
    tracker = SlidingWindowTracker(maxlen=30)
    # Normal event
    assert tracker.determine_health_status({
        "cpu_usage": 40.0, "memory_usage": 50.0, "response_time": 120.0
    }, is_anomaly=False) == "HEALTHY"

    # Elevated event -> Warning
    assert tracker.determine_health_status({
        "cpu_usage": 78.0, "memory_usage": 60.0, "response_time": 200.0
    }, is_anomaly=False) == "WARNING"

    # Anomaly -> Critical
    assert tracker.determine_health_status({
        "cpu_usage": 95.0, "memory_usage": 92.0, "response_time": 1200.0
    }, is_anomaly=True) == "CRITICAL"


def test_stream_processor_single_event(tmp_path):
    test_db = DatabaseManager(db_type="sqlite", sqlite_path=str(tmp_path / "test.db"))
    processor = MetricStreamProcessor(db_manager=test_db)

    raw_event = {
        "timestamp": "2026-10-30T12:00:00Z",
        "server_id": "test-srv",
        "cpu_usage": 45.0,
        "memory_usage": 55.0,
        "network_traffic": 30.0,
        "response_time": 150.0
    }
    processed = processor.process_event(raw_event)
    assert processed is not None
    assert processed["server_id"] == "test-srv"
    assert "cpu_rolling_avg" in processed
    assert "is_anomaly" in processed
    assert "anomaly_score" in processed
    test_db.close()

