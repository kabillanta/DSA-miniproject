"""
End-to-End Integration Test for All Six Layers.
Verifies the complete pipeline:
Data Generator -> Processing -> ML Scoring -> Database Persistence -> Dashboard Query Retrieval.
"""

import pytest
from producer.generator import ServerMetricGenerator
from ml.model import ServerAnomalyDetector
from processor.stream_processor import MetricStreamProcessor
from database.db import DatabaseManager


def test_complete_end_to_end_pipeline(tmp_path):
    # 1. Initialize Database Layer
    db_file = str(tmp_path / "e2e_pipeline.db")
    db = DatabaseManager(db_type="sqlite", sqlite_path=db_file)

    # 2. Initialize Stream Processor (loads ML model once and uses DB)
    processor = MetricStreamProcessor(db_manager=db)

    # 3. Initialize Data Source Generator
    generator = ServerMetricGenerator(
        server_ids=["prod-web-01", "prod-web-02"],
        anomaly_probability=0.1,
        seed=999
    )

    # 4. Stream 50 telemetry events
    normal_events_count = 0
    anomalous_events_count = 0

    for i in range(50):
        # Force an anomaly at event 25
        force_anomaly = (i == 25)
        raw_event = generator.generate_metric(force_anomaly=force_anomaly)

        processed = processor.process_event(raw_event)
        assert processed is not None
        assert "cpu_rolling_avg" in processed
        assert "is_anomaly" in processed
        assert "anomaly_score" in processed

        if processed["is_anomaly"]:
            anomalous_events_count += 1
        else:
            normal_events_count += 1

    # 5. Verify Serving Layer Storage
    recent_metrics = db.get_recent_metrics(limit=100)
    assert len(recent_metrics) == 50

    # Verify chronological ordering and rolling averages
    latest = recent_metrics.iloc[0]
    assert latest["cpu_rolling_avg"] > 0
    assert latest["memory_rolling_avg"] > 0

    # 6. Verify Summary Aggregations
    summary = db.get_summary()
    assert len(summary) >= 1
    total_recorded = summary["total_events"].sum()
    assert total_recorded == 50

    # 7. Verify Anomaly Retrieval for Dashboard Alerts
    kpis = db.get_aggregate_kpis()
    assert kpis["total_events"] == 50
    assert kpis["total_anomalies"] >= 1  # At least the forced one at index 25

    anomalies_df = db.get_recent_anomalies()
    assert not anomalies_df.empty
    assert (anomalies_df["anomaly_score"] > 0.5).all()

    # Clean up
    db.close()

