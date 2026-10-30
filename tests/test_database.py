"""
Unit tests for Layer 5: Persistent Serving Layer - DatabaseManager.
"""

import pytest
from database.db import DatabaseManager


@pytest.fixture
def temp_db(tmp_path):
    db_file = str(tmp_path / "test_health.db")
    manager = DatabaseManager(db_type="sqlite", sqlite_path=db_file)
    yield manager
    manager.close()


def test_database_table_creation(temp_db):
    assert temp_db.check_connection() is True
    # Query tables
    cur = temp_db.conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cur.fetchall()]
    assert "server_metrics" in tables
    assert "server_summary" in tables


def test_insert_and_retrieve_metric(temp_db):
    event = {
        "timestamp": "2026-10-30T10:00:00Z",
        "server_id": "srv-01",
        "cpu_usage": 42.5,
        "memory_usage": 58.0,
        "network_traffic": 18.2,
        "response_time": 95.0,
        "cpu_rolling_avg": 40.0,
        "memory_rolling_avg": 55.0,
        "network_rolling_avg": 17.0,
        "response_time_rolling_avg": 90.0,
        "is_anomaly": False,
        "anomaly_score": 0.12
    }
    inserted_id = temp_db.insert_metric(event)
    assert inserted_id is not None

    df = temp_db.get_recent_metrics(limit=10)
    assert len(df) == 1
    assert df.iloc[0]["server_id"] == "srv-01"
    assert df.iloc[0]["cpu_usage"] == 42.5
    assert df.iloc[0]["is_anomaly"] == False


def test_update_and_get_summary(temp_db):
    temp_db.update_summary(
        server_id="srv-01",
        current_cpu=45.0,
        current_memory=60.0,
        current_network=25.0,
        current_response_time=110.0,
        peak_cpu=80.0,
        peak_memory=75.0,
        total_events=100,
        total_anomalies=3,
        health_status="HEALTHY"
    )

    summaries = temp_db.get_summary(server_id="srv-01")
    assert len(summaries) == 1
    assert summaries.iloc[0]["current_cpu"] == 45.0
    assert summaries.iloc[0]["peak_cpu"] == 80.0
    assert summaries.iloc[0]["total_events"] == 100
    assert summaries.iloc[0]["total_anomalies"] == 3

    kpis = temp_db.get_aggregate_kpis()
    assert kpis["total_events"] == 100
    assert kpis["total_anomalies"] == 3
    assert kpis["health_status"] == "HEALTHY"

