-- ==============================================================================
-- Real-Time Server Health Monitoring and Anomaly Detection - Database Schema
-- Database: PostgreSQL 15+
-- ==============================================================================

-- Layer 5: Persistent Serving Layer Tables

-- Table 1: Detailed Server Metrics and ML Inferences
CREATE TABLE IF NOT EXISTS server_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    server_id VARCHAR(50) NOT NULL,
    cpu_usage FLOAT NOT NULL,
    memory_usage FLOAT NOT NULL,
    network_traffic FLOAT NOT NULL,
    response_time FLOAT NOT NULL,
    cpu_rolling_avg FLOAT,
    memory_rolling_avg FLOAT,
    network_rolling_avg FLOAT,
    response_time_rolling_avg FLOAT,
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_score FLOAT NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for high-performance dashboard queries and time-series aggregations
CREATE INDEX IF NOT EXISTS idx_server_metrics_timestamp ON server_metrics(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_server_metrics_server_id ON server_metrics(server_id);
CREATE INDEX IF NOT EXISTS idx_server_metrics_is_anomaly ON server_metrics(is_anomaly);
CREATE INDEX IF NOT EXISTS idx_server_metrics_composite ON server_metrics(server_id, timestamp DESC);

-- Table 2: Live Server Health Summary (Continuously Updated Serving View)
CREATE TABLE IF NOT EXISTS server_summary (
    id SERIAL PRIMARY KEY,
    server_id VARCHAR(50) UNIQUE NOT NULL,
    current_cpu FLOAT NOT NULL DEFAULT 0.0,
    current_memory FLOAT NOT NULL DEFAULT 0.0,
    current_network FLOAT NOT NULL DEFAULT 0.0,
    current_response_time FLOAT NOT NULL DEFAULT 0.0,
    peak_cpu FLOAT NOT NULL DEFAULT 0.0,
    peak_memory FLOAT NOT NULL DEFAULT 0.0,
    total_events INTEGER NOT NULL DEFAULT 0,
    total_anomalies INTEGER NOT NULL DEFAULT 0,
    health_status VARCHAR(20) NOT NULL DEFAULT 'HEALTHY',
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_server_summary_server_id ON server_summary(server_id);
