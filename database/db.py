"""
Layer 5: Persistent Serving Layer - Database Manager.
Manages database connection, auto-creates schema, executes parameterized queries,
maintains server_summary records, and provides high-performance data retrieval for Streamlit.
Supports PostgreSQL (production) with automatic SQLite local fallback.
"""

import os
import sys
import time
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# Ensure root directory is on python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
from config.config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_TYPE, SQLITE_DB_PATH
)

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages persistent storage for server metrics, sliding rolling statistics,
    ML anomaly inferences, and real-time summary statistics.
    """

    def __init__(
        self,
        db_type: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        sqlite_path: Optional[str] = None
    ):
        self.db_type = (db_type or DB_TYPE).lower()
        self.host = host or DB_HOST
        self.port = port or DB_PORT
        self.dbname = dbname or DB_NAME
        self.user = user or DB_USER
        self.password = password or DB_PASSWORD
        self.sqlite_path = sqlite_path or SQLITE_DB_PATH

        self.conn = None
        self._is_sqlite = False

        self.connect()
        self.create_tables_if_not_exist()

    def connect(self, max_retries: int = 2, retry_delay: float = 0.5) -> bool:
        """
        Establishes database connection with automatic reconnection and fallback.
        """
        # If explicitly configured as SQLite, connect immediately
        if self.db_type == "sqlite":
            return self._connect_sqlite()

        # Attempt PostgreSQL connection
        import psycopg2
        for attempt in range(1, max_retries + 1):
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    connect_timeout=5
                )
                self.conn.autocommit = True
                self._is_sqlite = False
                logger.info(f"Connected to PostgreSQL database '{self.dbname}' at {self.host}:{self.port}")
                return True
            except Exception as e:
                logger.warning(
                    f"PostgreSQL connection attempt {attempt}/{max_retries} failed ({self.host}:{self.port}): {e}"
                )
                if attempt < max_retries:
                    time.sleep(retry_delay)

        # Fallback to SQLite if PostgreSQL fails and allow seamless continuous operation
        logger.warning("PostgreSQL unreachable. Falling back to local SQLite database.")
        return self._connect_sqlite()

    def _connect_sqlite(self) -> bool:
        """Connect to local SQLite database."""
        try:
            db_dir = Path(self.sqlite_path).parent
            db_dir.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
            self.conn.isolation_level = None  # Autocommit
            self._is_sqlite = True
            logger.info(f"Connected to local SQLite database at: {self.sqlite_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            return False

    def check_connection(self) -> bool:
        """Verifies active connection or triggers reconnect."""
        try:
            if self.conn is None:
                return self.connect(max_retries=1)
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            logger.info("Database connection lost. Reconnecting...")
            return self.connect(max_retries=2)

    def create_tables_if_not_exist(self):
        """
        Automatically creates required tables (server_metrics, server_summary)
        and performance indexes if they do not exist.
        """
        if not self.check_connection():
            logger.error("Cannot create tables: Database not connected.")
            return

        cursor = self.conn.cursor()
        try:
            if self._is_sqlite:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS server_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        server_id TEXT NOT NULL,
                        cpu_usage REAL NOT NULL,
                        memory_usage REAL NOT NULL,
                        network_traffic REAL NOT NULL,
                        response_time REAL NOT NULL,
                        cpu_rolling_avg REAL,
                        memory_rolling_avg REAL,
                        network_rolling_avg REAL,
                        response_time_rolling_avg REAL,
                        is_anomaly INTEGER NOT NULL DEFAULT 0,
                        anomaly_score REAL NOT NULL DEFAULT 0.0,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_server_metrics_ts ON server_metrics(timestamp DESC);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_server_metrics_sid ON server_metrics(server_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_server_metrics_anom ON server_metrics(is_anomaly);")

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS server_summary (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        server_id TEXT UNIQUE NOT NULL,
                        current_cpu REAL NOT NULL DEFAULT 0.0,
                        current_memory REAL NOT NULL DEFAULT 0.0,
                        current_network REAL NOT NULL DEFAULT 0.0,
                        current_response_time REAL NOT NULL DEFAULT 0.0,
                        peak_cpu REAL NOT NULL DEFAULT 0.0,
                        peak_memory REAL NOT NULL DEFAULT 0.0,
                        total_events INTEGER NOT NULL DEFAULT 0,
                        total_anomalies INTEGER NOT NULL DEFAULT 0,
                        health_status TEXT NOT NULL DEFAULT 'HEALTHY',
                        last_updated TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                """)
            else:
                cursor.execute("""
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
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_server_metrics_timestamp ON server_metrics(timestamp DESC);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_server_metrics_server_id ON server_metrics(server_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_server_metrics_is_anomaly ON server_metrics(is_anomaly);")

                cursor.execute("""
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
                """)
            logger.info("Verified/created database schema (server_metrics, server_summary).")
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
        finally:
            cursor.close()

    def insert_metric(self, event: Dict[str, Any]) -> Optional[int]:
        """
        Inserts a single processed metric record with ML inferences and rolling averages.
        Uses parameterized query to prevent SQL injection.
        """
        if not self.check_connection():
            logger.error("Cannot insert metric: Database disconnected.")
            return None

        cursor = self.conn.cursor()
        try:
            ts = event["timestamp"]
            sid = event["server_id"]
            cpu = float(event["cpu_usage"])
            mem = float(event["memory_usage"])
            net = float(event["network_traffic"])
            resp = float(event["response_time"])
            cpu_roll = float(event.get("cpu_rolling_avg", cpu))
            mem_roll = float(event.get("memory_rolling_avg", mem))
            net_roll = float(event.get("network_rolling_avg", net))
            resp_roll = float(event.get("response_time_rolling_avg", resp))
            is_anomaly = bool(event.get("is_anomaly", False))
            anomaly_score = float(event.get("anomaly_score", 0.0))

            if self._is_sqlite:
                query = """
                    INSERT INTO server_metrics (
                        timestamp, server_id, cpu_usage, memory_usage,
                        network_traffic, response_time, cpu_rolling_avg,
                        memory_rolling_avg, network_rolling_avg, response_time_rolling_avg,
                        is_anomaly, anomaly_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(query, (
                    ts, sid, cpu, mem, net, resp,
                    cpu_roll, mem_roll, net_roll, resp_roll,
                    1 if is_anomaly else 0, anomaly_score
                ))
                inserted_id = cursor.lastrowid
            else:
                query = """
                    INSERT INTO server_metrics (
                        timestamp, server_id, cpu_usage, memory_usage,
                        network_traffic, response_time, cpu_rolling_avg,
                        memory_rolling_avg, network_rolling_avg, response_time_rolling_avg,
                        is_anomaly, anomaly_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                cursor.execute(query, (
                    ts, sid, cpu, mem, net, resp,
                    cpu_roll, mem_roll, net_roll, resp_roll,
                    is_anomaly, anomaly_score
                ))
                inserted_id = cursor.fetchone()[0]

            return inserted_id
        except Exception as e:
            logger.error(f"Error inserting metric record: {e}")
            return None
        finally:
            cursor.close()

    def update_summary(
        self,
        server_id: str,
        current_cpu: float,
        current_memory: float,
        current_network: float,
        current_response_time: float,
        peak_cpu: float,
        peak_memory: float,
        total_events: int,
        total_anomalies: int,
        health_status: str = "HEALTHY"
    ) -> bool:
        """
        Updates or inserts live server health summary record.
        """
        if not self.check_connection():
            return False

        cursor = self.conn.cursor()
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            if self._is_sqlite:
                query = """
                    INSERT INTO server_summary (
                        server_id, current_cpu, current_memory, current_network,
                        current_response_time, peak_cpu, peak_memory, total_events,
                        total_anomalies, health_status, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(server_id) DO UPDATE SET
                        current_cpu = excluded.current_cpu,
                        current_memory = excluded.current_memory,
                        current_network = excluded.current_network,
                        current_response_time = excluded.current_response_time,
                        peak_cpu = MAX(server_summary.peak_cpu, excluded.peak_cpu),
                        peak_memory = MAX(server_summary.peak_memory, excluded.peak_memory),
                        total_events = excluded.total_events,
                        total_anomalies = excluded.total_anomalies,
                        health_status = excluded.health_status,
                        last_updated = excluded.last_updated
                """
                cursor.execute(query, (
                    server_id, current_cpu, current_memory, current_network,
                    current_response_time, peak_cpu, peak_memory, total_events,
                    total_anomalies, health_status, now_iso
                ))
            else:
                query = """
                    INSERT INTO server_summary (
                        server_id, current_cpu, current_memory, current_network,
                        current_response_time, peak_cpu, peak_memory, total_events,
                        total_anomalies, health_status, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT(server_id) DO UPDATE SET
                        current_cpu = EXCLUDED.current_cpu,
                        current_memory = EXCLUDED.current_memory,
                        current_network = EXCLUDED.current_network,
                        current_response_time = EXCLUDED.current_response_time,
                        peak_cpu = GREATEST(server_summary.peak_cpu, EXCLUDED.peak_cpu),
                        peak_memory = GREATEST(server_summary.peak_memory, EXCLUDED.peak_memory),
                        total_events = EXCLUDED.total_events,
                        total_anomalies = EXCLUDED.total_anomalies,
                        health_status = EXCLUDED.health_status,
                        last_updated = NOW()
                """
                cursor.execute(query, (
                    server_id, current_cpu, current_memory, current_network,
                    current_response_time, peak_cpu, peak_memory, total_events,
                    total_anomalies, health_status
                ))
            return True
        except Exception as e:
            logger.error(f"Error updating server_summary for {server_id}: {e}")
            return False
        finally:
            cursor.close()

    def get_recent_metrics(
        self,
        limit: int = 100,
        server_id: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieves recent metrics ordered by timestamp descending.
        Used by the Streamlit dashboard to power live graphs and recent event tables.
        """
        if not self.check_connection():
            return pd.DataFrame()

        try:
            ph = "?" if self._is_sqlite else "%s"
            if server_id and server_id != "ALL":
                query = f"""
                    SELECT id, timestamp, server_id, cpu_usage, memory_usage,
                           network_traffic, response_time, cpu_rolling_avg,
                           memory_rolling_avg, network_rolling_avg, response_time_rolling_avg,
                           is_anomaly, anomaly_score, created_at
                    FROM server_metrics
                    WHERE server_id = {ph}
                    ORDER BY timestamp DESC
                    LIMIT {ph}
                """
                df = pd.read_sql_query(query, self.conn, params=[server_id, limit])
            else:
                query = f"""
                    SELECT id, timestamp, server_id, cpu_usage, memory_usage,
                           network_traffic, response_time, cpu_rolling_avg,
                           memory_rolling_avg, network_rolling_avg, response_time_rolling_avg,
                           is_anomaly, anomaly_score, created_at
                    FROM server_metrics
                    ORDER BY timestamp DESC
                    LIMIT {ph}
                """
                df = pd.read_sql_query(query, self.conn, params=[limit])

            if not df.empty:
                df["is_anomaly"] = df["is_anomaly"].astype(bool)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        except Exception as e:
            logger.error(f"Error fetching recent metrics: {e}")
            return pd.DataFrame()

    def get_summary(self, server_id: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieves server summaries for all or a specific server.
        """
        if not self.check_connection():
            return pd.DataFrame()

        try:
            ph = "?" if self._is_sqlite else "%s"
            if server_id and server_id != "ALL":
                query = f"SELECT * FROM server_summary WHERE server_id = {ph}"
                df = pd.read_sql_query(query, self.conn, params=[server_id])
            else:
                query = "SELECT * FROM server_summary ORDER BY server_id ASC"
                df = pd.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            logger.error(f"Error fetching server summary: {e}")
            return pd.DataFrame()

    def get_aggregate_kpis(self) -> Dict[str, Any]:
        """
        Computes system-wide aggregate KPIs across all monitored servers.
        """
        default_kpis = {
            "current_cpu": 0.0,
            "current_memory": 0.0,
            "current_network": 0.0,
            "avg_response_time": 0.0,
            "peak_cpu": 0.0,
            "peak_memory": 0.0,
            "total_events": 0,
            "total_anomalies": 0,
            "health_status": "HEALTHY"
        }

        if not self.check_connection():
            return default_kpis

        try:
            summaries = self.get_summary()
            if summaries.empty:
                return default_kpis

            total_events = int(summaries["total_events"].sum())
            total_anomalies = int(summaries["total_anomalies"].sum())
            avg_cpu = float(summaries["current_cpu"].mean())
            avg_mem = float(summaries["current_memory"].mean())
            avg_net = float(summaries["current_network"].sum())
            avg_resp = float(summaries["current_response_time"].mean())
            peak_cpu = float(summaries["peak_cpu"].max())
            peak_memory = float(summaries["peak_memory"].max())

            # Evaluate cluster health status
            if "CRITICAL" in summaries["health_status"].values:
                status = "CRITICAL"
            elif "WARNING" in summaries["health_status"].values:
                status = "WARNING"
            else:
                status = "HEALTHY"

            return {
                "current_cpu": round(avg_cpu, 2),
                "current_memory": round(avg_mem, 2),
                "current_network": round(avg_net, 2),
                "avg_response_time": round(avg_resp, 2),
                "peak_cpu": round(peak_cpu, 2),
                "peak_memory": round(peak_memory, 2),
                "total_events": total_events,
                "total_anomalies": total_anomalies,
                "health_status": status
            }
        except Exception as e:
            logger.error(f"Error computing aggregate KPIs: {e}")
            return default_kpis

    def get_recent_anomalies(self, limit: int = 30) -> pd.DataFrame:
        """
        Retrieves recent anomalous events for alerts view.
        """
        if not self.check_connection():
            return pd.DataFrame()

        try:
            ph = "?" if self._is_sqlite else "%s"
            query = f"""
                SELECT id, timestamp, server_id, cpu_usage, memory_usage,
                       network_traffic, response_time, anomaly_score
                FROM server_metrics
                WHERE is_anomaly = 1 OR is_anomaly = TRUE
                ORDER BY timestamp DESC
                LIMIT {ph}
            """
            df = pd.read_sql_query(query, self.conn, params=[limit])
            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        except Exception as e:
            logger.error(f"Error fetching recent anomalies: {e}")
            return pd.DataFrame()

    def close(self):
        """Clean up connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Database connection closed.")
            except Exception:
                pass
