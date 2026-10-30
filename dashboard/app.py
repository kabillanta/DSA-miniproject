"""
Layer 6: Visualization - Modern Real-Time Streamlit Dashboard.
Monitors server health telemetry, rolling statistics, and Isolation Forest ML anomaly inferences.
Automatically refreshes every 2-3 seconds directly querying the PostgreSQL database.
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

# Ensure root directory is on python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

DASHBOARD_REFRESH_INTERVAL = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "2"))
DASHBOARD_HISTORY_LIMIT = int(os.getenv("DASHBOARD_HISTORY_LIMIT", "100"))

# Attempt to import DatabaseManager from database package; provide self-contained fallback for Docker
try:
    from database.db import DatabaseManager
except ImportError:
    import sqlite3
    import psycopg2

    class DatabaseManager:
        def __init__(self):
            self.host = os.getenv("DB_HOST", "localhost")
            self.port = int(os.getenv("DB_PORT", "5432"))
            self.dbname = os.getenv("DB_NAME", "server_health_db")
            self.user = os.getenv("DB_USER", "postgres")
            self.password = os.getenv("DB_PASSWORD", "postgres")
            self.sqlite_path = os.getenv("SQLITE_DB_PATH", "server_health.db")
            self.conn = None
            self._is_sqlite = False
            self.connect()

        def connect(self):
            try:
                self.conn = psycopg2.connect(
                    host=self.host, port=self.port, dbname=self.dbname,
                    user=self.user, password=self.password, connect_timeout=4
                )
                self.conn.autocommit = True
                self._is_sqlite = False
                return True
            except Exception:
                try:
                    self.conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
                    self._is_sqlite = True
                    return True
                except Exception:
                    return False

        def get_recent_metrics(self, limit=100, server_id=None):
            if not self.conn and not self.connect():
                return pd.DataFrame()
            try:
                ph = "?" if self._is_sqlite else "%s"
                if server_id and server_id != "ALL":
                    q = f"SELECT * FROM server_metrics WHERE server_id = {ph} ORDER BY timestamp DESC LIMIT {ph}"
                    df = pd.read_sql_query(q, self.conn, params=[server_id, limit])
                else:
                    q = f"SELECT * FROM server_metrics ORDER BY timestamp DESC LIMIT {ph}"
                    df = pd.read_sql_query(q, self.conn, params=[limit])
                if not df.empty:
                    df["is_anomaly"] = df["is_anomaly"].astype(bool)
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                return df
            except Exception:
                return pd.DataFrame()

        def get_summary(self, server_id=None):
            if not self.conn and not self.connect():
                return pd.DataFrame()
            try:
                return pd.read_sql_query("SELECT * FROM server_summary ORDER BY server_id ASC", self.conn)
            except Exception:
                return pd.DataFrame()

        def get_aggregate_kpis(self):
            sums = self.get_summary()
            if sums.empty:
                return {"current_cpu": 0.0, "current_memory": 0.0, "current_network": 0.0,
                        "avg_response_time": 0.0, "peak_cpu": 0.0, "peak_memory": 0.0,
                        "total_events": 0, "total_anomalies": 0, "health_status": "HEALTHY"}
            status = "CRITICAL" if "CRITICAL" in sums.get("health_status", "").values else ("WARNING" if "WARNING" in sums.get("health_status", "").values else "HEALTHY")
            return {
                "current_cpu": round(float(sums["current_cpu"].mean()), 2),
                "current_memory": round(float(sums["current_memory"].mean()), 2),
                "current_network": round(float(sums["current_network"].sum()), 2),
                "avg_response_time": round(float(sums["current_response_time"].mean()), 2),
                "peak_cpu": round(float(sums["peak_cpu"].max()), 2),
                "peak_memory": round(float(sums["peak_memory"].max()), 2),
                "total_events": int(sums["total_events"].sum()),
                "total_anomalies": int(sums["total_anomalies"].sum()),
                "health_status": status
            }

        def get_recent_anomalies(self, limit=30):
            if not self.conn and not self.connect():
                return pd.DataFrame()
            try:
                ph = "?" if self._is_sqlite else "%s"
                q = f"SELECT * FROM server_metrics WHERE is_anomaly = 1 OR is_anomaly = TRUE ORDER BY timestamp DESC LIMIT {ph}"
                df = pd.read_sql_query(q, self.conn, params=[limit])
                if not df.empty:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                return df
            except Exception:
                return pd.DataFrame()

# Page Configuration
st.set_page_config(
    page_title="Real-Time Server Health & Anomaly Detection",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom High-Aesthetic Styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1E293B;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.0rem;
        color: #64748B;
        margin-bottom: 1.2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%);
        border-radius: 10px;
        padding: 16px;
        color: #FFFFFF;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        border: 1px solid #334155;
    }
    .badge-healthy {
        background-color: #10B981;
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .badge-warning {
        background-color: #F59E0B;
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .badge-critical {
        background-color: #EF4444;
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.6; }
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db():
    """Singleton Database connection manager."""
    return DatabaseManager()


def main():
    db = get_db()

    # Sidebar Controls
    st.sidebar.image("https://img.icons8.com/fluency/96/server.png", width=64)
    st.sidebar.title("Telemetry Controls")
    
    server_summaries = db.get_summary()
    server_list = ["ALL"]
    if not server_summaries.empty:
        server_list.extend(server_summaries["server_id"].tolist())

    selected_server = st.sidebar.selectbox("Filter by Server ID", server_list, index=0)
    history_limit = st.sidebar.slider("Historical Event Window", min_value=30, max_value=300, value=DASHBOARD_HISTORY_LIMIT, step=10)
    auto_refresh = st.sidebar.checkbox("Auto-Refresh Dashboard", value=True)
    refresh_rate = st.sidebar.slider("Refresh Interval (seconds)", min_value=1, max_value=10, value=DASHBOARD_REFRESH_INTERVAL, step=1)

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Architecture Layers:**")
    st.sidebar.markdown("1. 🔹 Synthetic Data Generator")
    st.sidebar.markdown("2. 🔹 Apache Kafka Broker (`server-metrics`)")
    st.sidebar.markdown("3. 🔹 Sliding-Window Stream Processor (30 Events)")
    st.sidebar.markdown("4. 🔹 Scikit-Learn Isolation Forest")
    st.sidebar.markdown("5. 🔹 PostgreSQL Serving Storage")
    st.sidebar.markdown("6. 🔹 Streamlit Auto-Refreshing UI")
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Last Poll: {datetime.now().strftime('%H:%M:%S')}")

    # Header Title
    col_title, col_status = st.columns([4, 1])
    with col_title:
        st.markdown('<div class="main-header">⚡ Real-Time Server Health & Anomaly Detection</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="sub-header">Continuous Stream Processing with Apache Kafka, Sliding-Window Rolling Analytics & Isolation Forest ML</div>',
            unsafe_allow_html=True
        )

    # Fetch fresh metrics from database
    df_metrics = db.get_recent_metrics(limit=history_limit, server_id=selected_server)
    kpis = db.get_aggregate_kpis()

    with col_status:
        status = kpis["health_status"]
        if status == "CRITICAL":
            st.markdown('<div style="text-align: right; margin-top: 10px;"><span class="badge-critical">🔴 CRITICAL ALERT</span></div>', unsafe_allow_html=True)
        elif status == "WARNING":
            st.markdown('<div style="text-align: right; margin-top: 10px;"><span class="badge-warning">🟡 WARNING</span></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="text-align: right; margin-top: 10px;"><span class="badge-healthy">🟢 ALL HEALTHY</span></div>', unsafe_allow_html=True)

    if df_metrics.empty:
        st.info("👋 Waiting for incoming server telemetry... Ensure the Kafka Producer and Stream Processor are running.")
        st.code("""
# Terminal 1: Train ML Model (if not already trained)
python ml/train.py

# Terminal 2: Start Stream Processor
python -m processor.stream_processor

# Terminal 3: Start Kafka Metric Producer
python -m producer.producer
        """, language="bash")
        if auto_refresh:
            time.sleep(refresh_rate)
            st.rerun()
        return

    # Sort in chronological order for graphs
    df_sorted = df_metrics.sort_values("timestamp", ascending=True).copy()
    latest_event = df_metrics.iloc[0]

    # --------------------------------------------------------------------------
    # Row 1: KPI Summary Cards
    # --------------------------------------------------------------------------
    c1, c2, c3, c4, c5, c6 = st.columns(6)

    cpu_val = latest_event["cpu_usage"]
    cpu_roll = latest_event["cpu_rolling_avg"]
    cpu_delta = f"{cpu_val - cpu_roll:+.1f}% vs roll"

    mem_val = latest_event["memory_usage"]
    mem_roll = latest_event["memory_rolling_avg"]
    mem_delta = f"{mem_val - mem_roll:+.1f}% vs roll"

    with c1:
        st.metric(label="Current CPU", value=f"{cpu_val:.1f}%", delta=cpu_delta)
    with c2:
        st.metric(label="Current Memory", value=f"{mem_val:.1f}%", delta=mem_delta)
    with c3:
        st.metric(label="Network Traffic", value=f"{latest_event['network_traffic']:.1f} MB/s")
    with c4:
        st.metric(label="Avg Response Time", value=f"{latest_event['response_time']:.1f} ms")
    with c5:
        total_anom = kpis["total_anomalies"]
        st.metric(label="Total Anomalies", value=f"{total_anom}", delta="Detected by ML" if total_anom > 0 else "Normal")
    with c6:
        st.metric(label="Total Processed", value=f"{kpis['total_events']} events")

    st.markdown("---")

    # --------------------------------------------------------------------------
    # Row 2: Live Charts with Rolling Averages and Red Anomaly Markers
    # --------------------------------------------------------------------------
    chart_col1, chart_col2 = st.columns(2)

    # 1. CPU Utilization Graph
    with chart_col1:
        fig_cpu = go.Figure()
        # Normal trace
        fig_cpu.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["cpu_usage"],
            mode="lines",
            name="CPU Live (%)",
            line=dict(color="#00D2FF", width=2)
        ))
        # 30-Event Rolling Average
        fig_cpu.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["cpu_rolling_avg"],
            mode="lines",
            name="30-Event Rolling Avg",
            line=dict(color="#FFA500", width=2, dash="dot")
        ))
        # Anomalies Highlighted in Red
        anomalies_cpu = df_sorted[df_sorted["is_anomaly"] == True]
        if not anomalies_cpu.empty:
            fig_cpu.add_trace(go.Scatter(
                x=anomalies_cpu["timestamp"],
                y=anomalies_cpu["cpu_usage"],
                mode="markers",
                name="🚨 Anomaly Detected",
                marker=dict(color="#EF4444", size=10, symbol="x", line=dict(width=2, color="#FFFFFF"))
            ))

        fig_cpu.update_layout(
            title="<b>CPU Utilization (%) & Rolling Trend</b>",
            xaxis_title="Timestamp",
            yaxis_title="CPU (%)",
            yaxis_range=[0, 105],
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=40, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_cpu, use_container_width=True)

    # 2. Memory Utilization Graph
    with chart_col2:
        fig_mem = go.Figure()
        fig_mem.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["memory_usage"],
            mode="lines",
            name="Memory Live (%)",
            line=dict(color="#A855F7", width=2)
        ))
        fig_mem.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["memory_rolling_avg"],
            mode="lines",
            name="30-Event Rolling Avg",
            line=dict(color="#FFA500", width=2, dash="dot")
        ))
        anomalies_mem = df_sorted[df_sorted["is_anomaly"] == True]
        if not anomalies_mem.empty:
            fig_mem.add_trace(go.Scatter(
                x=anomalies_mem["timestamp"],
                y=anomalies_mem["memory_usage"],
                mode="markers",
                name="🚨 Anomaly Detected",
                marker=dict(color="#EF4444", size=10, symbol="x", line=dict(width=2, color="#FFFFFF"))
            ))

        fig_mem.update_layout(
            title="<b>Memory Utilization (%) & Rolling Trend</b>",
            xaxis_title="Timestamp",
            yaxis_title="Memory (%)",
            yaxis_range=[0, 105],
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=40, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_mem, use_container_width=True)

    # --------------------------------------------------------------------------
    # Row 3: Network Traffic & Response Time Graphs
    # --------------------------------------------------------------------------
    chart_col3, chart_col4 = st.columns(2)

    # 3. Network Traffic Graph
    with chart_col3:
        fig_net = go.Figure()
        fig_net.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["network_traffic"],
            mode="lines",
            fill="tozeroy",
            name="Throughput (MB/s)",
            line=dict(color="#3B82F6", width=2),
            fillcolor="rgba(59, 130, 246, 0.15)"
        ))
        anomalies_net = df_sorted[df_sorted["is_anomaly"] == True]
        if not anomalies_net.empty:
            fig_net.add_trace(go.Scatter(
                x=anomalies_net["timestamp"],
                y=anomalies_net["network_traffic"],
                mode="markers",
                name="🚨 Traffic Spike",
                marker=dict(color="#EF4444", size=10, symbol="x")
            ))

        fig_net.update_layout(
            title="<b>Network Traffic (MB/s)</b>",
            xaxis_title="Timestamp",
            yaxis_title="MB/s",
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=40, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_net, use_container_width=True)

    # 4. Response Time Graph
    with chart_col4:
        fig_resp = go.Figure()
        fig_resp.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["response_time"],
            mode="lines",
            name="Latency (ms)",
            line=dict(color="#10B981", width=2)
        ))
        fig_resp.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["response_time_rolling_avg"],
            mode="lines",
            name="Rolling Latency Avg",
            line=dict(color="#FFA500", width=2, dash="dot")
        ))
        anomalies_resp = df_sorted[df_sorted["is_anomaly"] == True]
        if not anomalies_resp.empty:
            fig_resp.add_trace(go.Scatter(
                x=anomalies_resp["timestamp"],
                y=anomalies_resp["response_time"],
                mode="markers",
                name="🚨 Latency Spike",
                marker=dict(color="#EF4444", size=10, symbol="x")
            ))

        fig_resp.update_layout(
            title="<b>Server Latency & Response Time (ms)</b>",
            xaxis_title="Timestamp",
            yaxis_title="Response Time (ms)",
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=40, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_resp, use_container_width=True)

    # --------------------------------------------------------------------------
    # Row 4: Isolation Forest Anomaly Confidence Score Timeline
    # --------------------------------------------------------------------------
    st.subheader("🤖 Isolation Forest Anomaly Confidence Timeline")
    fig_score = go.Figure()
    fig_score.add_trace(go.Scatter(
        x=df_sorted["timestamp"],
        y=df_sorted["anomaly_score"],
        mode="lines+markers",
        name="ML Anomaly Score",
        line=dict(color="#64748B", width=1.5),
        marker=dict(
            size=6,
            color=df_sorted["is_anomaly"].map({True: "#EF4444", False: "#10B981"})
        )
    ))
    # Threshold indicator line at 0.5
    fig_score.add_hline(
        y=0.5,
        line_dash="dash",
        line_color="#EF4444",
        annotation_text="Anomaly Decision Boundary (Score = 0.50)",
        annotation_position="bottom right"
    )
    fig_score.update_layout(
        xaxis_title="Timestamp",
        yaxis_title="Anomaly Confidence [0.0 - 1.0]",
        yaxis_range=[0, 1.05],
        template="plotly_white",
        height=260,
        margin=dict(l=40, r=20, t=30, b=30)
    )
    st.plotly_chart(fig_score, use_container_width=True)

    # --------------------------------------------------------------------------
    # Row 5: Detailed Data Tables & Anomaly Alerts
    # --------------------------------------------------------------------------
    tab1, tab2, tab3 = st.tabs(["📋 Recent Server Events", "🚨 Anomaly Alerts Log", "🖥️ Server Fleet Summary"])

    with tab1:
        st.markdown(f"**Showing most recent {len(df_metrics)} events (Anomalies highlighted in Red):**")
        
        display_cols = [
            "timestamp", "server_id", "cpu_usage", "memory_usage",
            "network_traffic", "response_time", "cpu_rolling_avg",
            "is_anomaly", "anomaly_score"
        ]
        display_df = df_metrics[display_cols].copy()
        display_df["timestamp"] = display_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        def style_anomalies(row):
            if row["is_anomaly"]:
                return ["background-color: rgba(239, 68, 68, 0.2); font-weight: bold; color: #B91C1C;"] * len(row)
            return [""] * len(row)

        st.dataframe(
            display_df.style.apply(style_anomalies, axis=1).format({
                "cpu_usage": "{:.2f}%",
                "memory_usage": "{:.2f}%",
                "network_traffic": "{:.2f} MB/s",
                "response_time": "{:.1f} ms",
                "cpu_rolling_avg": "{:.2f}%",
                "anomaly_score": "{:.4f}"
            }),
            use_container_width=True,
            height=320
        )

    with tab2:
        df_anomalies = db.get_recent_anomalies(limit=30)
        if df_anomalies.empty:
            st.success("✅ No anomalies detected in recent historical window. All systems nominal.")
        else:
            st.warning(f"⚠️ {len(df_anomalies)} anomalous server events recorded recently:")
            for _, anom in df_anomalies.iterrows():
                ts_str = anom["timestamp"].strftime("%Y-%m-%d %H:%M:%S UTC")
                with st.expander(
                    f"🔴 [{ts_str}] Server {anom['server_id']} — Anomaly Score: {anom['anomaly_score']:.3f}"
                ):
                    c_a, c_b, c_c, c_d = st.columns(4)
                    c_a.metric("CPU Spiked To", f"{anom['cpu_usage']:.1f}%")
                    c_b.metric("Memory Spiked To", f"{anom['memory_usage']:.1f}%")
                    c_c.metric("Network Traffic", f"{anom['network_traffic']:.1f} MB/s")
                    c_d.metric("Response Time", f"{anom['response_time']:.1f} ms")

    with tab3:
        if not server_summaries.empty:
            st.dataframe(
                server_summaries.format({
                    "current_cpu": "{:.2f}%",
                    "current_memory": "{:.2f}%",
                    "current_network": "{:.2f} MB/s",
                    "current_response_time": "{:.1f} ms",
                    "peak_cpu": "{:.2f}%",
                    "peak_memory": "{:.2f}%"
                }),
                use_container_width=True
            )
        else:
            st.info("No server summaries available yet.")

    # Auto-refresh loop
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()


if __name__ == "__main__":
    main()
