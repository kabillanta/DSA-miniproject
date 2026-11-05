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
                    ts = pd.to_datetime(df["timestamp"])
                    try:
                        local_tz = datetime.now().astimezone().tzinfo
                        if ts.dt.tz is not None:
                            df["timestamp"] = ts.dt.tz_convert(local_tz).dt.tz_localize(None)
                        else:
                            df["timestamp"] = ts
                    except Exception:
                        df["timestamp"] = ts
                return df
            except Exception:
                return pd.DataFrame()

        def get_summary(self, server_id=None):
            if not self.conn and not self.connect():
                return pd.DataFrame()
            try:
                ph = "?" if self._is_sqlite else "%s"
                if server_id and server_id != "ALL":
                    q = f"SELECT * FROM server_summary WHERE server_id = {ph}"
                    return pd.read_sql_query(q, self.conn, params=[server_id])
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
                "current_cpu": round(float(sums["current_cpu"].mean()), 2) if "current_cpu" in sums else 0.0,
                "current_memory": round(float(sums["current_memory"].mean()), 2) if "current_memory" in sums else 0.0,
                "current_network": round(float(sums["current_network"].sum()), 2) if "current_network" in sums else 0.0,
                "avg_response_time": round(float(sums["current_response_time"].mean()), 2) if "current_response_time" in sums else 0.0,
                "peak_cpu": round(float(sums["peak_cpu"].max()), 2) if "peak_cpu" in sums else 0.0,
                "peak_memory": round(float(sums["peak_memory"].max()), 2) if "peak_memory" in sums else 0.0,
                "total_events": int(sums["total_events"].sum()) if "total_events" in sums else 0,
                "total_anomalies": int(sums["total_anomalies"].sum()) if "total_anomalies" in sums else 0,
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
                    ts = pd.to_datetime(df["timestamp"])
                    try:
                        local_tz = datetime.now().astimezone().tzinfo
                        if ts.dt.tz is not None:
                            df["timestamp"] = ts.dt.tz_convert(local_tz).dt.tz_localize(None)
                        else:
                            df["timestamp"] = ts
                    except Exception:
                        df["timestamp"] = ts
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

# Custom High-Aesthetic Enterprise CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    .main-header {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(135deg, #0F172A 0%, #2563EB 50%, #38BDF8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
        letter-spacing: -0.02em;
    }
    
    .sub-header {
        font-size: 0.95rem;
        color: #64748B;
        margin-bottom: 1.2rem;
        font-weight: 500;
    }

    /* Modern Glassmorphic Metric Cards */
    .metric-card-container {
        background: linear-gradient(145deg, #FFFFFF, #F8FAFC);
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 18px 20px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.03);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        position: relative;
        overflow: hidden;
    }
    .metric-card-container:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.06);
    }
    .metric-card-title {
        font-size: 0.82rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
        color: #64748B;
        margin-bottom: 6px;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .metric-card-value {
        font-size: 1.85rem;
        font-weight: 700;
        color: #0F172A;
        letter-spacing: -0.02em;
    }
    .metric-card-delta {
        font-size: 0.8rem;
        font-weight: 600;
        margin-top: 4px;
    }
    .delta-pos { color: #DC2626; }
    .delta-neg { color: #16A34A; }
    .delta-neutral { color: #64748B; }

    /* Health Badges */
    .badge-healthy {
        background: linear-gradient(135deg, #10B981, #059669);
        color: white;
        padding: 6px 16px;
        border-radius: 9999px;
        font-weight: 700;
        font-size: 0.85rem;
        display: inline-flex;
        align-items: center;
        gap: 6px;
        box-shadow: 0 4px 10px rgba(16, 185, 129, 0.3);
    }
    .badge-warning {
        background: linear-gradient(135deg, #F59E0B, #D97706);
        color: white;
        padding: 6px 16px;
        border-radius: 9999px;
        font-weight: 700;
        font-size: 0.85rem;
        display: inline-flex;
        align-items: center;
        gap: 6px;
        box-shadow: 0 4px 10px rgba(245, 158, 11, 0.3);
    }
    .badge-critical {
        background: linear-gradient(135deg, #EF4444, #DC2626);
        color: white;
        padding: 6px 16px;
        border-radius: 9999px;
        font-weight: 700;
        font-size: 0.85rem;
        display: inline-flex;
        align-items: center;
        gap: 6px;
        box-shadow: 0 4px 14px rgba(239, 68, 68, 0.4);
        animation: pulse-glow 1.5s infinite;
    }
    @keyframes pulse-glow {
        0%, 100% { transform: scale(1); opacity: 1; }
        50% { transform: scale(1.02); opacity: 0.85; }
    }

    /* Live Pulse Indicator */
    .live-dot {
        height: 10px;
        width: 10px;
        background-color: #10B981;
        border-radius: 50%;
        display: inline-block;
        box-shadow: 0 0 8px #10B981;
        animation: pulse-dot 1.5s infinite;
    }
    @keyframes pulse-dot {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db():
    """Singleton Database connection manager."""
    return DatabaseManager()


def main():
    db = get_db()

    # --------------------------------------------------------------------------
    # Sidebar Controls & Live Testing Tools
    # --------------------------------------------------------------------------
    st.sidebar.markdown("### ⚡ **System Telemetry**")
    
    server_summaries = db.get_summary()
    server_list = ["ALL"]
    if not server_summaries.empty and "server_id" in server_summaries.columns:
        server_list.extend(server_summaries["server_id"].tolist())

    selected_server = st.sidebar.selectbox("Filter Target Server", server_list, index=0)
    history_limit = st.sidebar.slider("Events Buffer Window", min_value=30, max_value=300, value=DASHBOARD_HISTORY_LIMIT, step=10)
    auto_refresh = st.sidebar.toggle("Auto-Refresh Dashboard", value=True)
    refresh_rate = st.sidebar.slider("Refresh Interval (seconds)", min_value=1, max_value=10, value=DASHBOARD_REFRESH_INTERVAL, step=1)

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🧪 **Interactive Anomaly Test**")
    anomaly_choice = st.sidebar.selectbox(
        "Inject Anomaly Signature",
        ["cpu_spike", "memory_exhaustion", "network_flood", "latency_spike", "cascading_failure"],
        help="Simulate a real-time server failure to test Isolation Forest anomaly detection live."
    )
    if st.sidebar.button("🚨 Inject Anomaly Now", use_container_width=True):
        try:
            from producer.generator import ServerMetricGenerator
            from processor.stream_processor import MetricStreamProcessor
            target = selected_server if selected_server != "ALL" else "server-01"
            gen = ServerMetricGenerator(server_ids=[target], anomaly_probability=1.0)
            event = gen.generate_metric(server_id=target, force_anomaly=True, force_anomaly_type=anomaly_choice)
            proc = MetricStreamProcessor(db_manager=db)
            proc.process_event(event)
            st.sidebar.success(f"Injected {anomaly_choice} on {target}!")
            time.sleep(0.5)
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Injection error: {e}")

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "<div style='font-size: 0.8rem; color: #64748B; line-height: 1.5;'>"
        "<b>Architecture Layers:</b><br/>"
        "1. 🔹 Synthetic Telemetry Generator<br/>"
        "2. 🔹 Apache Kafka Broker (<code>server-metrics</code>)<br/>"
        "3. 🔹 30-Event Sliding-Window Processor<br/>"
        "4. 🔹 Scikit-Learn Isolation Forest ML<br/>"
        "5. 🔹 PostgreSQL Persistent Storage<br/>"
        "6. 🔹 Streamlit Auto-Refreshing Dashboard"
        "</div>",
        unsafe_allow_html=True
    )
    st.sidebar.caption(f"Last Polled: {datetime.now().strftime('%H:%M:%S')} (IST / Local Time)")

    # --------------------------------------------------------------------------
    # Main Header & System Health Status Badge
    # --------------------------------------------------------------------------
    col_title, col_status = st.columns([4, 1.2])
    with col_title:
        st.markdown('<div class="main-header">⚡ Real-Time Server Health & Anomaly Detection</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="sub-header"><span class="live-dot"></span> Live Telemetry Stream | Timezone: <b>India Standard Time (IST / UTC+05:30)</b></div>',
            unsafe_allow_html=True
        )

    # Fetch fresh metrics from database
    df_metrics = db.get_recent_metrics(limit=history_limit, server_id=selected_server)
    kpis = db.get_aggregate_kpis()

    with col_status:
        status = kpis["health_status"]
        if status == "CRITICAL":
            st.markdown('<div style="text-align: right; margin-top: 8px;"><span class="badge-critical">🚨 CRITICAL ALERT</span></div>', unsafe_allow_html=True)
        elif status == "WARNING":
            st.markdown('<div style="text-align: right; margin-top: 8px;"><span class="badge-warning">⚠️ WARNING ELEVATED</span></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="text-align: right; margin-top: 8px;"><span class="badge-healthy">✅ ALL SYSTEMS NOMINAL</span></div>', unsafe_allow_html=True)

    if df_metrics.empty:
        st.info("👋 Waiting for incoming server telemetry... Run the simulation script to stream events into the system:")
        st.code("""
# Option A: Local Python Streaming Simulation
python simulate_stream.py --interval 1.0

# Option B: Run Kafka Producer & Consumer
python -m processor.stream_processor
python -m producer.producer
        """, language="bash")
        if auto_refresh:
            time.sleep(refresh_rate)
            st.rerun()
        return

    # Sort chronological for time-series charts
    df_sorted = df_metrics.sort_values("timestamp", ascending=True).copy()
    latest_event = df_metrics.iloc[0]

    # --------------------------------------------------------------------------
    # Row 1: Modern Styled KPI Metric Cards
    # --------------------------------------------------------------------------
    c1, c2, c3, c4, c5, c6 = st.columns(6)

    cpu_val = float(latest_event["cpu_usage"])
    cpu_roll = float(latest_event.get("cpu_rolling_avg", cpu_val))
    cpu_delta = cpu_val - cpu_roll
    cpu_class = "delta-pos" if cpu_delta > 5.0 else ("delta-neg" if cpu_delta < -5.0 else "delta-neutral")

    mem_val = float(latest_event["memory_usage"])
    mem_roll = float(latest_event.get("memory_rolling_avg", mem_val))
    mem_delta = mem_val - mem_roll
    mem_class = "delta-pos" if mem_delta > 5.0 else ("delta-neg" if mem_delta < -5.0 else "delta-neutral")

    with c1:
        st.markdown(f"""
        <div class="metric-card-container">
            <div class="metric-card-title">⚡ CPU Load</div>
            <div class="metric-card-value">{cpu_val:.1f}%</div>
            <div class="metric-card-delta {cpu_class}">{cpu_delta:+.1f}% vs 30-roll</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="metric-card-container">
            <div class="metric-card-title">🧠 Memory</div>
            <div class="metric-card-value">{mem_val:.1f}%</div>
            <div class="metric-card-delta {mem_class}">{mem_delta:+.1f}% vs 30-roll</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        net_val = float(latest_event["network_traffic"])
        st.markdown(f"""
        <div class="metric-card-container">
            <div class="metric-card-title">🌐 Network</div>
            <div class="metric-card-value">{net_val:.1f} <span style="font-size: 1rem; font-weight: 500;">MB/s</span></div>
            <div class="metric-card-delta delta-neutral">Throughput</div>
        </div>
        """, unsafe_allow_html=True)

    with c4:
        resp_val = float(latest_event["response_time"])
        resp_class = "delta-pos" if resp_val > 400.0 else "delta-neg"
        st.markdown(f"""
        <div class="metric-card-container">
            <div class="metric-card-title">⏱️ Latency</div>
            <div class="metric-card-value">{resp_val:.0f} <span style="font-size: 1rem; font-weight: 500;">ms</span></div>
            <div class="metric-card-delta {resp_class}">Response Time</div>
        </div>
        """, unsafe_allow_html=True)

    with c5:
        total_anom = int(kpis["total_anomalies"])
        anom_class = "delta-pos" if total_anom > 0 else "delta-neg"
        st.markdown(f"""
        <div class="metric-card-container">
            <div class="metric-card-title">🚨 Anomalies</div>
            <div class="metric-card-value" style="color: {'#EF4444' if total_anom > 0 else '#10B981'};">{total_anom}</div>
            <div class="metric-card-delta {anom_class}">{'Flagged by ML' if total_anom > 0 else 'All Clean'}</div>
        </div>
        """, unsafe_allow_html=True)

    with c6:
        total_events = int(kpis["total_events"])
        st.markdown(f"""
        <div class="metric-card-container">
            <div class="metric-card-title">📊 Processed</div>
            <div class="metric-card-value">{total_events}</div>
            <div class="metric-card-delta delta-neutral">Total Stream Events</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 18px;'></div>", unsafe_allow_html=True)

    # --------------------------------------------------------------------------
    # Row 2: Live Charts with Rolling Averages and Red Anomaly Markers
    # --------------------------------------------------------------------------
    chart_col1, chart_col2 = st.columns(2)

    # 1. CPU Utilization Graph
    with chart_col1:
        fig_cpu = go.Figure()
        fig_cpu.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["cpu_usage"],
            mode="lines",
            name="CPU Live (%)",
            line=dict(color="#0284C7", width=2.2),
            hovertemplate="<b>%{x|%H:%M:%S} IST</b><br>Live CPU: %{y:.1f}%<extra></extra>"
        ))
        fig_cpu.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["cpu_rolling_avg"],
            mode="lines",
            name="30-Event Rolling Avg",
            line=dict(color="#F59E0B", width=2.2, dash="dash"),
            hovertemplate="<b>%{x|%H:%M:%S} IST</b><br>Rolling Avg: %{y:.1f}%<extra></extra>"
        ))
        anomalies_cpu = df_sorted[df_sorted["is_anomaly"] == True]
        if not anomalies_cpu.empty:
            fig_cpu.add_trace(go.Scatter(
                x=anomalies_cpu["timestamp"],
                y=anomalies_cpu["cpu_usage"],
                mode="markers",
                name="🚨 Anomaly Spike",
                marker=dict(color="#DC2626", size=11, symbol="x", line=dict(width=2.5, color="#FFFFFF")),
                hovertemplate="<b>ANOMALY DETECTED</b><br>%{x|%H:%M:%S} IST<br>CPU: %{y:.1f}%<extra></extra>"
            ))

        fig_cpu.update_layout(
            title=dict(text="<b>CPU Utilization (%) & 30-Event Rolling Trend</b>", font=dict(size=14, color="#1E293B")),
            xaxis=dict(title="Time (IST)", showgrid=True, gridcolor="#F1F5F9"),
            yaxis=dict(title="CPU (%)", range=[0, 105], showgrid=True, gridcolor="#F1F5F9"),
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=40, b=30),
            height=280,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10))
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
            line=dict(color="#9333EA", width=2.2),
            hovertemplate="<b>%{x|%H:%M:%S} IST</b><br>Live Memory: %{y:.1f}%<extra></extra>"
        ))
        fig_mem.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["memory_rolling_avg"],
            mode="lines",
            name="30-Event Rolling Avg",
            line=dict(color="#F59E0B", width=2.2, dash="dash"),
            hovertemplate="<b>%{x|%H:%M:%S} IST</b><br>Rolling Avg: %{y:.1f}%<extra></extra>"
        ))
        anomalies_mem = df_sorted[df_sorted["is_anomaly"] == True]
        if not anomalies_mem.empty:
            fig_mem.add_trace(go.Scatter(
                x=anomalies_mem["timestamp"],
                y=anomalies_mem["memory_usage"],
                mode="markers",
                name="🚨 Anomaly Spike",
                marker=dict(color="#DC2626", size=11, symbol="x", line=dict(width=2.5, color="#FFFFFF")),
                hovertemplate="<b>ANOMALY DETECTED</b><br>%{x|%H:%M:%S} IST<br>Memory: %{y:.1f}%<extra></extra>"
            ))

        fig_mem.update_layout(
            title=dict(text="<b>Memory Utilization (%) & 30-Event Rolling Trend</b>", font=dict(size=14, color="#1E293B")),
            xaxis=dict(title="Time (IST)", showgrid=True, gridcolor="#F1F5F9"),
            yaxis=dict(title="Memory (%)", range=[0, 105], showgrid=True, gridcolor="#F1F5F9"),
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=40, b=30),
            height=280,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10))
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
            line=dict(color="#2563EB", width=2.0),
            fillcolor="rgba(37, 99, 235, 0.12)",
            hovertemplate="<b>%{x|%H:%M:%S} IST</b><br>Network: %{y:.1f} MB/s<extra></extra>"
        ))
        anomalies_net = df_sorted[df_sorted["is_anomaly"] == True]
        if not anomalies_net.empty:
            fig_net.add_trace(go.Scatter(
                x=anomalies_net["timestamp"],
                y=anomalies_net["network_traffic"],
                mode="markers",
                name="🚨 Traffic Flood",
                marker=dict(color="#DC2626", size=10, symbol="x"),
                hovertemplate="<b>TRAFFIC FLOOD SPIKE</b><br>%{x|%H:%M:%S} IST<br>Throughput: %{y:.1f} MB/s<extra></extra>"
            ))

        fig_net.update_layout(
            title=dict(text="<b>Network Bandwidth Throughput (MB/s)</b>", font=dict(size=14, color="#1E293B")),
            xaxis=dict(title="Time (IST)", showgrid=True, gridcolor="#F1F5F9"),
            yaxis=dict(title="Throughput (MB/s)", showgrid=True, gridcolor="#F1F5F9"),
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=40, b=30),
            height=260,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10))
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
            line=dict(color="#059669", width=2.0),
            hovertemplate="<b>%{x|%H:%M:%S} IST</b><br>Latency: %{y:.1f} ms<extra></extra>"
        ))
        fig_resp.add_trace(go.Scatter(
            x=df_sorted["timestamp"],
            y=df_sorted["response_time_rolling_avg"],
            mode="lines",
            name="Rolling Latency Avg",
            line=dict(color="#F59E0B", width=2.0, dash="dash"),
            hovertemplate="<b>%{x|%H:%M:%S} IST</b><br>Rolling Latency: %{y:.1f} ms<extra></extra>"
        ))
        anomalies_resp = df_sorted[df_sorted["is_anomaly"] == True]
        if not anomalies_resp.empty:
            fig_resp.add_trace(go.Scatter(
                x=anomalies_resp["timestamp"],
                y=anomalies_resp["response_time"],
                mode="markers",
                name="🚨 Latency Spike",
                marker=dict(color="#DC2626", size=10, symbol="x"),
                hovertemplate="<b>LATENCY SPIKE</b><br>%{x|%H:%M:%S} IST<br>Latency: %{y:.1f} ms<extra></extra>"
            ))

        fig_resp.update_layout(
            title=dict(text="<b>Server Latency & Turnaround Response Time (ms)</b>", font=dict(size=14, color="#1E293B")),
            xaxis=dict(title="Time (IST)", showgrid=True, gridcolor="#F1F5F9"),
            yaxis=dict(title="Response Time (ms)", showgrid=True, gridcolor="#F1F5F9"),
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=20, t=40, b=30),
            height=260,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10))
        )
        st.plotly_chart(fig_resp, use_container_width=True)

    # --------------------------------------------------------------------------
    # Row 4: Isolation Forest Anomaly Confidence Score Timeline
    # --------------------------------------------------------------------------
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    fig_score = go.Figure()
    
    # Scatter trace with color-coded points
    marker_colors = ["#DC2626" if a else "#10B981" for a in df_sorted["is_anomaly"]]
    fig_score.add_trace(go.Scatter(
        x=df_sorted["timestamp"],
        y=df_sorted["anomaly_score"],
        mode="lines+markers",
        name="ML Anomaly Score",
        line=dict(color="#94A3B8", width=1.5),
        marker=dict(size=6, color=marker_colors),
        hovertemplate="<b>%{x|%H:%M:%S} IST</b><br>Score: %{y:.4f}<extra></extra>"
    ))
    # Threshold indicator line at 0.50
    fig_score.add_hline(
        y=0.50,
        line_dash="dash",
        line_color="#DC2626",
        line_width=1.8,
        annotation_text="Anomaly Decision Boundary (Score = 0.50)",
        annotation_position="bottom right",
        annotation_font=dict(color="#DC2626", size=10, weight="bold")
    )
    fig_score.update_layout(
        title=dict(text="<b>🤖 Isolation Forest Anomaly Confidence Score Timeline</b>", font=dict(size=14, color="#1E293B")),
        xaxis=dict(title="Time (IST)", showgrid=True, gridcolor="#F1F5F9"),
        yaxis=dict(title="Confidence [0.0 - 1.0]", range=[0, 1.05], showgrid=True, gridcolor="#F1F5F9"),
        template="plotly_white",
        height=240,
        margin=dict(l=40, r=20, t=35, b=25)
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
        available_cols = [c for c in display_cols if c in df_metrics.columns]
        display_df = df_metrics[available_cols].copy()
        
        if "timestamp" in display_df.columns:
            display_df["timestamp"] = pd.to_datetime(display_df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            display_df = display_df.rename(columns={"timestamp": "timestamp (IST)"})

        # Color code anomalous rows
        def style_anomalies(row):
            if row.get("is_anomaly", False):
                return ["background-color: rgba(239, 68, 68, 0.16); font-weight: 600; color: #B91C1C;"] * len(row)
            return [""] * len(row)

        fmt_metrics = {}
        for c, f in [
            ("cpu_usage", "{:.2f}%"),
            ("memory_usage", "{:.2f}%"),
            ("network_traffic", "{:.2f} MB/s"),
            ("response_time", "{:.1f} ms"),
            ("cpu_rolling_avg", "{:.2f}%"),
            ("anomaly_score", "{:.4f}")
        ]:
            if c in display_df.columns:
                fmt_metrics[c] = f

        st.dataframe(
            display_df.style.apply(style_anomalies, axis=1).format(fmt_metrics),
            use_container_width=True,
            height=340
        )

    with tab2:
        df_anomalies = db.get_recent_anomalies(limit=30)
        if df_anomalies.empty:
            st.success("✅ No anomalies detected in recent telemetry buffer. All server systems nominal.")
        else:
            st.warning(f"⚠️ {len(df_anomalies)} anomalous operational events detected:")
            for _, anom in df_anomalies.iterrows():
                ts_str = pd.to_datetime(anom["timestamp"]).strftime("%Y-%m-%d %H:%M:%S IST")
                with st.expander(
                    f"🔴 [{ts_str}] Host: {anom['server_id']} — Anomaly Confidence: {float(anom['anomaly_score']):.3f}"
                ):
                    c_a, c_b, c_c, c_d = st.columns(4)
                    c_a.metric("CPU Surged To", f"{float(anom['cpu_usage']):.1f}%")
                    c_b.metric("Memory Surged To", f"{float(anom['memory_usage']):.1f}%")
                    c_c.metric("Network Throughput", f"{float(anom['network_traffic']):.1f} MB/s")
                    c_d.metric("Response Latency", f"{float(anom['response_time']):.1f} ms")

    with tab3:
        # BUG FIX: Safely style server_summaries using style.format without calling non-existent DataFrame.format
        if not server_summaries.empty:
            display_summary = server_summaries.copy()
            
            # Map health status into clean visual tags
            if "health_status" in display_summary.columns:
                display_summary["health_status"] = display_summary["health_status"].apply(
                    lambda v: "🔴 CRITICAL" if v == "CRITICAL" else ("🟡 WARNING" if v == "WARNING" else "🟢 HEALTHY")
                )

            # Ensure last_updated is formatted in IST
            if "last_updated" in display_summary.columns:
                try:
                    ts_upd = pd.to_datetime(display_summary["last_updated"])
                    local_tz = datetime.now().astimezone().tzinfo
                    if ts_upd.dt.tz is not None:
                        ts_upd = ts_upd.dt.tz_convert(local_tz).dt.tz_localize(None)
                    display_summary["last_updated (IST)"] = ts_upd.dt.strftime("%Y-%m-%d %H:%M:%S")
                    display_summary = display_summary.drop(columns=["last_updated"])
                except Exception:
                    pass

            fmt_dict = {}
            for col, fmt in [
                ("current_cpu", "{:.2f}%"),
                ("current_memory", "{:.2f}%"),
                ("current_network", "{:.2f} MB/s"),
                ("current_response_time", "{:.1f} ms"),
                ("peak_cpu", "{:.2f}%"),
                ("peak_memory", "{:.2f}%")
            ]:
                if col in display_summary.columns:
                    fmt_dict[col] = fmt

            st.dataframe(
                display_summary.style.format(fmt_dict),
                use_container_width=True
            )
        else:
            st.info("No server summary records available yet. Telemetry streaming will populate this view automatically.")

    # --------------------------------------------------------------------------
    # Auto-Refresh Execution Loop
    # --------------------------------------------------------------------------
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()


if __name__ == "__main__":
    main()
