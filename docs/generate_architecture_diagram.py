"""
Generates a publication-grade architecture flowchart diagram for the academic report.
Saves high-resolution PNG: docs/assets/architecture_flowchart.png
"""
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path

OUTPUT_PATH = Path("docs/assets/architecture_flowchart.png")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# Set high DPI and aesthetic figure dimensions
fig, ax = plt.subplots(figsize=(15, 9.5), dpi=300)
ax.set_facecolor("#F8FAFC")
fig.patch.set_facecolor("#FFFFFF")

# Remove standard axes
ax.set_xlim(0, 15)
ax.set_ylim(0, 9.5)
ax.axis("off")

# Title Block
plt.text(7.5, 9.05, "Six-Layer Real-Time Streaming Analytics Architecture", 
         fontsize=18, fontweight="bold", ha="center", va="center", color="#0F172A")
plt.text(7.5, 8.7, "Decoupled Telemetry Pipeline with Apache Kafka, Sliding-Window Analytics, Isolation Forest ML & Streamlit", 
         fontsize=10.5, ha="center", va="center", color="#475569")

# Define layer properties: (x, y, w, h, bg_color, border_color, layer_num, title, subtitle, bullets, badge)
layers = [
    {
        "x": 0.6, "y": 4.6, "w": 4.0, "h": 3.6,
        "bg": "#EFF6FF", "border": "#3B82F6",
        "num": "LAYER 1", "title": "Data Source (Telemetry Generator)",
        "tech": "Python 3.13 | Synthetic Engine",
        "items": [
            "• Generates 4D metrics continuously (1.5s interval)",
            "• CPU (%), Memory (%), Network (MB/s), Latency (ms)",
            "• Diurnal baseline curves + Gaussian noise",
            "• Injects 5 anomaly patterns (spikes, floods, leaks)",
            "• Strict schema validation & bounds clamping"
        ],
        "badge": "Synthetic Telemetry"
    },
    {
        "x": 5.5, "y": 4.6, "w": 4.0, "h": 3.6,
        "bg": "#F5F3FF", "border": "#8B5CF6",
        "num": "LAYER 2", "title": "Streaming Tool (Apache Kafka)",
        "tech": "Kafka 7.4.0 Broker | Topic: server-metrics",
        "items": [
            "• High-throughput decoupled distributed pub/sub",
            "• Keyed partitioning (key = server_id)",
            "• Guarantees causal chronological ordering per server",
            "• Consumer group coordination with offset tracking",
            "• Exponential backoff connection resilience"
        ],
        "badge": "Distributed Buffer"
    },
    {
        "x": 10.4, "y": 4.6, "w": 4.0, "h": 3.6,
        "bg": "#ECFDF5", "border": "#10B981",
        "num": "LAYER 3", "title": "Stream Processor (Window Analytics)",
        "tech": "Python Stateful Engine | collections.deque",
        "items": [
            "• In-memory 30-event FIFO sliding window per host",
            "• Real-time moving average computation",
            "• Lifetime peak metric tracking (Peak CPU/Mem)",
            "• Dynamic health state evaluation (CRITICAL / OK)",
            "• Orchestrates ML scoring & atomic persistence"
        ],
        "badge": "Sliding Window"
    },
    {
        "x": 0.6, "y": 0.6, "w": 4.0, "h": 3.4,
        "bg": "#FEF2F2", "border": "#EF4444",
        "num": "LAYER 4", "title": "Machine Learning (Isolation Forest)",
        "tech": "Scikit-Learn 1.6+ | Joblib Model Artifact",
        "items": [
            "• Unsupervised ensemble tree partitioning (100 trees)",
            "• Pre-trained offline on 4,000 baseline samples",
            "• Sub-millisecond live scoring (< 0.4 ms/event)",
            "• Normalizes anomaly confidence scores [0.0 - 1.0]",
            "• Zero live retraining: loads once at startup"
        ],
        "badge": "Real-Time ML"
    },
    {
        "x": 5.5, "y": 0.6, "w": 4.0, "h": 3.4,
        "bg": "#FFFBEB", "border": "#F59E0B",
        "num": "LAYER 5", "title": "Serving Layer (Database)",
        "tech": "PostgreSQL 15 / SQLite Relational DB",
        "items": [
            "• Table: server_metrics (raw + rolling metrics + ML)",
            "• Table: server_summary (live server state rollup)",
            "• B-tree indexes for fast reverse-chronological reads",
            "• Parameterized SQL queries preventing injection",
            "• Resilient SQLite fallback when host constrained"
        ],
        "badge": "Persistent Serving"
    },
    {
        "x": 10.4, "y": 0.6, "w": 4.0, "h": 3.4,
        "bg": "#F0FDF4", "border": "#059669",
        "num": "LAYER 6", "title": "Visualization Dashboard",
        "tech": "Streamlit Enterprise UI | Plotly Express",
        "items": [
            "• Auto-refreshing reactive UI (2-second interval)",
            "• 6 Glassmorphic KPI cards with live delta states",
            "• Plotly dual-trace charts (Live vs 30-Event Avg)",
            "• Red markers for ML-flagged operational anomalies",
            "• Full IST (UTC+05:30) synchronization across all views"
        ],
        "badge": "Live UI Dashboard"
    }
]

# Draw Cards
for l in layers:
    # Outer Card
    card = patches.FancyBboxPatch(
        (l["x"], l["y"]), l["w"], l["h"],
        boxstyle="round,pad=0.15,rounding_size=0.18",
        facecolor=l["bg"], edgecolor=l["border"], linewidth=1.8,
        zorder=2
    )
    ax.add_patch(card)

    # Header Badge
    badge_w, badge_h = 1.3, 0.32
    badge_rect = patches.FancyBboxPatch(
        (l["x"] + 0.15, l["y"] + l["h"] - 0.42), badge_w, badge_h,
        boxstyle="round,pad=0.04,rounding_size=0.08",
        facecolor=l["border"], edgecolor="none", zorder=3
    )
    ax.add_patch(badge_rect)
    plt.text(l["x"] + 0.15 + badge_w / 2, l["y"] + l["h"] - 0.26, l["num"],
             fontsize=8.5, fontweight="bold", color="#FFFFFF", ha="center", va="center", zorder=4)

    # Title & Subtitle
    plt.text(l["x"] + 0.15, l["y"] + l["h"] - 0.68, l["title"],
             fontsize=10.2, fontweight="bold", color="#0F172A", ha="left", va="center", zorder=3)
    plt.text(l["x"] + 0.15, l["y"] + l["h"] - 0.98, l["tech"],
             fontsize=8.2, fontstyle="italic", fontweight="bold", color=l["border"], ha="left", va="center", zorder=3)

    # Separator Line
    ax.plot([l["x"] + 0.15, l["x"] + l["w"] - 0.15], [l["y"] + l["h"] - 1.15, l["y"] + l["h"] - 1.15],
            color="#CBD5E1", linewidth=0.8, zorder=3)

    # Bullet Points
    for idx, item in enumerate(l["items"]):
        plt.text(l["x"] + 0.18, l["y"] + l["h"] - 1.45 - (idx * 0.36), item,
                 fontsize=8.0, color="#334155", ha="left", va="center", zorder=3)


# Connecting Data-Flow Arrows
def draw_arrow(start, end, label=""):
    ax.annotate(
        "", xy=end, xytext=start,
        arrowprops=dict(
            arrowstyle="-|>", color="#1E3A8A", lw=2.2,
            mutation_scale=18, shrinkA=3, shrinkB=3
        ),
        zorder=5
    )
    if label:
        mid_x = (start[0] + end[0]) / 2
        mid_y = (start[1] + end[1]) / 2
        bbox_props = dict(boxstyle="round,pad=0.25", fc="#FFFFFF", ec="#CBD5E1", lw=0.8, alpha=0.95)
        plt.text(mid_x, mid_y, label, fontsize=7.8, fontweight="bold", color="#1E3A8A",
                 ha="center", va="center", bbox=bbox_props, zorder=6)

# Flow 1: Layer 1 -> Layer 2
draw_arrow((4.75, 6.4), (5.35, 6.4), "JSON Telemetry\n(4D Metrics)")

# Flow 2: Layer 2 -> Layer 3
draw_arrow((9.65, 6.4), (10.25, 6.4), "Partitioned Stream\n(server-metrics)")

# Flow 3: Layer 3 -> Layer 4 (Bi-directional scoring)
draw_arrow((10.4, 4.9), (4.75, 2.5), "Feature Vector\n(Live Inference)")
draw_arrow((4.75, 2.0), (10.4, 4.6), "Score & Flag\n(is_anomaly)")

# Flow 4: Layer 3 -> Layer 5
draw_arrow((11.5, 4.45), (9.65, 2.3), "Enriched Records\n& Summary Upserts")

# Flow 5: Layer 5 -> Layer 6
draw_arrow((9.65, 2.3), (10.25, 2.3), "SQL Polling\n(Every 2s / IST)")

# Pipeline Protocol Flow Note at Bottom
plt.text(7.5, 0.18, "End-to-End Pipeline Latency: < 15ms  |  Isolation Forest Scoring: < 0.40ms  |  Dashboard Polling: 2.0s  |  Timezone: India Standard Time (IST, UTC+05:30)",
         fontsize=8.5, fontstyle="italic", ha="center", va="center", color="#64748B")

plt.tight_layout()
plt.savefig(OUTPUT_PATH, dpi=300, bbox_inches="tight")
print(f"Architecture diagram successfully generated at: {OUTPUT_PATH}")
