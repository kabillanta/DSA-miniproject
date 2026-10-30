# Real-Time Server Health Monitoring and Anomaly Detection Using Apache Kafka and Machine Learning

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Kafka](https://img.shields.io/badge/streaming-Apache%20Kafka%207.4-black.svg)](https://kafka.apache.org/)
[![Machine Learning](https://img.shields.io/badge/ML-Isolation%20Forest%20(99.5%25)-green.svg)](https://scikit-learn.org/)
[![PostgreSQL](https://img.shields.io/badge/database-PostgreSQL%2015-blue.svg)](https://www.postgresql.org/)
[![Streamlit](https://img.shields.io/badge/dashboard-Streamlit%201.29+-red.svg)](https://streamlit.io/)
[![Tests](https://img.shields.io/badge/tests-16%20passed%20(100%25)-brightgreen.svg)]()

A complete, production-grade academic mini-project for real-time distributed server health monitoring, sliding-window stream analytics, unsupervised machine-learning anomaly detection, persistent relational storage, and auto-refreshing interactive visualization.

---

## 📐 System Architecture

The pipeline strictly implements a decoupled **Six-Layer Architecture**:

```
[ Layer 1: Data Source ]
         │ Synthetic Multi-Server Telemetry Generator (Normal Walk + Injected Anomalies)
         ▼
[ Layer 2: Streaming Tool ]
         │ Apache Kafka Producer -> Topic: server-metrics (Key = server_id)
         ▼
[ Layer 3: Stream Processing ]
         │ Python Stream Processor (30-Event Sliding Window, Rolling Avgs, Peak Tracking)
         ▼
[ Layer 4: Machine Learning ]
         │ Scikit-Learn Isolation Forest (Sub-millisecond Inference via Joblib Artifact)
         ▼
[ Layer 5: Persistent Serving Layer ]
         │ PostgreSQL Database (server_metrics & server_summary with B-Tree Indexes)
         ▼
[ Layer 6: Visualization ]
         │ Streamlit Interactive Dashboard (Auto-Refreshes every 2s, Red Anomaly Highlights)
```

---

## 🚀 Key Monitored Metrics

1. **CPU Utilization (%)**: Processor usage with 30-event rolling average trend.
2. **Memory Utilization (%)**: RAM usage with moving average tracking.
3. **Network Traffic (MB/s)**: Ingress/egress bandwidth and DDoS flood detection.
4. **Server Response Time (ms)**: Turnaround latency and database lock/timeout detection.

---

## 📁 Project Structure

```
dsaminiproject/
├── config/                         # Centralized configuration
│   ├── __init__.py
│   └── config.py                   # Environment variable loader
├── producer/                       # Layer 1 & 2: Telemetry Generation & Kafka Producer
│   ├── __init__.py
│   ├── generator.py                # Synthetic telemetry engine with anomaly injection
│   └── producer.py                 # Apache Kafka producer with retry & AdminClient
├── processor/                      # Layer 3: Python Stream Processor
│   ├── __init__.py
│   └── stream_processor.py         # 30-event sliding window, rolling stats & ML invocation
├── ml/                             # Layer 4: Machine Learning
│   ├── __init__.py
│   ├── model.py                    # Inference detector loading Joblib artifact once
│   └── train.py                    # Historical training script with evaluation metrics
├── database/                       # Layer 5: Persistent Serving Layer
│   ├── __init__.py
│   └── db.py                       # PostgreSQL client with auto-fallback to SQLite
├── dashboard/                      # Layer 6: Web Dashboard
│   ├── __init__.py
│   ├── app.py                      # Streamlit interactive auto-refreshing dashboard
│   ├── Dockerfile
│   └── requirements.txt
├── db/
│   └── init.sql                    # PostgreSQL schema definition (metrics & summary)
├── models/                         # Serialized ML artifacts
│   ├── isolation_forest.joblib     # Pre-trained Isolation Forest model
│   └── isolation_forest.json       # Model evaluation & metadata
├── tests/                          # Automated Pytest suite
│   ├── test_generator.py           # Layer 1 unit tests
│   ├── test_ml_model.py            # Layer 4 ML unit tests
│   ├── test_processor.py           # Layer 3 stream processor tests
│   ├── test_database.py            # Layer 5 database tests
│   └── test_pipeline.py            # End-to-end integration test
├── docs/                           # Academic Deliverables
│   ├── PROJECT_REPORT.md           # 15-section comprehensive academic project report
│   └── DEMO_GUIDE.md               # Step-by-step examiner evaluation & demo guide
├── docker-compose.yml              # Complete 6-service Docker Compose orchestration
├── simulate_stream.py              # Local pipeline simulator for rapid testing
├── requirements.txt                # Unified Python dependencies
├── .env.example                    # Sample environment variables
└── README.md                       # Main documentation
```

---

## 🛠️ Technology Stack

| Architectural Layer | Technology | Primary Role |
|---|---|---|
| **Layer 1: Data Source** | Python, NumPy | Stochastic telemetry generator with diurnal load modeling |
| **Layer 2: Streaming Tool** | Apache Kafka 7.4.0 | Distributed message buffer (`server-metrics` topic) |
| **Layer 3: Stream Processing** | Python, Collections | 30-event sliding window, rolling averages, peak tracking |
| **Layer 4: Machine Learning** | Scikit-Learn Isolation Forest | Unsupervised multidimensional anomaly detection |
| **Layer 5: Database** | PostgreSQL 15 | Relational time-series tables (`server_metrics`, `server_summary`) |
| **Layer 6: Visualization** | Streamlit & Plotly | Real-time web UI auto-refreshing every 2 seconds |

---

## ⚡ Quick Start Guide

### Option 1: Standalone Python Execution (Quickest for Local Demo)

1. **Clone repository & install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Train the Isolation Forest model (generates `models/isolation_forest.joblib`):**
   ```bash
   python ml/train.py
   ```

3. **Start the Streamlit dashboard:**
   ```bash
   streamlit run dashboard/app.py
   ```
   Open your browser at `http://localhost:8501`.

4. **Start the local stream simulation (in a separate terminal):**
   ```bash
   python simulate_stream.py --interval 1.0
   ```
   Watch the dashboard automatically plot arriving telemetry and flag anomalies in red!

---

### Option 2: Docker Compose Orchestration

Start all 6 microservices with a single command:
```bash
docker compose up -d
```

Verify service status:
```bash
docker compose ps
```

Access the dashboard at `http://localhost:8501`.

To stop services:
```bash
docker compose down -v
```

---

## 🧪 Testing & Validation

Execute the automated test suite covering all six layers:
```bash
python -m pytest -v
```

**Results:**
- ✅ 16 unit and integration tests passing (`100% pass rate`)
- ✅ Verified metric validation, sliding window rolling averages, peak tracking, ML classification, database upserts, and end-to-end telemetry pipeline.

---

## 📊 Machine Learning Benchmark

Trained on 4,000 synthetic historical server telemetry records ($contamination = 0.05$):

| Metric | Measured Value |
|---|---|
| **Accuracy** | **99.52%** |
| **Precision** | **100.00%** (Zero false alarms) |
| **Recall** | **91.32%** |
| **F1-Score** | **0.9547** |
| **Inference Latency** | **< 0.4 ms** per event |

---

## 📖 Academic Documentation

- Comprehensive Academic Project Report (15 sections, Mermaid diagram, LaTeX formulas, empirical results): [PROJECT_REPORT.md](docs/PROJECT_REPORT.md)
- Examiner Demonstration Guide (Step-by-step viva presentation script): [DEMO_GUIDE.md](docs/DEMO_GUIDE.md)

---

## 📄 License
Academic Course Project - Apache 2.0 License.
