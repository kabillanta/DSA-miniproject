# Real-Time Server Health Monitoring and Anomaly Detection Using Apache Kafka and Machine Learning

**Academic Project Report**  
**Course:** Data Stream Analytics  
**Architecture:** Six-Layer Decoupled Real-Time Streaming Pipeline  
**Document Classification:** Academic Technical Report  

---

## Table of Contents
1. [Abstract](#1-abstract)
2. [Introduction](#2-introduction)
3. [Problem Statement and Objectives](#3-problem-statement-and-objectives)
4. [System Architecture](#4-system-architecture)
5. [Dataset Description](#5-dataset-description)
6. [Implementation of the Six Layers](#6-implementation-of-the-six-layers)
   - [6.1 Data Source](#61-data-source)
   - [6.2 Streaming Infrastructure](#62-streaming-infrastructure)
   - [6.3 Stream Processing](#63-stream-processing)
   - [6.4 Machine Learning Model](#64-machine-learning-model)
   - [6.5 Database Layer](#65-database-layer)
   - [6.6 Visualization Dashboard](#66-visualization-dashboard)
7. [Machine Learning Methodology](#7-machine-learning-methodology)
8. [Results and Observations](#8-results-and-observations)
9. [Conclusion](#9-conclusion)
10. [Limitations and Future Enhancements](#10-limitations-and-future-enhancements)
11. [References](#11-references)

---

## 1. Abstract

Modern cloud infrastructures and enterprise server clusters demand continuous operational visibility to safeguard against hardware degradation, runaway processes, distributed denial-of-service (DDoS) events, and memory saturation. Traditional periodic polling and static scalar thresholding fail to capture complex multidimensional correlations, resulting in delayed interventions and high false alarm rates. This report presents the design, implementation, and empirical validation of a real-time server health monitoring and anomaly detection system engineered in accordance with a six-layer decoupled streaming analytics architecture. The pipeline continuously synthesizes four-dimensional server telemetry—CPU utilization, memory allocation, network throughput, and turnaround response time—and streams events via Apache Kafka to a distributed buffer. A Python-based stream processor validates incoming payloads, maintains sliding windows of the latest 30 events, computes moving averages, tracks peak metrics, and evaluates cluster health status. An unsupervised Isolation Forest model, pre-trained offline on 4,000 baseline historical instances, scores each incoming vector in sub-millisecond latency without live retraining. Enriched events and aggregated summaries are persisted into PostgreSQL relational serving tables with B-tree indexes, serving an automatically refreshing Streamlit dashboard configured on a two-second polling interval. Empirical validation demonstrates an anomaly classification accuracy of 99.52%, 100.0% precision, 91.32% recall, and an F1-score of 0.9547 across multi-server environments.

---

## 2. Introduction

Real-time stream analytics represents a fundamental paradigm shift from traditional batch-oriented data processing architectures. In conventional batch processing architectures, telemetry records are accumulated in cold storage over extended horizons (e.g., hourly or daily batches) before executing extract, transform, load (ETL) routines and analytical aggregations. While suitable for retrospective reporting, batch paradigms introduce high latency that renders them incapable of mitigating operational infrastructure failures as they emerge.

Conversely, stream processing treats data as an unbounded, continuous sequence of discrete events evaluated with minimal end-to-end latency. In distributed server environments, where service degradation or resource saturation can trigger cascading failovers across dependent microservices within seconds, real-time telemetry processing is imperative. 

```
Batch Processing Paradigm (High Latency, Retrospective):
  Events Accumulate ──> Disk / Cold Storage ──> Periodic Batch Job ──> Delayed Incident Report

Stream Processing Paradigm (Sub-Second Latency, Proactive):
  Continuous Stream ──> Message Buffer (Kafka) ──> Live Sliding Window & ML ──> Instant Alerting & Visuals
```

Continuous anomaly detection across server telemetry involves tracking the simultaneous interplay of multiple operational indicators. For example, an isolated CPU surge accompanied by normal network ingress and fast response latency may indicate an anticipated batch calculation, whereas an identical CPU surge coupled with zero network activity or surging response latency often signals an unhandled lock contention, infinite loop, or resource leak. By leveraging Apache Kafka for decoupled message ingestion, a stateful Python stream processor for sliding-window temporal aggregation, Scikit-Learn's Isolation Forest for unsupervised spatial partitioning, PostgreSQL for persistent serving, and Streamlit for dynamic visualization, this project demonstrates a robust, resilient end-to-end streaming architecture.

---

## 3. Problem Statement and Objectives

### 3.1 Problem Statement
Enterprise cloud infrastructures face significant operational vulnerabilities when relying on legacy monitoring paradigms:
1. **Detection Latency:** Static polling intervals (e.g., cron checks every 5 to 15 minutes) leave substantial blind spots during which critical services can become completely unresponsive.
2. **Alert Fatigue and False Alarms:** Threshold-based alerting rules (such as `CPU > 80%`) lack awareness of operational context and correlation across dimensions, producing frequent false alarms during benign traffic surges and failing to identify subtle degradation patterns.
3. **Decoupling and Scalability Bottlenecks:** Tightly coupled monitoring agents that push telemetry directly into serving databases create high connection concurrency and write lock contention during cluster traffic bursts.

### 3.2 Project Objectives
To resolve these engineering limitations, the project delivers the following measurable objectives:
1. **Autonomous Telemetry Synthesis (Layer 1):** Construct a continuous synthetic generator reproducing realistic server baseline distributions (diurnal harmonic load, stochastic noise) and injecting controllable anomaly signatures (CPU spikes, memory saturation, DDoS floods, database lock latency, cascading failures).
2. **Fault-Tolerant Distributed Ingestion (Layer 2):** Deploy Apache Kafka as a decoupled streaming tool with topic partitioning keyed by `server_id` to guarantee strictly ordered delivery per host.
3. **Sliding-Window Stateful Stream Processing (Layer 3):** Implement an in-memory sliding window maintaining the 30 most recent events per server to derive moving averages, track lifetime peak metrics, and infer dynamic server health classifications.
4. **Offline-Trained Machine Learning Inference (Layer 4):** Train an Isolation Forest algorithm offline on multi-dimensional telemetry, persist the model artifact via Joblib, and perform sub-millisecond live scoring during stream consumption without live retraining overhead.
5. **Relational Serving Layer (Layer 5):** Design a normalized PostgreSQL schema featuring detailed event time-series (`server_metrics`) and upserted fleet summaries (`server_summary`) backed by B-tree indexing.
6. **Auto-Refreshing Observability Dashboard (Layer 6):** Deliver a Streamlit web visualization interface that autonomously re-queries PostgreSQL every 2 to 3 seconds, displaying real-time KPI metrics, rolling trend curves, and highlighting anomalous points in red.

---

## 4. System Architecture

The implemented system adheres strictly to the six-layer decoupled streaming analytics architecture. Each layer maintains a defined separation of concerns, communicating exclusively through documented network protocols and database interfaces.

### 4.1 Architectural Diagram
```mermaid
flowchart TD
    subgraph L1["Layer 1: Data Source"]
        GEN["ServerMetricGenerator<br/>(producer/generator.py)<br/>- Diurnal Walk + Gaussian Noise<br/>- Anomaly Injections"]
    end

    subgraph L2["Layer 2: Streaming Infrastructure"]
        KP["Kafka Producer<br/>(producer/producer.py)<br/>Key = server_id"]
        KB[("Kafka Broker<br/>Topic: server-metrics<br/>Partitioned Buffer")]
    end

    subgraph L3["Layer 3: Stream Processing"]
        KC["Kafka Consumer Group"]
        SW["Sliding Window Buffer<br/>(collections.deque, maxlen=30)"]
        RA["Rolling Analytics Engine<br/>(Averages, Peaks, Health State)"]
    end

    subgraph L4["Layer 4: Machine Learning"]
        IF["Isolation Forest Artifact<br/>(models/isolation_forest.joblib)"]
        INF["Inference Engine<br/>(predict -> boolean, score -> float)"]
    end

    subgraph L5["Layer 5: Database / Serving Layer"]
        PG[("PostgreSQL Database<br/>(server_health_db)")]
        T1["server_metrics<br/>(Telemetry + Rolling Stats + Anomaly Scores)"]
        T2["server_summary<br/>(Current KPIs, Peaks, Health Status)"]
    end

    subgraph L6["Layer 6: Visualization Dashboard"]
        ST["Streamlit Application<br/>(dashboard/app.py)<br/>Auto-Refresh: 2s"]
        KPI["Live KPI Metric Cards"]
        PLT["Plotly Charts with Red Anomaly Highlights"]
        ALT["Recent Anomalies Alert Log & Fleet View"]
    end

    GEN -->|Validated Event| KP
    KP -->|Produce JSON| KB
    KB -->|Consume Stream| KC
    KC --> SW
    SW --> RA
    RA -->|Feature Vector| IF
    IF --> INF
    INF -->|Enriched Event| PG
    PG --> T1
    PG --> T2
    T1 -.->|SQL Query Polling| ST
    T2 -.->|SQL Query Aggregates| ST
    ST --> KPI
    ST --> PLT
    ST --> ALT
```

### 4.2 Component Responsibilities and Inter-Layer Communication

| Layer | Component | Implementation File | Input | Output | Transport / Protocol |
|---|---|---|---|---|---|
| **Layer 1** | Data Source | `producer/generator.py` | State parameters, time step | Validated telemetry dictionary | Python internal memory / JSON |
| **Layer 2** | Streaming Tool | `producer/producer.py` | Validated telemetry payload | Serialized binary Kafka message | TCP (Kafka Protocol, port 9092/9094) |
| **Layer 3** | Stream Processor | `processor/stream_processor.py` | Kafka consumer record | Enriched dictionary with rolling averages | Python in-memory `deque` & computation |
| **Layer 4** | Machine Learning | `ml/model.py` | 4D Feature vector `[CPU, Mem, Net, Latency]` | `(is_anomaly: bool, anomaly_score: float)` | Scikit-learn C-optimized tree traversal |
| **Layer 5** | Database Layer | `database/db.py`, `db/init.sql` | Enriched telemetry dictionary | Persistent relational rows & summaries | PostgreSQL libpq / SQL over TCP port 5432 |
| **Layer 6** | Dashboard | `dashboard/app.py` | SQL `SELECT` queries from PostgreSQL | Interactive HTML/Canvas UI elements | Streamlit WebSockets / HTTP port 8501 |

---

## 5. Dataset Description

The system processes continuous four-dimensional time-series server telemetry. Because live production server telemetry from private enterprise infrastructure is restricted by confidentiality policies, an autonomous stochastic telemetry engine was developed in `producer/generator.py`.

### 5.1 Telemetry Attributes and Data Types

| Attribute | SQL Data Type | Python Type | Unit | Normal Operating Range | Injected Anomaly Range |
|---|---|---|---|---|---|
| `timestamp` | `TIMESTAMPTZ` | `str` (ISO-8601 UTC) | Date & Time | Continuous | Continuous |
| `server_id` | `VARCHAR(50)` | `str` | Nominal String | `server-01` to `server-03` | Identical |
| `cpu_usage` | `FLOAT` | `float` | Percentage (%) | 15.0% – 75.0% (mean ~35.0%) | 91.0% – 99.8% |
| `memory_usage` | `FLOAT` | `float` | Percentage (%) | 30.0% – 75.0% (mean ~55.0%) | 92.0% – 99.5% |
| `network_traffic`| `FLOAT` | `float` | Megabytes/sec (MB/s)| 5.0 – 65.0 MB/s (mean ~25.0) | 180.0 – 650.0 MB/s |
| `response_time` | `FLOAT` | `float` | Milliseconds (ms) | 40.0 – 250.0 ms (mean ~120.0) | 850.0 – 4500.0 ms |

### 5.2 Mathematical Formulation of Normal Telemetry
To simulate continuous server operation, each metric combines a baseline set-point, a periodic diurnal harmonic variation, and Gaussian stochastic noise:
$$\text{metric}_k(t) = \text{base}_k + A_k \sin(\omega t) + \epsilon_k, \quad \epsilon_k \sim \mathcal{N}(0, \sigma_k^2)$$
where $\omega = 0.05 \text{ rad/event}$, $A_{\text{cpu}} = 5.0\%$, $A_{\text{mem}} = 2.5\%$, $A_{\text{net}} = 7.5 \text{ MB/s}$, and $A_{\text{resp}} = 20.0 \text{ ms}$.

### 5.3 Simulated Anomaly Signatures
Artificial performance anomalies are injected at probability $p = 0.08$ across five distinct operational failure modes:
1. **CPU Spike:** Runaway loops or cryptographic routines driving CPU utilization between 91.0% and 99.8% with secondary response latency degradation.
2. **Memory Exhaustion:** Progressive heap allocation saturation exceeding 92.0%, inducing extended garbage collection latency.
3. **Network Flood:** Ingress traffic volumetric saturation between 180.0 and 650.0 MB/s, simulating network flooding and DDoS attacks.
4. **Latency Spike:** Downstream database lock contention or gateway timeouts driving response times to 850.0 – 3800.0 ms while CPU load remains moderate.
5. **Cascading Failure:** System-wide degradation where CPU, memory, network, and response time all breach critical thresholds simultaneously.

### 5.4 Representative Telemetry Event Payload
Below is a verified JSON telemetry event generated by `producer/generator.py`:
```json
{
  "timestamp": "2026-10-30T17:58:15.297238+00:00",
  "server_id": "server-02",
  "cpu_usage": 42.54,
  "memory_usage": 55.61,
  "network_traffic": 26.51,
  "response_time": 164.97,
  "_injected_anomaly": false,
  "_anomaly_type": null
}
```

### 5.5 Data Validation and Sanitization
The static method `ServerMetricGenerator.validate_metric(event)` enforces strict schema conformance:
- Asserts presence and non-null status of all mandatory keys.
- Validates that `timestamp` strictly follows ISO-8601 formatting.
- Verifies that metric values are numeric and rejects `NaN` and `Infinity`.
- Clamps out-of-bounds anomalies (e.g., negative percentages or utilization values exceeding 100.0%).

---

## 6. Implementation of the Six Layers

### 6.1 Data Source
The Data Source layer is implemented in `producer/generator.py` through the `ServerMetricGenerator` class. It manages persistent internal state machines for each server identifier in `SERVER_IDS` (`server-01`, `server-02`, `server-03`).

```python
# Code Snippet: State generation from producer/generator.py
def _next_baseline(self, server_id: str) -> Dict[str, float]:
    st = self._states[server_id]
    st["step"] += 1
    t = st["step"]
    diurnal = math.sin(t * 0.05) * 5.0

    cpu = st["cpu_base"] + diurnal + random.gauss(0, 3.5)
    mem = st["mem_base"] + (diurnal * 0.5) + random.gauss(0, 2.0)
    net = st["net_base"] + (diurnal * 1.5) + random.gauss(0, 4.0)
    resp = st["resp_base"] + (diurnal * 4.0) + random.gauss(0, 15.0)

    return {
        "cpu_usage": round(max(5.0, min(80.0, cpu)), 2),
        "memory_usage": round(max(20.0, min(80.0, mem)), 2),
        "network_traffic": round(max(1.0, min(95.0, net)), 2),
        "response_time": round(max(15.0, min(350.0, resp)), 2),
    }
```
The generator runs at a configurable frequency defined by `PRODUCER_INTERVAL_SECONDS` (default: 1.0 second per event).

### 6.2 Streaming Infrastructure
The streaming layer utilizes **Apache Kafka 7.4.0** orchestrated via Docker Compose (`docker-compose.yml`) alongside Zookeeper.
- **Topic Configuration:** Telemetry events are routed to topic `server-metrics`. The producer leverages the Kafka `AdminClient` API to programmatically discover and create the topic if absent (`replication_factor=1`, `num_partitions=1`).
- **Partitioning Strategy:** Events are published with `key=event["server_id"]`. This guarantees that all metrics for a given server are deterministically assigned to the same partition, preserving causal time ordering.
- **Fault-Tolerant Delivery:** The producer sets `acks='all'`, `retries=5`, and `retry.backoff.ms=500`. Delivery report callbacks handle asynchronous delivery acknowledgments and log socket exceptions.

### 6.3 Stream Processing
Implemented in `processor/stream_processor.py` (and containerized in `ml-consumer/consumer.py`), the `MetricStreamProcessor` maintains continuous consumption:
- **Sliding Window State:** Each server is allocated an independent `SlidingWindowTracker` backed by `collections.deque(maxlen=30)`. When a 31st event arrives, the oldest event is discarded in $O(1)$ time complexity.
- **Rolling Metric Aggregations:** For each incoming event, the processor computes moving averages across the current window contents:
  $$\text{cpu\_rolling\_avg} = \frac{1}{N} \sum_{i=1}^{N} \text{cpu\_usage}_i \quad (N \le 30)$$
- **Peak Utilization Tracking:** Maintains running maxima for `peak_cpu` and `peak_memory` over the entire operational session.
- **Dynamic Health Status:** Evaluates cluster node state:
  - `CRITICAL`: Anomaly classified by ML OR CPU $\ge 90\%$ OR Memory $\ge 90\%$ OR Latency $\ge 1000\text{ ms}$.
  - `WARNING`: CPU $\ge 75\%$ OR Memory $\ge 75\%$ OR Latency $\ge 400\text{ ms}$.
  - `HEALTHY`: Normal operating thresholds.

### 6.4 Machine Learning Model
The Machine Learning component is implemented in `ml/model.py` and `ml/train.py`.
- **Algorithm:** Scikit-Learn's `IsolationForest(n_estimators=100, contamination=0.05, max_samples='auto', random_state=42)`.
- **Training Workflow:** Trained offline in `ml/train.py` on 4,000 synthetic historical records. The trained ensemble is serialized to disk as a binary artifact using Joblib (`models/isolation_forest.joblib`).
- **Inference Workflow:** `ServerAnomalyDetector` loads the Joblib artifact **once** upon stream processor initialization. Live streaming events are scored in sub-millisecond time. Model retraining is strictly isolated from live message consumption loops.

### 6.5 Database Layer
The serving database is **PostgreSQL 15** (with automatic SQLite local fallback implemented in `database/db.py`):
- **Schema Initialization:** Executed automatically via `db/init.sql` upon container initialization or via `create_tables_if_not_exist()` in Python.
- **Table 1: `server_metrics`:** Records individual events, computed rolling metrics, anomaly flags, and normalized scores:
```sql
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
CREATE INDEX IF NOT EXISTS idx_server_metrics_timestamp ON server_metrics(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_server_metrics_server_id ON server_metrics(server_id);
CREATE INDEX IF NOT EXISTS idx_server_metrics_is_anomaly ON server_metrics(is_anomaly);
```
- **Table 2: `server_summary`:** Serves live aggregated KPIs updated via atomic SQL upserts (`INSERT ... ON CONFLICT (server_id) DO UPDATE`).

### 6.6 Visualization Dashboard
Built using **Streamlit** and **Plotly** in `dashboard/app.py`:
- **Auto-Refresh Mechanism:** Implemented via continuous polling every 2 seconds (`DASHBOARD_REFRESH_INTERVAL = 2`), directly querying PostgreSQL without cached data locks.
- **Top Metric Cards:** Render current CPU utilization (with rolling delta), memory utilization, network bandwidth, response time, total detected anomalies, and total processed events.
- **Visualizations:**
  - Real-time CPU and Memory utilization graphs featuring dual traces (instantaneous metric line and 30-event rolling average trend line).
  - Abnormal events highlighted with distinct **Red Markers (X)**.
  - Network throughput area chart and response latency curve with critical threshold lines.
  - Anomaly confidence timeline with a decision boundary reference line at $s = 0.50$.
- **Data Tables and Alerts:** Styled data tables highlighting anomalous rows in soft red background shading, accompanied by an expandable anomaly alerts log.

---

## 7. Machine Learning Methodology

### 7.1 Working Principle of Isolation Forest
Isolation Forest operates on the foundational observation that anomalous observations in multidimensional feature space are structurally isolated with fewer partitions than normal cluster points.

Given an $n$-instance training dataset, the algorithm constructs an ensemble of $T = 100$ isolation trees ($i$Trees). At each node of an $i$Tree, an attribute $q \in \{\text{cpu\_usage}, \text{memory\_usage}, \text{network\_traffic}, \text{response\_time}\}$ is randomly selected, and an isolation split value $p$ is chosen uniformly at random between $[\min(q), \max(q)]$.

Partitioning terminates recursively when an observation is isolated in a leaf node or when the maximum tree depth $h_{\max} = \lceil \log_2(n) \rceil$ is attained.

The path length $h(x)$ denotes the number of edges traversed from the tree root to the terminal leaf. The average path length $c(n)$ for a dataset of size $n$ is defined analytically as:
$$c(n) = 2 \left( \ln(n - 1) + 0.5772156649 \right) - \frac{2(n - 1)}{n}$$

The normalized anomaly score $s(x, n)$ across an ensemble of $T$ isolation trees is given by:
$$s(x, n) = 2^{-\frac{\mathbb{E}(h(x))}{c(n)}}$$

- When $\mathbb{E}(h(x)) \to 0$, $s \to 1$: The data point is isolated rapidly across the ensemble, confirming an **outlier anomaly**.
- When $\mathbb{E}(h(x)) \to c(n)$, $s \to 0.5$: The observation exhibits structural characteristics typical of normal baseline data.
- When $\mathbb{E}(h(x)) \to n - 1$, $s \to 0$: The observation is embedded deep within a dense cluster.

### 7.2 Training Configuration and Contamination
The model training script (`ml/train.py`) configures the Isolation Forest with the following verified hyperparameters:
- `n_estimators = 100`: Number of isolation trees in the ensemble.
- `contamination = 0.05`: Expected proportion of anomalies in the operational environment (5%).
- `max_samples = 'auto'`: Sample size per tree set to $\min(256, n)$.
- `random_state = 42`: Deterministic seed for reproducible tree partition splits.
- `n_jobs = -1`: Multi-threaded CPU parallelization across all available cores.

### 7.3 Live Prediction and Score Normalization
During live stream ingestion in `ml/model.py`, the incoming four-dimensional vector is scored using the pre-loaded model:
```python
raw_pred = self.model.predict(X)[0]               # Returns 1 (normal) or -1 (anomaly)
raw_score = float(self.model.decision_function(X)[0]) # Negative for anomalies, positive for normal

is_anomaly = bool(raw_pred == -1)
# Normalized anomaly confidence score mapped to [0.0, 1.0] via logistic transformation
normalized_score = float(1.0 / (1.0 + np.exp(raw_score * 8.0)))
```
This continuous transformation maps the decision boundary ($raw\_score = 0.0$) directly to an intuitive confidence threshold of $0.50$, enabling threshold visualization on the Streamlit dashboard.

---

## 8. Results and Observations

### 8.1 Empirical Evaluation Metrics
The Isolation Forest model was evaluated on 4,000 synthetic historical records with a contamination rate of 5.0%. Model performance was evaluated against ground-truth labels recorded by the generator:

| Metric | Measured Value | Operational Interpretation |
|---|---|---|
| **Overall Accuracy** | **99.52%** | High fidelity across normal baseline and anomalous space |
| **Precision** | **100.00%** | **Zero false positives**: Every alert triggered represented a genuine anomaly |
| **Recall** | **91.32%** | Successfully isolated >91% of subtle and multi-metric spikes |
| **F1-Score** | **0.9547** | Balanced harmonic mean validating high operational reliability |
| **Training Duration** | **0.38 seconds** | Fast training convergence across 100 trees |
| **Inference Latency** | **< 0.40 ms** | Sub-millisecond evaluation suitable for high-frequency streaming |

#### Classification Report Breakdown
```
              precision    recall  f1-score   support
      Normal       0.99      1.00      1.00      3781
     Anomaly       1.00      0.91      0.95       219
    accuracy                           1.00      4000
   macro avg       1.00      0.96      0.98      4000
weighted avg       1.00      1.00      1.00      4000
```

### 8.2 Confusion Matrix Analysis
Figure 1 depicts the empirical confusion matrix from the 4,000-sample historical evaluation run:

![Figure 1: Confusion Matrix](assets/figure1_confusion_matrix.png)

Out of 3,781 true normal records, all 3,781 were correctly classified as normal (zero false positives, $FP = 0$). Out of 219 injected anomalies, 200 were accurately detected ($TP = 200$), with only 19 border cases falling marginally below the isolation threshold ($FN = 19$).

### 8.3 Multi-Dimensional Telemetry Distributions
Figure 2 displays the probability density functions of the four monitored metrics across normal operating telemetry and injected anomalies:

![Figure 2: Telemetry Distributions](assets/figure2_telemetry_distributions.png)

The distribution plots confirm distinct multimodal separation:
- **CPU Utilization:** Normal baseline centers around $36.31\% \pm 12.4\%$, whereas anomalies cluster heavily between $91.0\%$ and $99.8\%$.
- **Memory Utilization:** Normal baseline maintains $45.84\% \pm 9.58\%$, with memory exhaustion events clustering above $93.0\%$.
- **Network Traffic:** Baseline ingress averages $34.40 \text{ MB/s}$, while DDoS flood events extend past $600.0 \text{ MB/s}$.
- **Response Latency:** Normal requests resolve in $173.79 \text{ ms} \pm 39.6 \text{ ms}$, whereas latency spikes extend up to $4,495.0 \text{ ms}$.

### 8.4 Live Sliding-Window Stream Tracking
Figure 3 demonstrates the live stream processing execution over an 80-event sequential run:

![Figure 3: Sliding Window Stream Processing](assets/figure3_stream_sliding_window.png)

The live CPU metric (solid blue line) fluctuates naturally with Gaussian variance. The 30-event rolling average (dashed orange line) provides a smooth representation of underlying load drift. Injected anomalous spikes trigger immediate identification (red X markers) without distorting the long-term moving baseline.

---

## 9. Conclusion

This project has demonstrated the successful design, implementation, and empirical verification of a complete, functional six-layer real-time server health monitoring and anomaly detection system. By decoupling data ingestion (Apache Kafka), stream analytics (sliding-window rollups), unsupervised machine learning (Isolation Forest), persistent storage (PostgreSQL), and live visualization (Streamlit), the architecture addresses the operational limitations of legacy batch polling.

The empirical results confirm that Isolation Forest is well-suited for multidimensional server telemetry, achieving an overall accuracy of 99.52% and 100.0% precision with zero false alarms. Sub-millisecond inference latency guarantees that operational anomalies are persisted and presented on the Streamlit dashboard within the two-second refresh window. The implementation satisfies all functional requirements set forth in the Data Stream Analytics course curriculum.

---

## 10. Limitations and Future Enhancements

### 10.1 System Limitations
1. **Synthetic Telemetry Baseline:** While stochastic diurnal modeling captures realistic server variance, real-world cloud environments exhibit non-stationary seasonality (e.g., promotional traffic events) that may require adaptive baseline recalibration.
2. **Single-Broker Kafka Topology:** The default deployment utilizes a single Kafka broker with one partition per topic. In large-scale production clusters generating tens of thousands of metrics per second, horizontal topic partitioning partitioned by hash of `server_id` across multi-node broker clusters is required.
3. **Dashboard Pull-Polling:** Streamlit retrieves updates via client-side polling queries on a 2-second timer rather than maintaining a bidirectional push WebSocket connection from the database.

### 10.2 Future Enhancements
1. **Distributed Stream Processing with Apache Flink:** Migrate the sliding window processor from Python `collections.deque` to an Apache Flink cluster to leverage distributed state snapshots, watermarking, and out-of-order event handling.
2. **Predictive Forecasting via Deep Learning:** Incorporate Temporal Fusion Transformers (TFT) or LSTM Autoencoders to forecast resource exhaustion 5 to 10 minutes prior to physical saturation.
3. **Automated Remediation Webhooks:** Introduce automated remediation hooks triggered by `CRITICAL` health states to trigger Kubernetes Horizontal Pod Autoscaling (HPA) or gracefully cycle degraded service containers.

---

## 11. References

1. Liu, F. T., Ting, K. M., and Zhou, Z. H., "Isolation Forest," *2008 Eighth IEEE International Conference on Data Mining*, Pisa, Italy, 2008, pp. 413-422, doi: 10.1109/ICDM.2008.17.
2. Kreps, J., Narkhede, N., and Rao, J., "Kafka: A Distributed Messaging System for Log Processing," *Proceedings of the ACM SIGMOD Workshop on Networking Meets Databases (NetDB)*, Athens, Greece, 2011, pp. 1-7.
3. Chandola, V., Banerjee, A., and Kumar, V., "Anomaly Detection: A Survey," *ACM Computing Surveys*, vol. 41, no. 3, pp. 1-58, Jul. 2009, doi: 10.1145/1541880.1541882.
4. Pedregosa, F. et al., "Scikit-learn: Machine Learning in Python," *Journal of Machine Learning Research*, vol. 12, pp. 2825-2830, Nov. 2011.
5. Stonebraker, M., Çetintemel, U., and Zdonik, S., "The 8 Requirements of Real-Time Stream Processing," *ACM SIGMOD Record*, vol. 34, no. 4, pp. 42-47, Dec. 2005, doi: 10.1145/1107499.1107504.
6. Carbone, P., Katsifodimos, A., Ewen, S., Markl, V., Haridi, S., and Tzoumas, K., "Apache Flink: Stream and Batch Processing in a Single Engine," *IEEE Data Engineering Bulletin*, vol. 38, no. 4, pp. 28-38, Dec. 2015.
7. PostgreSQL Global Development Group, *PostgreSQL 15.0 Documentation*, PostgreSQL Documentation Project, 2023. [Online]. Available: https://www.postgresql.org/docs/15/
8. Streamlit Inc., *Streamlit Documentation: Architecture and Reactive Execution*, 2024. [Online]. Available: https://docs.streamlit.io/
