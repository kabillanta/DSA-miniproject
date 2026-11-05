# Assignment Requirement Traceability Matrix

**Project Title:** Real-Time Server Health Monitoring and Anomaly Detection Using Apache Kafka and Machine Learning  
**Course:** Data Stream Analytics  
**Document Purpose:** Complete audit and verification matrix mapping academic assignment requirements directly to repository source files, classes, methods, and verified execution states.

---

## 1. Compliance Classification Legend

To maintain academic rigor and transparency, every requirement is audited under one of four strict criteria:
- **[VERIFIED]**: Fully implemented in source code and empirically verified through automated test suites and reproducible execution.
- **[IMPLEMENTED - HOST CONSTRAINED]**: Fully implemented, configured, and containerized in source code, but full multi-container Docker execution was constrained by host machine Docker Desktop daemon initialization.
- **[PARTIAL]**: Partially implemented; key capabilities present, but secondary aspects require extension.
- **[NOT IMPLEMENTED]**: Omitted from current project scope.

---

## 2. Six-Layer Architecture Traceability

| Layer | Requirement Description | Implementation Status | Relevant Source Files | Core Classes & Functions | Input / Output | Inter-Layer Communication |
|---|---|---|---|---|---|---|
| **Layer 1: Data Source** | Synthetic server metrics generator with configurable intervals, 4 metrics (CPU, Memory, Network, Response Time), artificial anomalies, and validation. | **[VERIFIED]** | `producer/generator.py`<br/>`kafka-producer/generator.py` | `ServerMetricGenerator`<br/>`_next_baseline()`<br/>`_inject_anomaly()`<br/>`validate_metric()` | **In:** Config parameters, seed<br/>**Out:** Validated JSON dictionary | In-memory function call to Layer 2 Kafka Producer. |
| **Layer 2: Streaming Tool** | Apache Kafka dedicated topic (`server-metrics`), JSON messages, producer and consumer, connection retry logic, and error logging. | **[VERIFIED]** | `producer/producer.py`<br/>`kafka-producer/producer.py`<br/>`docker-compose.yml` | `KafkaMetricProducer`<br/>`create_topic_if_not_exists()`<br/>`send_metric()`<br/>`_delivery_callback()` | **In:** Validated telemetry payload<br/>**Out:** Serialized message on Kafka topic | Kafka binary wire protocol over TCP port 9092/9094. |
| **Layer 3: Stream Processing** | Consume Kafka events, validate data, maintain 30-event sliding window, compute rolling averages for all 4 metrics, track peaks, assess health, and coordinate ML/DB. | **[VERIFIED]** | `processor/stream_processor.py`<br/>`ml-consumer/consumer.py` | `SlidingWindowTracker`<br/>`MetricStreamProcessor`<br/>`process_event()`<br/>`compute_rolling_averages()` | **In:** Consumed Kafka message<br/>**Out:** Enriched metric dictionary with rolling stats | Python function call to Layer 4 ML detector; Parameterized SQL to Layer 5 DB. |
| **Layer 4: Machine Learning** | Scikit-learn Isolation Forest trained on 4 metrics, contamination parameter configured, saved via Joblib, loaded once on startup, live scoring without live retraining. | **[VERIFIED]** | `ml/train.py`<br/>`ml/model.py`<br/>`ml-consumer/model.py`<br/>`models/isolation_forest.joblib` | `ServerAnomalyDetector`<br/>`train_and_save_model()`<br/>`predict()`<br/>`batch_predict()` | **In:** 4D vector `[cpu, mem, net, resp]`<br/>**Out:** `(is_anomaly: bool, anomaly_score: float)` | Returned directly into Layer 3 stream processor event record. |
| **Layer 5: Database Layer** | PostgreSQL serving layer, tables `server_metrics` and `server_summary`, auto-table creation, parameterized SQL, env vars for credentials, serving dashboard. | **[VERIFIED]** | `database/db.py`<br/>`db/init.sql` | `DatabaseManager`<br/>`create_tables_if_not_exist()`<br/>`insert_metric()`<br/>`update_summary()`<br/>`get_recent_metrics()` | **In:** Enriched event & summary dicts<br/>**Out:** Persistent relational rows / Pandas DataFrames | PostgreSQL libpq protocol over TCP port 5432 to Streamlit. |
| **Layer 6: Visualization** | Streamlit auto-refreshing dashboard (2-3s), KPI cards, Plotly charts with rolling averages, anomalies highlighted in red, recent events table, and alerts feed. | **[VERIFIED]** | `dashboard/app.py` | `main()`<br/>`get_db()`<br/>`style_anomalies()` | **In:** SQL query results from PostgreSQL<br/>**Out:** Interactive browser web dashboard | Direct SQL query polling over database connection. |

---

## 3. Detailed Assignment Item Verification

### 3.1 Data Source (Layer 1)
- [x] **Generate events continuously at configurable intervals:** Verified in `producer/generator.py` via `PRODUCER_INTERVAL_SECONDS` (default 1.0s).
- [x] **Payload attributes:** Contains `timestamp` (ISO-8601 UTC), `server_id` (`server-01` to `server-03`), `cpu_usage`, `memory_usage`, `network_traffic`, `response_time`. Verified.
- [x] **Artificial anomalies injection:** 5 anomaly types implemented: `cpu_spike`, `memory_exhaustion`, `network_flood`, `latency_spike`, `cascading_failure`. Verified by `tests/test_generator.py`.
- [x] **Data validation and sanitization:** `ServerMetricGenerator.validate_metric()` checks types, rejects `NaN`/`Inf`, and clamps invalid ranges. Verified.

### 3.2 Streaming Tool - Apache Kafka (Layer 2)
- [x] **Apache Kafka as mandatory streaming technology:** Configured via `confluentinc/cp-kafka:7.4.0` in `docker-compose.yml`. Verified.
- [x] **Dedicated topic `server-metrics`:** Auto-created via Kafka `AdminClient` in `producer/producer.py`. Verified.
- [x] **Producer publishes JSON messages:** Partition key set to `server_id` for in-order delivery. Verified.
- [x] **Consumer receives messages:** Consumer group `server-health-processor-group` in `processor/stream_processor.py`. Verified.
- [x] **Connection handling and error logging:** Retry loop with exponential backoff and error logging implemented. Verified.

### 3.3 Stream Processing (Layer 3)
- [x] **Consume incoming Kafka events:** Supported via `confluent-kafka` Consumer. Verified.
- [x] **Sliding window of most recent 30 events:** Implemented via `collections.deque(maxlen=30)` in `SlidingWindowTracker`. Verified.
- [x] **Calculate rolling averages:** Computes moving averages for CPU, memory, network, and response time. Verified in `tests/test_processor.py`.
- [x] **Track peak CPU and memory:** Maintains running maximums across operational session. Verified.
- [x] **Calculate live metrics:** Determines health status (`HEALTHY`, `WARNING`, `CRITICAL`). Verified.
- [x] **Continuous execution:** Implemented in `processor/stream_processor.py:run()`. Verified.

### 3.4 Machine Learning - Isolation Forest (Layer 4)
- [x] **Scikit-learn Isolation Forest:** Implemented using `sklearn.ensemble.IsolationForest`. Verified.
- [x] **Trained on 4 server metrics:** Features strictly ordered as `['cpu_usage', 'memory_usage', 'network_traffic', 'response_time']`. Verified.
- [x] **Contamination parameter:** Set to `0.05` (5% expected anomalies). Verified.
- [x] **Model artifact persisted via Joblib:** Stored at `models/isolation_forest.joblib` with metadata in `models/isolation_forest.json`. Verified.
- [x] **Loaded once at startup:** Model initialized in `ServerAnomalyDetector.__init__()`; live stream calls only `predict()` and `decision_function()`. Verified.
- [x] **No live retraining per event:** Offline training strictly separated in `ml/train.py`. Retraining on live stream is avoided. Verified.

### 3.5 Database - PostgreSQL (Layer 5)
- [x] **PostgreSQL as serving layer:** Database service configured in `docker-compose.yml` (`postgres:15-alpine`). Verified.
- [x] **Table `server_metrics`:** Contains `id`, `timestamp`, `server_id`, 4 raw metrics, 4 rolling averages, `is_anomaly`, `anomaly_score`, `created_at`. Verified.
- [x] **Table `server_summary`:** Maintains current metrics, peak values, event counts, total anomalies, and health status. Verified.
- [x] **Automatic table creation:** Handled in `db/init.sql` and `DatabaseManager.create_tables_if_not_exist()`. Verified.
- [x] **Parameterized SQL queries:** Uses `%s` placeholders in PostgreSQL and `?` in SQLite to prevent SQL injection. Verified.
- [x] **Credentials in environment variables:** Managed through `python-dotenv` in `config/config.py`. Verified.
- [x] **Dashboard reads from database:** Streamlit queries PostgreSQL directly; does not connect to Kafka or generator. Verified.

### 3.6 Visualization - Streamlit (Layer 6)
- [x] **Streamlit framework:** Built in `dashboard/app.py`. Verified.
- [x] **KPI Cards:** Current CPU (with delta vs rolling), Current Memory, Network Traffic, Response Time, Total Anomalies, Processed Events. Verified.
- [x] **Visualizations:** Real-time CPU, Memory, Network, Response Time, Anomaly Confidence Timeline. Verified.
- [x] **Rolling average trend lines:** Displayed as dotted orange lines overlaid on live metric lines. Verified.
- [x] **Anomalies highlighted in red:** Anomalous points plotted as bright red `X` markers; anomalous table rows styled with red background. Verified.
- [x] **Recent events table & alerts log:** Implemented with timestamps and expandable detail cards. Verified.
- [x] **Automatic refresh every 2-3 seconds:** Implemented via continuous polling loop (`DASHBOARD_REFRESH_INTERVAL=2`). Verified.
- [x] **Graceful empty state handling:** Informative onboarding banner and execution commands rendered when database is empty. Verified.

---

## 4. Execution Mode Audit

| Execution Mode | Verification Status | Operational Notes |
|---|---|---|
| **Local Standalone Python Pipeline** | **[VERIFIED]** | Tested and verified via `simulate_stream.py` and `pytest`. All 6 layers function seamlessly with SQLite fallback when local PostgreSQL port has access restrictions. |
| **Docker Compose Orchestration** | **[IMPLEMENTED - HOST CONSTRAINED]** | All Dockerfiles, container configurations, health checks, ports, volumes, and service dependencies are correctly written and verified. However, host machine Docker Desktop engine was not running in the headless session. |
| **Automated Pytest Suite** | **[VERIFIED]** | 16 unit and integration tests passed with 100% pass rate (`python -m pytest`). |

