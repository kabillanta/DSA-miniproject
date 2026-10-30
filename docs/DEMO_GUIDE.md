# Demonstration & Evaluation Guide

**Project Title:** Real-Time Server Health Monitoring and Anomaly Detection Using Apache Kafka and Machine Learning  
**Target Audience:** Academic Reviewers, Evaluators, and Examiners  

---

## 1. Quick Startup Options

You can demonstrate the system using either **Docker Compose** (full multi-container stack) or **Local Python Execution** (standalone lightweight mode).

---

### Option A: Local Python Standalone Execution (Fastest & Simplest for Live Demos)

This option requires zero external Docker daemon dependencies and runs immediately in 3 terminals:

#### Step 1: Install Dependencies & Prepare Model
In your primary terminal:
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Train the Isolation Forest Model (takes ~1 second)
python ml/train.py
```
*Expected Output:*
```
Accuracy:  0.9952 | Precision: 1.0000 | Recall: 0.9132 | F1-Score: 0.9547
Model successfully saved to: models/isolation_forest.joblib
```

#### Step 2: Start the Streamlit Dashboard
```bash
streamlit run dashboard/app.py
```
*Action:* Open your web browser to `http://localhost:8501`. You will see the telemetry dashboard.

#### Step 3: Start Stream Telemetry
In a second terminal:
```bash
python simulate_stream.py --interval 1.0
```
*Action:* Watch the Streamlit dashboard auto-refresh every 2 seconds as new events stream into the database!

---

### Option B: Docker Compose Full Stack Execution

```bash
# 1. Start all 6 containerized services in background
docker compose up -d

# 2. Verify all services are healthy and running
docker compose ps

# 3. Access the Streamlit Dashboard
# Open browser to: http://localhost:8501
```

To view live service logs:
```bash
docker compose logs -f ml-consumer
docker compose logs -f kafka-producer
```

To shut down containers:
```bash
docker compose down -v
```

---

## 2. Step-by-Step Demonstration Walkthrough

Follow this step-by-step checklist during your project evaluation or viva defense:

### Step 2.1: Demonstrating Continuous Ingestion & Auto-Refresh
1. Open the dashboard at `http://localhost:8501`.
2. Notice the top **Status Badge** is `🟢 ALL HEALTHY`.
3. Check the **KPI Cards**:
   - `Current CPU`: Displays current load and delta vs. 30-event rolling average (`+X.X% vs roll`).
   - `Current Memory`: Displays current RAM load and delta vs. rolling average.
   - `Total Processed`: Increments every 2-3 seconds without refreshing the browser!
4. **Examiner Explanation:**
   > *"The dashboard polls PostgreSQL every 2 seconds. The stream processor updates the database upon receiving Kafka messages, ensuring zero manual browser reload is needed."*

---

### Step 2.2: Demonstrating 30-Event Rolling Averages
1. Look at the **CPU Utilization (%) & Rolling Trend** graph.
2. Point out the two lines:
   - **Solid Cyan Line:** Live, raw instantaneous metric.
   - **Dotted Orange Line:** 30-event sliding window moving average ($\overline{x}_{30}$).
3. **Examiner Explanation:**
   > *"Layer 3 maintains an in-memory double-ended queue (`deque(maxlen=30)`) per server. As raw metrics fluctuate, the rolling average provides a smoothed baseline to detect structural drift without noise sensitivity."*

---

### Step 2.3: Demonstrating Real-Time Anomaly Detection (Red Highlights)
You can trigger an immediate, high-impact anomaly on demand:

In a new terminal window, execute:
```bash
# Force an immediate CPU & Latency spike
python -m producer.producer --inject-anomaly --anomaly-type cpu_spike
```
Or via simulation:
```bash
python simulate_stream.py --anomaly-prob 0.50 --count 10
```

**What to Observe on the Dashboard:**
1. The **Status Badge** in the top right changes from `🟢 ALL HEALTHY` to `🔴 CRITICAL ALERT`.
2. On the CPU and Memory charts, a distinct **Bright Red Marker (X)** appears at the anomalous timestamp.
3. In the **Isolation Forest Anomaly Confidence Timeline**, the score jumps above the red dashed decision threshold ($0.50$).
4. The **Recent Server Events Table** highlights the anomalous row in red text and pink background.
5. In the **🚨 Anomaly Alerts Log** tab, an expander card details the exact spike metrics and severity.

**Examiner Explanation:**
> *"The Isolation Forest model evaluated the 4-dimensional vector `[CPU, Memory, Network, Latency]`. Because CPU surged to >95% while response time degraded, the path length in the isolation trees was short, yielding a normalized anomaly score of ~0.80 and triggering the alert."*

---

### Step 2.4: Demonstrating Producer Pause & Resume
1. In the terminal running the producer/simulator, press `Ctrl + C`.
2. **Observe:**
   - The producer prints `Flushing pending messages... Producer shut down cleanly.`
   - On the dashboard, graphs stop adding new points, and metrics remain stable at the last recorded state.
3. Re-run `python simulate_stream.py --interval 1.0`.
4. **Observe:**
   - Graph immediately resumes plotting newly arriving points in real time.

---

### Step 2.5: Demonstrating Multi-Server Filtering
1. In the dashboard sidebar, locate the **Filter by Server ID** dropdown.
2. Switch from `ALL` to `server-01`.
3. **Observe:** The graphs and tables instantly isolate data specifically for `server-01`.
4. Switch to the **Fleet Summary** tab to observe the cluster-wide state table showing current metrics, peak CPU, and total anomalies for every node.

---

## 3. Explaining the Six-Layer Architecture

When asked by examiners about the system architecture, use this concise explanation:

| Layer | Component | Core Function & Justification |
|---|---|---|
| **Layer 1: Data Source** | `producer/generator.py` | Generates continuous stochastic multi-server telemetry with Gaussian baseline fluctuations and injected anomaly signatures. |
| **Layer 2: Streaming Tool** | `producer/producer.py` | Apache Kafka broker buffering messages into topic `server-metrics` with partition key `server_id` for ordered delivery. |
| **Layer 3: Stream Processing** | `processor/stream_processor.py` | Sliding window of 30 events, computing moving averages, tracking lifetime peaks, and coordinating scoring. |
| **Layer 4: Machine Learning** | `ml/model.py` | Scikit-Learn Isolation Forest loaded once as a joblib artifact; classifies normal vs anomaly in $<0.4\text{ ms}$ without live retraining. |
| **Layer 5: Database** | `database/db.py` | PostgreSQL relational serving layer with indexed `server_metrics` and upserted `server_summary`. |
| **Layer 6: Visualization** | `dashboard/app.py` | Streamlit interactive UI auto-refreshing every 2 seconds with KPI cards, Plotly charts, and red anomaly highlights. |

---

## 4. Verification & Testing Checklist

To prove project functionality to evaluators, run the comprehensive automated test suite:

```bash
python -m pytest -v
```

*Expected Result:*
```
tests/test_database.py::test_database_table_creation PASSED              [  6%]
tests/test_database.py::test_insert_and_retrieve_metric PASSED           [ 12%]
tests/test_database.py::test_update_and_get_summary PASSED               [ 18%]
tests/test_generator.py::test_generator_initialization PASSED            [ 25%]
tests/test_generator.py::test_baseline_metric_ranges PASSED              [ 31%]
tests/test_generator.py::test_forced_anomaly_generation PASSED           [ 37%]
tests/test_generator.py::test_validation_missing_field PASSED            [ 43%]
tests/test_generator.py::test_validation_out_of_bounds_clamping PASSED   [ 50%]
tests/test_ml_model.py::test_model_loaded PASSED                         [ 56%]
tests/test_ml_model.py::test_predict_normal_event PASSED                 [ 62%]
tests/test_ml_model.py::test_predict_anomalous_event PASSED              [ 68%]
tests/test_ml_model.py::test_batch_prediction PASSED                     [ 75%]
tests/test_pipeline.py::test_complete_end_to_end_pipeline PASSED         [ 81%]
tests/test_processor.py::test_sliding_window_tracker PASSED              [ 87%]
tests/test_processor.py::test_health_status_transitions PASSED           [ 93%]
tests/test_processor.py::test_stream_processor_single_event PASSED       [100%]
======================== 16 passed in 3.5s ========================
```

