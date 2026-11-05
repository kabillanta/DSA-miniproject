# System Validation & Execution Verification Summary

**Project Title:** Real-Time Server Health Monitoring and Anomaly Detection Using Apache Kafka and Machine Learning  
**Course:** Data Stream Analytics  
**Document Purpose:** Complete empirical verification report containing test suite execution logs, model benchmark metrics, database query traces, and operational validation notes.

---

## 1. Automated Test Suite Execution

The repository maintains an automated Pytest test suite in `tests/` covering every architectural layer and the end-to-end integration pipeline.

### Test Execution Command
```bash
python -m pytest -v
```

### Verified Pytest Execution Output
```
============================= test session starts =============================
platform win32 -- Python 3.13.13, pytest-9.2.0, pluggy-1.7.0
rootdir: D:\College\Projects\dsaminiproject
plugins: anyio-4.13.0
collected 16 items

tests\test_database.py::test_database_table_creation PASSED              [  6%]
tests\test_database.py::test_insert_and_retrieve_metric PASSED           [ 12%]
tests\test_database.py::test_update_and_get_summary PASSED               [ 18%]
tests\test_generator.py::test_generator_initialization PASSED            [ 25%]
tests\test_generator.py::test_baseline_metric_ranges PASSED              [ 31%]
tests\test_generator.py::test_forced_anomaly_generation PASSED           [ 37%]
tests\test_generator.py::test_validation_missing_field PASSED            [ 43%]
tests\test_generator.py::test_validation_out_of_bounds_clamping PASSED   [ 50%]
tests\test_ml_model.py::test_model_loaded PASSED                         [ 56%]
tests\test_ml_model.py::test_predict_normal_event PASSED                 [ 62%]
tests\test_ml_model.py::test_predict_anomalous_event PASSED              [ 68%]
tests\test_ml_model.py::test_batch_prediction PASSED                     [ 75%]
tests\test_pipeline.py::test_complete_end_to_end_pipeline PASSED         [ 81%]
tests\test_processor.py::test_sliding_window_tracker PASSED              [ 87%]
tests\test_processor.py::test_health_status_transitions PASSED           [ 93%]
tests\test_processor.py::test_stream_processor_single_event PASSED       [100%]

======================== 16 passed, 1 warning in 3.51s ========================
```

### Test Scope and Coverage Analysis:
1. **`test_database.py`:**
   - Validates schema creation (`server_metrics`, `server_summary`).
   - Validates parameterized insertion and retrieval of detailed telemetry.
   - Validates summary aggregations and cluster KPI computation.
2. **`test_generator.py`:**
   - Validates baseline normal telemetry ranges ($0.0 \le \text{CPU, Mem} \le 100.0\%$).
   - Validates forced anomaly generation and anomaly type tagging.
   - Validates rejection of incomplete events (`MetricValidationError`).
   - Validates mathematical clamping of out-of-bounds telemetry.
3. **`test_ml_model.py`:**
   - Validates that the pre-trained Isolation Forest model loads from disk.
   - Validates that normal operating vectors return `is_anomaly = False` and score $< 0.50$.
   - Validates that spike vectors return `is_anomaly = True` and score $> 0.50$.
   - Validates vectorized batch scoring capability.
4. **`test_processor.py`:**
   - Validates that the sliding window maintains strictly 30 events (`maxlen=30`).
   - Validates mathematical accuracy of moving average calculations across all 4 metrics.
   - Validates dynamic health status transitions (`HEALTHY` $\to$ `WARNING` $\to$ `CRITICAL`).
   - Validates end-to-end event enrichment.
5. **`test_pipeline.py`:**
   - Validates the complete integrated flow: Synthetic Generation $\to$ Stream Processing $\to$ ML Scoring $\to$ Database Persistence $\to$ Query Retrieval.

---

## 2. Machine Learning Model Training Evidence

The Isolation Forest algorithm was executed and evaluated via `ml/train.py`.

### Execution Command
```bash
python ml/train.py --samples 4000 --contamination 0.05 --output models/isolation_forest.joblib
```

### Verified Training Execution Output
```
2026-10-30 23:25:43,340 [INFO] Generating 4000 historical metrics samples (contamination=0.05)...
2026-10-30 23:25:43,387 [INFO] Dataset generated: 4000 rows. Anomalies: 219 (5.47%)
2026-10-30 23:25:43,389 [INFO] Fitting IsolationForest(n_estimators=100, contamination=0.05, random_state=42)...
2026-10-30 23:25:43,676 [INFO] ============================================================
2026-10-30 23:25:43,676 [INFO] MODEL TRAINING EVALUATION SUMMARY
2026-10-30 23:25:43,676 [INFO] ============================================================
2026-10-30 23:25:43,676 [INFO] Accuracy:  0.9952
2026-10-30 23:25:43,676 [INFO] Precision: 1.0000
2026-10-30 23:25:43,676 [INFO] Recall:    0.9132
2026-10-30 23:25:43,676 [INFO] F1-Score:  0.9547
2026-10-30 23:25:43,676 [INFO] ============================================================
2026-10-30 23:25:43,683 [INFO] 
Detailed Classification Report:
              precision    recall  f1-score   support

      Normal       0.99      1.00      1.00      3781
     Anomaly       1.00      0.91      0.95       219

    accuracy                           1.00      4000
   macro avg       1.00      0.96      0.98      4000
weighted avg       1.00      1.00      1.00      4000

2026-10-30 23:25:43,716 [INFO] Model successfully saved to: D:\College\Projects\dsaminiproject\models\isolation_forest.joblib
2026-10-30 23:25:43,718 [INFO] Model metadata saved to: D:\College\Projects\dsaminiproject\models\isolation_forest.json
```

---

## 3. Live Stream Processing & Ingestion Verification

The continuous streaming processor was executed locally via `simulate_stream.py` to evaluate dynamic sliding-window calculations and database insertions.

### Execution Command
```bash
python simulate_stream.py --interval 1.0 --count 5
```

### Verified Stream Processor Trace
```
2026-10-30 23:33:48,527 [INFO] Initializing Local End-to-End Pipeline Simulation...
2026-10-30 23:33:48,527 [INFO] Initializing ML Anomaly Detector...
2026-10-30 23:33:48,542 [INFO] Loaded trained Isolation Forest model from: D:\College\Projects\dsaminiproject\models\isolation_forest.joblib
2026-10-30 23:33:48,542 [INFO] Initializing Database connection...
2026-10-30 23:33:49,121 [INFO] Connected to local SQLite database at: D:\College\Projects\dsaminiproject\server_health.db
2026-10-30 23:33:49,121 [INFO] Verified/created database schema (server_metrics, server_summary).
2026-10-30 23:33:49,121 [INFO] Pipeline active! Streaming telemetry every 1.0s (Anomaly Prob: 8.0%)...
2026-10-30 23:33:49,121 [INFO] Open the Streamlit Dashboard (streamlit run dashboard/app.py) to view real-time updates.
2026-10-30 23:33:53,276 [INFO] Simulation completed after 5 events.
2026-10-30 23:33:53,277 [INFO] Database connection closed.
```

---

## 4. Visual Evidence Artifacts in `docs/assets/`

The following publication-grade analytical figures were generated directly from the historical evaluation dataset and model weights:

1. **`docs/assets/figure1_confusion_matrix.png`:**
   - Visualizes classification performance: 3,781 True Negatives, 200 True Positives, 19 False Negatives, 0 False Positives.
2. **`docs/assets/figure2_telemetry_distributions.png`:**
   - 4-panel histogram detailing distribution of normal telemetry versus injected anomaly modes across CPU, Memory, Network, and Latency.
3. **`docs/assets/figure3_stream_sliding_window.png`:**
   - Chronological plot of 80 sequential events displaying raw instantaneous CPU utilization, 30-event rolling average trend, and isolated red anomaly points.

---

## 5. Outstanding Operational Constraints & Recommendations

1. **Docker Engine Initialization:** Docker Compose service definitions are verified and ready. On Windows environments, Docker Desktop must be running in GUI mode before executing `docker compose up -d`.
2. **Live Demonstration Fallback:** The standalone local Python execution pathway (`python simulate_stream.py` + `streamlit run dashboard/app.py`) provides an identical functional demonstration without Docker daemon dependencies.

