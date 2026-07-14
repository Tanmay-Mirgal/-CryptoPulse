# 📈 CryptoPulse: Production-Grade MLOps Pipeline for Live Cryptocurrency Trading

CryptoPulse is a production-grade, end-to-end MLOps pipeline designed to ingest live cryptocurrency market data, compute real-time features, train machine learning models, generate trading signals, and monitor performance. 

This repository leverages **Astronomer Airflow** for orchestrating multi-stage DAGs, **Neon Serverless PostgreSQL** for database persistence, **MLflow & DagsHub** for experiment tracking and model registration, **DVC** for data versioning, and a live **Flask + Socket.IO + Chart.js** dashboard for real-time visualization, compact views, and step-by-step interactive walkthroughs.

---

## 🏗️ System Architecture

The pipeline consists of a continuous data feedback loop, split into 5 core DAGs and a live dashboard component:

```mermaid
graph TD
    %% Style definitions for premium look
    classDef source fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#78350f;
    classDef dag fill:#eff6ff,stroke:#2563eb,stroke-width:2px,color:#1e3a8a;
    classDef db fill:#ecfdf5,stroke:#059669,stroke-width:2px,color:#064e3b;
    classDef registry fill:#faf5ff,stroke:#7c3aed,stroke-width:2px,color:#581c87;
    classDef ui fill:#f5f3ff,stroke:#8b5cf6,stroke-width:2px,color:#4c1d95;
    classDef client fill:#f0fdf4,stroke:#16a34a,stroke-width:2px,color:#14532d;

    %% Data Source
    Binance["Binance API & WebSocket"]:::source

    %% Stage 1: Data Ingestion
    subgraph Stage1 ["1. Data Ingestion & Storage"]
        DAG1["DAG 1: Live Ingestion<br/>(Binance → Neon DB)"]:::dag
        DB_Raw[("Neon DB Table:<br/>raw_market_ticks")]:::db
        Binance -->|WebSocket Stream| DAG1
        DAG1 -->|Insert Raw Candles| DB_Raw
    end

    %% Stage 2: Feature Engineering
    subgraph Stage2 ["2. Feature Engineering"]
        DAG2["DAG 2: Feature Engineering<br/>(Indicator Computation)"]:::dag
        DB_Feat[("Neon DB Table:<br/>engineered_features")]:::db
        DB_Raw -->|Read Raw Candles| DAG2
        DAG2 -->|Compute RSI, MACD, MAs| DB_Feat
    end

    %% Stage 3: Model Training
    subgraph Stage3 ["3. Model Training"]
        DAG3["DAG 3: Model Training<br/>(XGBoost Training)"]:::dag
        MLflow["DagsHub MLflow<br/>(Model Registry)"]:::registry
        DB_Feat -->|Fetch Training Features| DAG3
        DAG3 -->|Log Model & Runs| MLflow
    end

    %% Stage 4: Live Inference
    subgraph Stage4 ["4. Live Inference"]
        DAG4["DAG 4: Live Inference<br/>(Generate Signals)"]:::dag
        DB_Pred[("Neon DB Table:<br/>predictions_log")]:::db
        Binance -.->|Fetch Latest 60m Candles| DAG4
        MLflow -->|Load Active Production Model| DAG4
        DAG4 -->|Log Predictions & Signals| DB_Pred
    end

    %% Stage 5: Monitoring & Metrics
    subgraph Stage5 ["5. Performance & Drift Monitoring"]
        DAG5["DAG 5: Data & Model Monitoring<br/>(Accuracy & PSI Drift)"]:::dag
        DB_Metrics[("Neon DB Table:<br/>model_metrics")]:::db
        
        DAG3 -->|Log Training Metrics| DB_Metrics
        DB_Pred -->|Compare Predictions & Actuals| DAG5
        DAG5 -->|Log Accuracy & PSI Drift| DB_Metrics
    end

    %% Stage 6: Visualization
    subgraph Stage6 ["6. Real-time Dashboard"]
        Dashboard["Flask + Socket.IO Dashboard"]:::ui
        Client["Client Browser (Chart.js)"]:::client
        
        DB_Raw -.->|Live Price Feed| Dashboard
        DB_Pred -.->|Predictions & Signals| Dashboard
        DB_Metrics -.->|Drift & Accuracy History| Dashboard
        MLflow -.->|Fetch Active Run Metadata| Dashboard
        Binance -.->|Direct WebSocket Stream| Dashboard
        Dashboard -->|Render Interactive SPA| Client
    end
```

---

## 🛠️ Technology Stack

| Technology | Purpose | Description |
| :--- | :--- | :--- |
| **Astro CLI / Airflow** | Pipeline Orchestration | Manages and monitors the execution of the 5 cron-scheduled DAGs. |
| **Neon Serverless PostgreSQL**| Relational Database | Serves as the central data store for ticks, engineered features, predictions, and drift logs. |
| **XGBoost & Scikit-Learn** | Machine Learning | XGBoost Classifier predicts BUY/SELL direction; XGBoost Regressor forecasts prices. |
| **MLflow & DagsHub** | Experiment Tracking & Registry | Logs parameters, metrics (Accuracy, F1, MAE, R2), artifacts, and hosts the Model Registry. |
| **DVC (Data Version Control)**| Data Versioning | Versions DB-exported CSV data and uploads tracking pointers to DagsHub S3-compatible remote. |
| **Flask + Socket.IO** | Backend Server & WebSockets | Serves APIs and streams Binance ticker updates in real time to the frontend. |
| **Vanilla CSS & HTML5** | Frontend Interface | Styled with modern, high-contrast, premium dark/light mode interfaces with custom animations. |
| **Chart.js** | Interactive Charting | Renders live market views, indicator overlays (Short/Long MA, RSI, MACD), and model evaluations. |

---

## 📂 Project Directory Structure

```
CryptoPulse/
├── .astro/                         # Astro CLI local config
├── .dvc/                           # Data Version Control config & cache pointers
├── dags/                           # Airflow DAG Definitions
│   ├── dag_crypto_ingestion.py     # DAG 1: Binance → Neon DB (Every 1 min)
│   ├── dag_feature_engineering.py # DAG 2: Tech indicator calculations (Every 5 min)
│   ├── dag_model_training.py      # DAG 3: XGBoost MLflow Model Training (Daily/Triggered)
│   ├── dag_live_inference.py      # DAG 4: Live prediction & trade signaling (Every 1 min)
│   └── dag_monitoring.py          # DAG 5: Accuracy & PSI drift analysis (Daily)
├── include/                        # Pipeline Helpers & Scripts
│   ├── setup_db.py                 # Neon PostgreSQL schema initializer
│   ├── dvc_export.py               # Database to DVC versioning pipeline script
│   └── features.py                 # Technical indicator mathematics (RSI, MACD, MA)
├── dashboard/                      # Real-time Web Dashboard Application
│   ├── static/                     # Custom Styles & CSS variables
│   ├── templates/
│   │   └── index.html              # Dashboard SPA (Walkthrough, Compact, Analytics)
│   ├── app.py                      # Flask + Socket.IO live streamer and API service
│   ├── db_migrate.py               # Column migrations for Neon database schema
│   ├── check_pred.py               # Console prediction & ML validation script
│   └── chk.py                      # Database connection verification tool
├── tests/                          # Automated Pytest Suite
│   └── dags/
│       └── test_dag_example.py     # DAG syntax and tag verification tests
├── requirements.txt                # Airflow & Dashboard dependencies
└── Dockerfile                      # Astronomer custom runtime container definition
```

---

## 🗄️ Database Schema Details

CryptoPulse relies on 4 distinct database tables inside Neon PostgreSQL:

### 1. `raw_market_ticks`
Stores the raw 1-minute BTC/USDT and ETH/USDT candlesticks from Binance.
* `timestamp` (TIMESTAMP, Primary Key)
* `open_price` / `high_price` / `low_price` / `close_price` (NUMERIC)
* `volume` (NUMERIC)
* `trades_count` (INT)

### 2. `engineered_features`
Stores calculated features and training targets.
* `timestamp` (TIMESTAMP, Primary Key)
* `close_price` (NUMERIC)
* `rsi_14` (NUMERIC) - Relative Strength Index (14 periods)
* `macd` / `macd_signal` (NUMERIC) - Moving Average Convergence Divergence
* `ma_short` / `ma_long` (NUMERIC) - 7-period and 25-period simple moving averages
* `target_label` (INT) - 1 if next candle close price went up, 0 if it went down
* `next_close_price` (NUMERIC) - The exact close price of the next candle (used for regression training)

### 3. `predictions_log`
Logs every live trade signal produced by the model.
* `prediction_id` (SERIAL, Primary Key)
* `timestamp` (TIMESTAMP, Default current time)
* `input_price` (NUMERIC)
* `predicted_signal` (INT) - 1 for BUY, 0 for SELL/HOLD
* `confidence` (NUMERIC) - Probability score assigned by XGBoost
* `actual_signal` (INT, Nullable) - Populated post-hoc to monitor live predictive performance

### 4. `model_metrics`
Tracks performance history and data drift analytics over time.
* `id` (SERIAL, Primary Key)
* `evaluation_time` (TIMESTAMP, Unique)
* `model_version` (VARCHAR) - The active MLflow run UUID
* `accuracy` / `f1_score` (NUMERIC) - Classification metric scores
* `data_drift_psi` (NUMERIC) - Population Stability Index value

---

## 🚀 Local Quick Start

Follow these steps to launch the pipeline and dashboard locally.

### 1. Prerequisites
Ensure you have the following installed on your machine:
* Python 3.10+
* Docker Desktop (required for Astro CLI)
* [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)

### 2. Clone the Repository & Configure `.env`
Create a `.env` file in the project root directory and supply your connection keys:

```env
# Neon Serverless PostgreSQL Connection
NEON_DATABASE_URL="postgresql://<user>:<password>@<host>:5432/<database>?sslmode=require"

# DagsHub & MLflow Authentication
DAGSHUB_USERNAME="YOUR_DAGSHUB_USERNAME"
DAGSHUB_TOKEN="YOUR_DAGSHUB_API_TOKEN"
DAGSHUB_REPO_OWNER="YOUR_DAGSHUB_REPO_OWNER"
DAGSHUB_REPO_NAME="YOUR_DAGSHUB_REPO_NAME"

# S3 Configurations (Optional, if using DagsHub storage manually)
AWS_ACCESS_KEY_ID="your-key-id"
AWS_SECRET_ACCESS_KEY="your-secret-key"
```

### 3. Setup Virtual Environment & Database Tables
Initialize your local environment, run the database migrations, and create the schema tables:

```bash
# Setup Python Virtual Environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run table creation script
python include/setup_db.py

# Run schema migrations (to add regression target columns)
python dashboard/db_migrate.py
```

### 4. Start Airflow via Astro CLI
To start the local Airflow instance using Astro CLI:

```bash
astro dev start
```
Once initialized, visit the Airflow Webserver:
* **URL**: [http://localhost:8080](http://localhost:8080)
* **Username**: `admin`
* **Password**: `admin`

Enable the DAGs in the following order:
1. `cryptopulse_live_ingestion` (Starts live data stream collection)
2. `cryptopulse_feature_engineering` (Calculates technical indicators)
3. `cryptopulse_model_training` (Trigger manually once >20 features are collected)
4. `cryptopulse_live_inference` (Starts emitting real-time signals)
5. `cryptopulse_monitoring` (Monitors drift daily)

### 5. Launch the Dashboard
Start the Flask + Socket.IO live dashboard locally:

```bash
python dashboard/app.py
```
Open your browser and navigate to: **[http://localhost:5050](http://localhost:5050)**

---

## 🖥️ Live Dashboard Tour

The Web App features three main workspaces optimized for model visualization and debugging:

1. **Live Dashboard**:
   * Displays real-time ticker prices directly from Binance WebSocket streams.
   * Renders interactive Chart.js graphs displaying candles, prices, custom MA overlays, RSI momentum, MACD lines, and predicted regression trends.
   * Includes a **Help Mode** providing contextual annotations and an **Interactive Walkthrough** guiding viewers through key features.
   * Features a toggleable **Compact View** for condensed data density.
2. **Model Center**:
   * Automatically polls experiments and run metadata directly from MLflow.
   * Visualizes model statistics (Accuracy, F1, AUC ROC, MAE, R2) inside interactive Bar, Line, or Radar charts.
   * Allows registering and designating active production models directly from the UI.
3. **Live Data Feed**:
   * Shows a live-updating stream of engineered dataset rows.
   * Tracks system performance stats including API latency and WebSocket update rate.

---

## ☁️ Deployment

### Airflow DAGs (Astronomer Cloud)
1. Install Astro CLI on your system:
   ```bash
   winget install Astronomer.Astro  # Windows
   brew install astronomer/tap/astro # macOS
   ```
2. Log into your Astronomer account:
   ```bash
   astro login
   ```
3. Deploy the project files:
   ```bash
   astro deploy
   ```
4. Configure database and DagsHub credentials in the environment variables tab inside the Astronomer Cloud console UI.

### Web Dashboard (Render.com)
You can host the Flask dashboard for free on Render.com:
1. Create a new **Web Service** on Render and link this repository.
2. Set the following configuration values:
   * **Runtime**: `Python`
   * **Build Command**: `pip install -r requirements.txt`
   * **Start Command**: `gunicorn --worker-class eventlet -w 1 dashboard.app:app`
   * **Instance Type**: `Free`
3. Add `NEON_DATABASE_URL`, `DAGSHUB_USERNAME`, `DAGSHUB_TOKEN`, `PORT` (`5050`) environment variables in Render's dashboard.

---

## 📦 Data Version Control (DVC)

To backup data files and maintain precise version history:
1. Run the backup utility script:
   ```bash
   python include/dvc_export.py
   ```
2. This script pulls the latest dataset tables from Neon DB, saves them as local CSV files (`data/raw_market_ticks.csv` and `data/engineered_features.csv`), updates tracking hash files (`.dvc`), and pushes the actual data payloads directly to DagsHub's remote storage.

---

## 📊 Pipeline Monitoring & Drift Thresholds

CryptoPulse monitors data stability daily via the Population Stability Index (PSI). The pipeline uses the following thresholds for alerting and automated model retraining:

| Metric | Threshold | Level | Trigger Action |
| :--- | :--- | :--- | :--- |
| **Model Accuracy** | `< 55%` | Critical 🔴 | Disables live trade execution & triggers immediate retrain. |
| **PSI (Data Drift)** | `< 0.10` | Stable ✅ | No action required. Pipeline stays healthy. |
| **PSI (Data Drift)** | `0.10 - 0.20` | Warning ⚠️ | Logs warning to MLflow metrics; monitors subsequent runs. |
| **PSI (Data Drift)** | `> 0.20` | Action Required 🔴 | Automates retraining of model models using the newest data distributions. |

---

## 📄 License
This project is licensed under the MIT License - see the LICENSE file for details.
