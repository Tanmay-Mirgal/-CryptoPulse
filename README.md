# CryptoPulse MLOps Pipeline 🚀

## Architecture Overview

```
Binance API (Free, No Key)
        │
        ▼ (every 1 min)
┌─────────────────────────────┐
│  DAG 1: Live Ingestion      │  ← raw_market_ticks (Neon DB)
└─────────────────────────────┘
        │
        ▼ (every 5 min)
┌─────────────────────────────┐
│  DAG 2: Feature Engineering │  ← RSI, MACD, MA → engineered_features (Neon DB)
└─────────────────────────────┘
        │
        ▼ (daily 02:00 UTC)
┌─────────────────────────────┐
│  DAG 3: Model Training      │  ← XGBoost → MLflow/DagsHub Registry
└─────────────────────────────┘
        │
        ▼ (every 1 min)
┌─────────────────────────────┐
│  DAG 4: Live Inference      │  ← BUY/SELL/HOLD → predictions_log (Neon DB)
└─────────────────────────────┘
        │
        ▼ (daily 03:00 UTC)
┌─────────────────────────────┐
│  DAG 5: Monitoring          │  ← Accuracy + PSI Drift → model_metrics (Neon DB) + MLflow
└─────────────────────────────┘
```

## Project Structure

```
test_astro_airflow/
├── dags/
│   ├── dag_crypto_ingestion.py      # Step 1: Binance → Neon DB (every 1 min)
│   ├── dag_feature_engineering.py  # Step 2: RSI/MACD/MA computation (every 5 min)
│   ├── dag_model_training.py       # Step 3: XGBoost training + MLflow tracking (daily)
│   ├── dag_live_inference.py       # Step 4: Live BUY/SELL prediction (every 1 min)
│   └── dag_monitoring.py           # Step 5: Accuracy + PSI drift detection (daily)
├── include/
│   ├── setup_db.py                 # DB schema initialization script
│   └── features.py                 # Feature engineering helper functions
├── .env                            # Environment variables (do not commit!)
├── requirements.txt                # Python dependencies
└── Dockerfile                      # Astro Runtime base image
```

## Environment Variables (.env)

| Variable | Description | Required |
|---|---|---|
| `NEON_DATABASE_URL` | Neon Serverless PostgreSQL connection string | ✅ |
| `DAGSHUB_USERNAME` | Your DagsHub username | ✅ for MLflow |
| `DAGSHUB_TOKEN` | DagsHub API token | ✅ for MLflow |
| `DAGSHUB_REPO_OWNER` | DagsHub repo owner | ✅ for MLflow |
| `DAGSHUB_REPO_NAME` | DagsHub repository name | ✅ for MLflow |
| `AWS_ACCESS_KEY_ID` | AWS credentials | Optional |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials | Optional |

## Quick Start

### 1. Initialize Database Tables
```bash
cd include
python setup_db.py
```

### 2. Start Airflow (Astro CLI)
```bash
astro dev start
```

### 3. Access Airflow UI
Open: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 4. Enable DAGs (in order)
1. `cryptopulse_live_ingestion` - Start data collection first
2. `cryptopulse_feature_engineering` - After ~30 minutes of data
3. `cryptopulse_model_training` - Trigger manually once enough data
4. `cryptopulse_live_inference` - After model is registered in MLflow
5. `cryptopulse_monitoring` - Enable last

## Database Tables

### `raw_market_ticks`
Stores 1-minute BTC/USDT OHLCV candlesticks from Binance.

### `engineered_features`
Stores computed ML features:
- `rsi_14` - Relative Strength Index (14 period)
- `macd` / `macd_signal` - MACD indicator
- `ma_short` / `ma_long` - Moving averages (7/25 period)
- `target_label` - 1=Price Up next candle, 0=Price Down

### `predictions_log`
Stores live trade signals with confidence scores and actual outcomes.

### `model_metrics`
Stores model performance history (accuracy, F1, PSI drift).

## MLflow / DagsHub Integration

All experiments are tracked at:
```
https://dagshub.com/Tanmay-Mirgal/CryptoPulse.mlflow
```

Models are registered under: `CryptoPulse-XGBoost`

## Monitoring Alerts

| Metric | Threshold | Action |
|---|---|---|
| Model Accuracy | < 55% | Retrain alert |
| PSI (Data Drift) | > 0.20 | Drift alert + retrain |

PSI Interpretation:
- `< 0.10` → No drift (stable) ✅
- `0.10 - 0.20` → Minor drift (monitor) ⚠️
- `> 0.20` → Major drift (retrain!) 🔴
