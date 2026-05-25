"""
CryptoPulse - Step 4: Live Inference / Prediction DAG
======================================================
Yeh DAG har 1 minute mein run hota hai:
1. Latest BTC/USDT market data fetch karta hai
2. Real-time features compute karta hai
3. MLflow Registry se latest 'Production' model load karta hai
4. BUY / SELL / HOLD prediction generate karta hai
5. Prediction + confidence score ko predictions_log table mein store karta hai
"""

import os
import sys
import psycopg2
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv

load_dotenv()

default_args = {
    "owner": "cryptopulse",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 20),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

SIGNAL_MAP = {0: "SELL/HOLD", 1: "BUY"}

with DAG(
    dag_id="cryptopulse_live_inference",
    default_args=default_args,
    description="Fetches live BTC data, runs XGBoost inference, stores BUY/SELL signal with confidence",
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cryptopulse", "inference", "mlops", "live"],
) as dag:

    @task(task_id="fetch_latest_market_data")
    def fetch_latest_market_data():
        """
        Binance se last 60 candles fetch karta hai (features compute karne ke liye
        enough history chahiye - RSI ke liye 14+, MACD ke liye 26+ rows).
        """
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 60}

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        records = []
        for kline in data:
            records.append({
                "timestamp": datetime.fromtimestamp(kline[0] / 1000).isoformat(),
                "open_price": float(kline[1]),
                "high_price": float(kline[2]),
                "low_price": float(kline[3]),
                "close_price": float(kline[4]),
                "volume": float(kline[5]),
                "trades_count": int(kline[8]),
            })

        print(f"[INFO] Fetched {len(records)} candles from Binance. Latest: {records[-1]['timestamp']}")
        return records

    @task(task_id="compute_live_features")
    def compute_live_features(records: list):
        """Latest record ke liye real-time features compute karta hai."""
        sys.path.insert(0, "/usr/local/airflow/include")
        from features import engineer_features

        if not records:
            raise ValueError("No market data received.")

        df = pd.DataFrame(records)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["close_price"] = df["close_price"].astype(float)

        featured_df = engineer_features(df)

        if featured_df.empty:
            print("[WARNING] Not enough data for feature computation yet (need 26+ rows).")
            return None

        # Sabse latest row lao (prediction ke liye)
        latest = featured_df.iloc[-1]
        feature_dict = {
            "timestamp": latest["timestamp"].isoformat(),
            "close_price": float(latest["close_price"]),
            "rsi_14": float(latest["rsi_14"]),
            "macd": float(latest["macd"]),
            "macd_signal": float(latest["macd_signal"]),
            "ma_short": float(latest["ma_short"]),
            "ma_long": float(latest["ma_long"]),
        }

        print(f"[INFO] Live features: {feature_dict}")
        return feature_dict

    @task(task_id="run_inference")
    def run_inference(features: dict):
        """
        MLflow Registry se latest Production model load karke prediction karta hai.
        Fallback: Agar model registered nahi hai, toh simple RSI-based rule use karta hai.
        """
        import mlflow
        import mlflow.xgboost
        import numpy as np

        if not features:
            print("[WARNING] No features available. Skipping inference.")
            return None

        dagshub_user = os.getenv("DAGSHUB_USERNAME", "")
        dagshub_token = os.getenv("DAGSHUB_TOKEN", "")
        dagshub_repo_owner = os.getenv("DAGSHUB_REPO_OWNER", "")
        dagshub_repo_name = os.getenv("DAGSHUB_REPO_NAME", "CryptoPulse")

        tracking_uri = f"https://dagshub.com/{dagshub_repo_owner}/{dagshub_repo_name}.mlflow"
        os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_user
        os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token
        mlflow.set_tracking_uri(tracking_uri)

        feature_cols = ["close_price", "rsi_14", "macd", "macd_signal", "ma_short", "ma_long"]
        X = np.array([[features[c] for c in feature_cols]])

        predicted_signal = None
        confidence = None
        model_version_used = "rule_based_fallback"

        # Try to load model from MLflow Registry
        try:
            model_uri = "models:/CryptoPulse-XGBoost/Production"
            model = mlflow.xgboost.load_model(model_uri)
            proba = model.predict_proba(X)[0]
            predicted_signal = int(proba.argmax())
            confidence = float(proba.max())
            model_version_used = "mlflow_production"
            print(f"[INFO] MLflow model loaded. Prediction: {SIGNAL_MAP[predicted_signal]} | Confidence: {confidence:.4f}")

        except Exception as e:
            print(f"[INFO] MLflow model not available yet ({e}). Using RSI rule-based fallback.")
            # Simple RSI rule: RSI < 35 = BUY, RSI > 65 = SELL, else HOLD
            rsi = features["rsi_14"]
            if rsi < 35:
                predicted_signal = 1   # BUY
                confidence = round(min(0.5 + (35 - rsi) / 70, 0.95), 4)
            elif rsi > 65:
                predicted_signal = 0   # SELL
                confidence = round(min(0.5 + (rsi - 65) / 70, 0.95), 4)
            else:
                predicted_signal = 0   # HOLD
                confidence = 0.5
            print(f"[FALLBACK] RSI={rsi:.2f} → {SIGNAL_MAP[predicted_signal]} | Confidence={confidence:.4f}")

        return {
            "timestamp": features["timestamp"],
            "input_price": features["close_price"],
            "predicted_signal": predicted_signal,
            "confidence": confidence,
            "model_version_used": model_version_used,
            "signal_label": SIGNAL_MAP.get(predicted_signal, "UNKNOWN"),
        }

    @task(task_id="store_prediction")
    def store_prediction(prediction: dict):
        """Prediction result ko predictions_log table mein store karta hai."""
        if not prediction:
            print("[WARNING] No prediction to store.")
            return "SKIPPED"

        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url or "YOUR_NEON" in db_url:
            print("[WARNING] DB not configured. Prediction result:")
            print(prediction)
            return "SKIPPED_DB"

        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO predictions_log (timestamp, input_price, predicted_signal, confidence)
                VALUES (%s, %s, %s, %s);
            """, (
                prediction["timestamp"],
                prediction["input_price"],
                prediction["predicted_signal"],
                prediction["confidence"],
            ))

            conn.commit()
            cur.close()
            print(f"[SUCCESS] 🚦 Signal: {prediction['signal_label']} | "
                  f"Price: ${prediction['input_price']:,.2f} | "
                  f"Confidence: {prediction['confidence']:.2%} | "
                  f"Model: {prediction['model_version_used']}")
            return "SUCCESS"

        except Exception as e:
            print(f"[ERROR] Failed to store prediction: {e}")
            return f"FAILED: {e}"
        finally:
            if conn:
                conn.close()

    # ── Pipeline Chain ──
    market_data = fetch_latest_market_data()
    live_features = compute_live_features(market_data)
    prediction = run_inference(live_features)
    store_prediction(prediction)
