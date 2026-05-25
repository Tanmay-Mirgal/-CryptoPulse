"""
CryptoPulse - Step 5: Monitoring, Drift Detection & Model Evaluation DAG
=========================================================================
Yeh DAG daily raat 3 baje (UTC) run hota hai:
1. predictions_log se last 24 ghante ki predictions fetch karta hai
2. Actual price action se compare karke accuracy calculate karta hai
3. PSI (Population Stability Index) compute karke data drift detect karta hai
4. Results model_metrics table mein store karta hai
5. Agar accuracy < 55% ya PSI > 0.2 ho, toh MLflow mein alert log karta hai
"""

import os
import sys
import psycopg2
import pandas as pd
import numpy as np
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
    "retry_delay": timedelta(minutes=5),
}

# Thresholds
ACCURACY_THRESHOLD = 0.55   # Isse kam hua toh retrain alert
PSI_THRESHOLD = 0.20        # Isse zyada hua toh data drift alert


def compute_psi(reference: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
    """
    PSI (Population Stability Index) compute karta hai.
    PSI < 0.1:  No drift (stable)
    PSI 0.1-0.2: Minor drift (monitor closely)
    PSI > 0.2:  Major drift (retrain model!)
    """
    ref_min = min(reference.min(), current.min())
    ref_max = max(reference.max(), current.max())
    bins = np.linspace(ref_min, ref_max, n_bins + 1)

    ref_hist, _ = np.histogram(reference, bins=bins)
    cur_hist, _ = np.histogram(current, bins=bins)

    ref_pct = (ref_hist / len(reference)) + 1e-9
    cur_pct = (cur_hist / len(current)) + 1e-9

    psi = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
    return round(float(psi), 4)


with DAG(
    dag_id="cryptopulse_monitoring",
    default_args=default_args,
    description="Monitors model performance, computes accuracy & PSI drift, triggers retrain alerts",
    schedule="0 3 * * *",  # Roz raat 3 baje UTC
    catchup=False,
    max_active_runs=1,
    tags=["cryptopulse", "monitoring", "drift", "mlops"],
) as dag:

    @task(task_id="fetch_predictions_for_evaluation")
    def fetch_predictions_for_evaluation():
        """
        Last 24 ghante ki predictions fetch karta hai jahan actual_signal available hai.
        actual_signal = actual future price direction (baad mein update hota hai).
        """
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url or "YOUR_NEON" in db_url:
            print("[WARNING] DB not configured.")
            return []

        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()

            # Last 48 ghante ki predictions lo (actual updates ke liye time chahiye)
            cur.execute("""
                SELECT prediction_id, timestamp, input_price, predicted_signal, confidence, actual_signal
                FROM predictions_log
                WHERE timestamp >= NOW() - INTERVAL '48 hours'
                ORDER BY timestamp ASC;
            """)
            rows = cur.fetchall()
            cur.close()

            columns = ["prediction_id", "timestamp", "input_price", "predicted_signal", "confidence", "actual_signal"]
            records = []
            for row in rows:
                record = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if isinstance(val, datetime):
                        val = val.isoformat()
                    elif hasattr(val, "__float__"):
                        val = float(val)
                    record[col] = val
                records.append(record)

            print(f"[INFO] Fetched {len(records)} prediction records for evaluation.")
            return records

        except Exception as e:
            print(f"[ERROR] Failed to fetch predictions: {e}")
            return []
        finally:
            if conn:
                conn.close()

    @task(task_id="backfill_actual_signals")
    def backfill_actual_signals():
        """
        Predictions mein actual_signal fill karta hai using raw_market_ticks.
        Logic: Prediction ke 5 minutes baad close_price zyada hai toh actual=1 (went UP), else 0.
        """
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url or "YOUR_NEON" in db_url:
            return "SKIPPED"

        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()

            # NULL actual_signal wale predictions find karo
            cur.execute("""
                SELECT p.prediction_id, p.timestamp, p.input_price,
                       t.close_price AS future_price
                FROM predictions_log p
                JOIN raw_market_ticks t ON t.timestamp = (
                    SELECT timestamp FROM raw_market_ticks
                    WHERE timestamp > p.timestamp
                    ORDER BY timestamp ASC
                    LIMIT 1 OFFSET 4  -- 5 minutes baad
                )
                WHERE p.actual_signal IS NULL
                AND p.timestamp <= NOW() - INTERVAL '10 minutes';
            """)
            rows = cur.fetchall()

            updated = 0
            for row in rows:
                pred_id, pred_ts, input_price, future_price = row
                if future_price and input_price:
                    actual = 1 if float(future_price) > float(input_price) else 0
                    cur.execute(
                        "UPDATE predictions_log SET actual_signal = %s WHERE prediction_id = %s",
                        (actual, pred_id)
                    )
                    updated += 1

            conn.commit()
            cur.close()
            print(f"[INFO] Backfilled actual_signal for {updated} predictions.")
            return f"UPDATED: {updated}"

        except Exception as e:
            print(f"[ERROR] Backfill failed: {e}")
            return f"FAILED: {e}"
        finally:
            if conn:
                conn.close()

    @task(task_id="compute_model_performance")
    def compute_model_performance(predictions: list, backfill_status: str):
        """
        Accuracy, F1-score aur PSI drift compute karta hai.
        """
        from sklearn.metrics import accuracy_score, f1_score

        if not predictions:
            print("[WARNING] No predictions to evaluate.")
            return None

        df = pd.DataFrame(predictions)

        # Sirf wo records jahan actual_signal available hai
        evaluated = df.dropna(subset=["actual_signal"])

        if len(evaluated) < 10:
            print(f"[WARNING] Only {len(evaluated)} evaluated predictions. Need at least 10 for metrics.")
            return None

        y_true = evaluated["actual_signal"].astype(int).values
        y_pred = evaluated["predicted_signal"].astype(int).values

        acc = accuracy_score(y_true, y_pred)
        f1 = f1_score(y_true, y_pred, average="weighted", zero_division=0)

        print(f"[METRICS] Accuracy: {acc:.4f} | F1-Score: {f1:.4f} | Evaluated: {len(evaluated)} predictions")

        # PSI Computation (close_price distribution)
        psi_value = None
        if len(df) >= 30:
            mid = len(df) // 2
            reference_prices = df["input_price"].values[:mid]
            current_prices = df["input_price"].values[mid:]
            psi_value = compute_psi(reference_prices, current_prices)
            print(f"[DRIFT] PSI = {psi_value:.4f} (threshold: {PSI_THRESHOLD})")

            if psi_value > PSI_THRESHOLD:
                print(f"[⚠️ ALERT] DATA DRIFT DETECTED! PSI={psi_value:.4f} exceeds threshold {PSI_THRESHOLD}")
                print("[ACTION] Consider retraining the model with recent data!")
        else:
            psi_value = 0.0
            print("[INFO] Not enough data for PSI calculation.")

        # Accuracy alert
        if acc < ACCURACY_THRESHOLD:
            print(f"[⚠️ ALERT] MODEL PERFORMANCE DEGRADED! Accuracy={acc:.4f} < Threshold={ACCURACY_THRESHOLD}")
            print("[ACTION] Manual review or automatic retraining recommended!")

        return {
            "accuracy": round(acc, 4),
            "f1_score": round(f1, 4),
            "psi": round(psi_value, 4) if psi_value is not None else 0.0,
            "evaluated_count": len(evaluated),
            "drift_detected": (psi_value or 0) > PSI_THRESHOLD,
            "accuracy_degraded": acc < ACCURACY_THRESHOLD,
        }

    @task(task_id="log_metrics_to_mlflow_and_db")
    def log_metrics_to_mlflow_and_db(metrics: dict):
        """
        Monitoring metrics ko MLflow aur Neon DB dono mein log karta hai.
        """
        if not metrics:
            print("[INFO] No metrics to log.")
            return "SKIPPED"

        # ── Log to Neon DB ──
        db_url = os.getenv("NEON_DATABASE_URL")
        if db_url and "YOUR_NEON" not in db_url:
            conn = None
            try:
                conn = psycopg2.connect(db_url)
                cur = conn.cursor()

                cur.execute("""
                    INSERT INTO model_metrics (evaluation_time, model_version, accuracy, f1_score, data_drift_psi)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (evaluation_time) DO UPDATE SET
                        accuracy = EXCLUDED.accuracy,
                        f1_score = EXCLUDED.f1_score,
                        data_drift_psi = EXCLUDED.data_drift_psi;
                """, (
                    datetime.utcnow(),
                    "monitoring_evaluation",
                    metrics["accuracy"],
                    metrics["f1_score"],
                    metrics["psi"],
                ))

                conn.commit()
                cur.close()
                print("[SUCCESS] Monitoring metrics stored in Neon DB.")

            except Exception as e:
                print(f"[ERROR] DB metrics storage failed: {e}")
            finally:
                if conn:
                    conn.close()

        # ── Log to MLflow ──
        try:
            import mlflow

            dagshub_user = os.getenv("DAGSHUB_USERNAME", "")
            dagshub_token = os.getenv("DAGSHUB_TOKEN", "")
            dagshub_repo_owner = os.getenv("DAGSHUB_REPO_OWNER", "")
            dagshub_repo_name = os.getenv("DAGSHUB_REPO_NAME", "CryptoPulse")

            os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_user
            os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token
            mlflow.set_tracking_uri(f"https://dagshub.com/{dagshub_repo_owner}/{dagshub_repo_name}.mlflow")

            mlflow.set_experiment("CryptoPulse-Monitoring")

            with mlflow.start_run(run_name=f"monitoring_{datetime.now().strftime('%Y%m%d_%H%M')}"):
                mlflow.log_metric("live_accuracy", metrics["accuracy"])
                mlflow.log_metric("live_f1_score", metrics["f1_score"])
                mlflow.log_metric("psi_drift", metrics["psi"])
                mlflow.log_metric("evaluated_predictions", metrics["evaluated_count"])
                mlflow.log_param("drift_detected", str(metrics["drift_detected"]))
                mlflow.log_param("accuracy_degraded", str(metrics["accuracy_degraded"]))

            print("[SUCCESS] Monitoring metrics logged to DagsHub MLflow.")

        except Exception as e:
            print(f"[WARNING] MLflow logging failed: {e}")

        # Summary
        status_icon = "🟢" if not metrics["drift_detected"] and not metrics["accuracy_degraded"] else "🔴"
        print(f"\n{'='*50}")
        print(f"{status_icon} CRYPTOPULSE MONITORING REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*50}")
        print(f"  Accuracy    : {metrics['accuracy']:.2%}")
        print(f"  F1-Score    : {metrics['f1_score']:.4f}")
        print(f"  PSI Drift   : {metrics['psi']:.4f}")
        print(f"  Evaluated   : {metrics['evaluated_count']} predictions")
        print(f"  Drift Alert : {'YES ⚠️' if metrics['drift_detected'] else 'NO ✓'}")
        print(f"  Perf Alert  : {'YES ⚠️' if metrics['accuracy_degraded'] else 'NO ✓'}")
        print(f"{'='*50}\n")

        return "SUCCESS"

    # ── Pipeline Chain ──
    preds = fetch_predictions_for_evaluation()
    backfill = backfill_actual_signals()
    performance = compute_model_performance(preds, backfill)
    log_metrics_to_mlflow_and_db(performance)
