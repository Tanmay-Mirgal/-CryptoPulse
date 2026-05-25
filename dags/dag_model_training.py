"""
CryptoPulse — Model Training DAG
==================================
Schedule: every 6 hours (also triggered by feature engineering DAG).
1. Fetch training data from engineered_features (Neon DB)
2. Train XGBoost Classifier  → direction (BUY=1 / SELL=0)
3. Train XGBoost Regressor   → predicted next close price ($)
4. Log both to MLflow → DagsHub
5. Save best metrics to model_metrics table (Neon DB)
"""
import os, warnings, psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

warnings.filterwarnings("ignore")

# ── Helpers ──────────────────────────────────────────────────────────────────
def _db_url():
    url = ""
    try:
        url = Variable.get("NEON_DATABASE_URL", default_var="")
    except Exception:
        pass
    if not url:
        url = os.environ.get("NEON_DATABASE_URL", "")
    if not url:
        url = "postgresql://neondb_owner:YOUR_NEON_DATABASE_PASSWORD@ep-flat-shadow-aq6optjf-pooler.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require"
    url = url.replace("&channel_binding=require", "").replace("?channel_binding=require&", "?").replace("?channel_binding=require", "")
    return url

def _get_var(key, default=""):
    val = ""
    try:
        val = Variable.get(key, default_var="")
    except Exception:
        pass
    return val or os.environ.get(key, default)

def _setup_mlflow():
    import mlflow
    user      = _get_var("DAGSHUB_USERNAME",  "Tanmay-Mirgal")
    token     = _get_var("DAGSHUB_TOKEN",     "YOUR_DAGSHUB_TOKEN")
    owner     = _get_var("DAGSHUB_REPO_OWNER","Tanmay-Mirgal")
    repo      = _get_var("DAGSHUB_REPO_NAME", "CryptoPulse")
    os.environ["MLFLOW_TRACKING_USERNAME"] = user
    os.environ["MLFLOW_TRACKING_PASSWORD"] = token
    mlflow.set_tracking_uri(f"https://dagshub.com/{owner}/{repo}.mlflow")
    mlflow.set_experiment("CryptoPulse-XGBoost-Trading")
    return mlflow

# ── DAG ──────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "cryptopulse",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 20),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cryptopulse_model_training",
    default_args=default_args,
    description="XGBoost Classifier + Regressor → MLflow/DagsHub → Neon DB metrics",
    schedule="0 */3 * * *",   # Every 3 hours

    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["cryptopulse", "training", "mlflow"],
) as dag:

    @task(task_id="fetch_training_data")
    def fetch_training_data():
        """Read engineered features with both targets set."""
        conn = None
        try:
            conn = psycopg2.connect(_db_url(), connect_timeout=15)
            cur  = conn.cursor()
            # Ensure table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS engineered_features (
                    id               SERIAL,
                    timestamp        TIMESTAMP NOT NULL PRIMARY KEY,
                    close_price      NUMERIC(20,8),
                    rsi_14           NUMERIC(10,4),
                    macd             NUMERIC(10,4),
                    macd_signal      NUMERIC(10,4),
                    ma_short         NUMERIC(20,8),
                    ma_long          NUMERIC(20,8),
                    target_label     INTEGER,
                    next_close_price NUMERIC(20,8),
                    created_at       TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS model_metrics (
                    id              SERIAL PRIMARY KEY,
                    evaluation_time TIMESTAMP UNIQUE DEFAULT NOW(),
                    model_version   VARCHAR(100),
                    accuracy        NUMERIC(6,4),
                    f1_score        NUMERIC(6,4),
                    data_drift_psi  NUMERIC(8,4)
                );
            """)
            conn.commit()

            cur.execute("""
                SELECT timestamp, close_price, rsi_14, macd, macd_signal,
                       ma_short, ma_long, target_label, next_close_price
                FROM engineered_features
                WHERE target_label IS NOT NULL
                  AND next_close_price IS NOT NULL
                  AND rsi_14 IS NOT NULL
                ORDER BY timestamp ASC;
            """)
            rows = cur.fetchall()
        finally:
            if conn: conn.close()

        cols = ["timestamp","close_price","rsi_14","macd","macd_signal",
                "ma_short","ma_long","target_label","next_close_price"]
        records = []
        for row in rows:
            rec = {}
            for i, col in enumerate(cols):
                v = row[i]
                rec[col] = v.isoformat() if isinstance(v, datetime) else (float(v) if v is not None else None)
            records.append(rec)

        print(f"[TRAIN] Fetched {len(records)} training rows")
        return records

    @task(task_id="train_classifier")
    def train_classifier(records: list):
        """XGBoost Classifier → BUY(1) or SELL(0) prediction."""
        import mlflow, mlflow.xgboost
        from xgboost import XGBClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

        if len(records) < 20:
            print(f"[CLF] Only {len(records)} rows — need 20+. Skipping.")
            return {"status": "SKIPPED", "model_type": "classifier", "accuracy": 0, "f1_score": 0, "auc_roc": 0}

        mlflow = _setup_mlflow()

        df   = pd.DataFrame(records)
        feat = ["close_price","rsi_14","macd","macd_signal","ma_short","ma_long"]
        df   = df.dropna(subset=feat + ["target_label"])
        X    = df[feat].values.astype(float)
        y    = df["target_label"].astype(int).values

        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=False)

        params = {"n_estimators":100,"max_depth":4,"learning_rate":0.05,
                  "subsample":0.8,"colsample_bytree":0.8,"random_state":42,"verbosity":0}

        import mlflow as mlf
        mlf.set_experiment("CryptoPulse-XGBoost-Trading")
        with mlf.start_run(run_name=f"clf_{datetime.utcnow().strftime('%Y%m%d_%H%M')}") as run:
            run_id = run.info.run_id
            model  = XGBClassifier(**params)
            model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
            y_pred = model.predict(X_te)
            y_prob = model.predict_proba(X_te)[:, 1]

            acc  = accuracy_score(y_te, y_pred)
            f1   = f1_score(y_te, y_pred, average="weighted", zero_division=0)
            prec = precision_score(y_te, y_pred, average="weighted", zero_division=0)
            rec  = recall_score(y_te, y_pred, average="weighted", zero_division=0)
            try:   auc = roc_auc_score(y_te, y_prob)
            except: auc = 0.0

            mlf.log_params({"model_type":"classifier","target":"direction_BUY_SELL",
                            "train_samples":len(X_tr), "test_samples":len(X_te), **params})
            mlf.log_metrics({"accuracy":round(acc,4),"f1_score":round(f1,4),
                             "precision":round(prec,4),"recall":round(rec,4),"auc_roc":round(auc,4)})
            mlf.xgboost.log_model(model, artifact_path="classifier_model",
                                  registered_model_name="CryptoPulse-Classifier",
                                  input_example=X_te[:3])

        print(f"[CLF] Acc={acc:.4f} F1={f1:.4f} AUC={auc:.4f} run={run_id[:8]}")
        return {"status":"SUCCESS","model_type":"classifier","run_id":run_id,
                "accuracy":round(acc,4),"f1_score":round(f1,4),"auc_roc":round(auc,4),
                "train_samples":len(X_tr),"test_samples":len(X_te)}

    @task(task_id="train_regressor")
    def train_regressor(records: list):
        """XGBoost Regressor → predicted next 1-min close price ($)."""
        import mlflow as mlf, mlflow.xgboost
        from xgboost import XGBRegressor
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        if len(records) < 20:
            print(f"[REG] Only {len(records)} rows — need 20+. Skipping.")
            return {"status": "SKIPPED", "model_type": "regressor", "mae": 0, "r2": 0}

        mlf.set_experiment("CryptoPulse-XGBoost-Trading")
        _setup_mlflow()

        df   = pd.DataFrame(records)
        feat = ["close_price","rsi_14","macd","macd_signal","ma_short","ma_long"]
        df   = df.dropna(subset=feat + ["next_close_price"])
        X    = df[feat].values.astype(float)
        y    = df["next_close_price"].astype(float).values

        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=False)

        params = {"n_estimators":100,"max_depth":4,"learning_rate":0.05,
                  "subsample":0.8,"colsample_bytree":0.8,"random_state":42,"verbosity":0}

        with mlf.start_run(run_name=f"reg_{datetime.utcnow().strftime('%Y%m%d_%H%M')}") as run:
            run_id = run.info.run_id
            model  = XGBRegressor(**params)
            model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
            y_pred = model.predict(X_te)

            mae  = mean_absolute_error(y_te, y_pred)
            rmse = float(np.sqrt(mean_squared_error(y_te, y_pred)))
            r2   = r2_score(y_te, y_pred)
            mape = float(np.mean(np.abs((y_te - y_pred) / np.maximum(np.abs(y_te), 1e-8))) * 100)

            mlf.log_params({"model_type":"regressor","target":"next_close_price_USD",
                            "train_samples":len(X_tr),"test_samples":len(X_te), **params})
            mlf.log_metrics({"mae":round(mae,4),"rmse":round(rmse,4),"r2":round(r2,4),"mape":round(mape,4)})
            mlf.xgboost.log_model(model, artifact_path="regressor_model",
                                  registered_model_name="CryptoPulse-Regressor",
                                  input_example=X_te[:3])

        print(f"[REG] MAE=${mae:.2f} RMSE=${rmse:.2f} R2={r2:.4f} MAPE={mape:.2f}% run={run_id[:8]}")
        return {"status":"SUCCESS","model_type":"regressor","run_id":run_id,
                "mae":round(mae,4),"rmse":round(rmse,4),"r2":round(r2,4),"mape":round(mape,4),
                "train_samples":len(X_tr),"test_samples":len(X_te)}

    @task(task_id="save_metrics")
    def save_metrics(clf: dict, reg: dict):
        """Save both models' metrics to model_metrics table (Neon DB)."""
        conn = None
        try:
            conn = psycopg2.connect(_db_url(), connect_timeout=15)
            cur  = conn.cursor()
            if clf.get("status") == "SUCCESS":
                cur.execute("""
                    INSERT INTO model_metrics (evaluation_time, model_version, accuracy, f1_score)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (evaluation_time) DO UPDATE SET
                        model_version = EXCLUDED.model_version,
                        accuracy      = EXCLUDED.accuracy,
                        f1_score      = EXCLUDED.f1_score;
                """, (datetime.utcnow(), clf.get("run_id","")[:50],
                      clf.get("accuracy",0), clf.get("f1_score",0)))
                conn.commit()
                print(f"[TRAIN] Metrics saved — Acc={clf['accuracy']} F1={clf['f1_score']}")
            if reg.get("status") == "SUCCESS":
                print(f"[TRAIN] Regressor — MAE={reg['mae']} R2={reg['r2']}")
        except Exception as e:
            print(f"[TRAIN] Metrics save error: {e}")
        finally:
            if conn: conn.close()

    # ── DAG Flow ──────────────────────────────────────────────────────────────
    data = fetch_training_data()
    clf  = train_classifier(data)
    reg  = train_regressor(data)
    save_metrics(clf, reg)
