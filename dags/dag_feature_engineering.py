"""
CryptoPulse — Feature Engineering DAG
=======================================
Triggered by ingestion DAG (also scheduled every 5 min as backup).
Reads raw_market_ticks → computes RSI/MACD/MA → writes engineered_features
→ triggers model_training every 6 hours.
"""
import os, psycopg2, psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable


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

def _db_conn():
    return psycopg2.connect(_db_url(), connect_timeout=15)

# ── DAG ──────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "cryptopulse",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 20),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="cryptopulse_feature_engineering",
    default_args=default_args,
    description="RSI / MACD / MA → engineered_features → trigger model training every 6h",
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["cryptopulse", "features"],
) as dag:

    @task(task_id="fetch_raw_ticks")
    def fetch_raw_ticks():
        """Read ALL raw_market_ticks ordered by time."""
        conn = _db_conn()
        try:
            cur = conn.cursor()
            # Ensure table exists (in case ingestion hasn't run yet)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_market_ticks (
                    id           SERIAL PRIMARY KEY,
                    timestamp    TIMESTAMP NOT NULL,
                    open_price   NUMERIC(20,8),
                    high_price   NUMERIC(20,8),
                    low_price    NUMERIC(20,8),
                    close_price  NUMERIC(20,8),
                    volume       NUMERIC(30,8),
                    trades_count INTEGER DEFAULT 0,
                    source       VARCHAR(50) DEFAULT 'binance.com',
                    created_at   TIMESTAMP DEFAULT NOW(),
                    UNIQUE(timestamp)
                );
            """)
            conn.commit()
            cur.execute("""
                SELECT timestamp, open_price, high_price, low_price,
                       close_price, volume, trades_count
                FROM raw_market_ticks
                ORDER BY timestamp ASC;
            """)
            rows = cur.fetchall()
        finally:
            conn.close()

        cols = ["timestamp","open_price","high_price","low_price","close_price","volume","trades_count"]
        records = []
        for row in rows:
            rec = {}
            for i, col in enumerate(cols):
                v = row[i]
                rec[col] = v.isoformat() if isinstance(v, datetime) else (float(v) if v is not None else None)
            records.append(rec)

        print(f"[FEAT] Fetched {len(records)} raw ticks")
        return records

    @task(task_id="compute_features")
    def compute_features(records: list):
        """Compute RSI(14), MACD(12,26,9), MA(7,25), target_label, next_close_price."""
        if len(records) < 10:
            print(f"[FEAT] Only {len(records)} rows — need at least 10. Skipping.")
            return []

        df = pd.DataFrame(records)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        close = df["close_price"].astype(float)
        n = len(close)

        # RSI
        delta    = close.diff()
        gain     = delta.clip(lower=0)
        loss     = (-delta).clip(lower=0)
        avg_gain = gain.ewm(com=13, min_periods=1).mean()
        avg_loss = loss.ewm(com=13, min_periods=1).mean()
        rs       = avg_gain / avg_loss.replace(0, np.nan)
        df["rsi_14"] = (100 - 100 / (1 + rs)).round(4)

        # MACD
        ema_fast       = close.ewm(span=min(12,n), min_periods=1, adjust=False).mean()
        ema_slow       = close.ewm(span=min(26,n), min_periods=1, adjust=False).mean()
        df["macd"]     = (ema_fast - ema_slow).round(4)
        df["macd_signal"] = df["macd"].ewm(span=min(9,n), min_periods=1, adjust=False).mean().round(4)

        # Moving Averages
        df["ma_short"] = close.rolling(window=min(7,n),  min_periods=1).mean().round(8)
        df["ma_long"]  = close.rolling(window=min(25,n), min_periods=1).mean().round(8)

        # Targets
        df["target_label"]    = (close.shift(-1) > close).astype(int)
        df["next_close_price"] = close.shift(-1)
        df = df[:-1]  # last row has no target

        out = []
        for _, r in df.iterrows():
            out.append({
                "timestamp":       r["timestamp"].isoformat(),
                "close_price":     float(r["close_price"]),
                "rsi_14":          round(float(r["rsi_14"]),       4) if pd.notna(r["rsi_14"]) else None,
                "macd":            round(float(r["macd"]),         4) if pd.notna(r["macd"])   else None,
                "macd_signal":     round(float(r["macd_signal"]),  4) if pd.notna(r["macd_signal"]) else None,
                "ma_short":        round(float(r["ma_short"]),     8) if pd.notna(r["ma_short"])    else None,
                "ma_long":         round(float(r["ma_long"]),      8) if pd.notna(r["ma_long"])     else None,
                "target_label":    int(r["target_label"])             if pd.notna(r["target_label"]) else None,
                "next_close_price":round(float(r["next_close_price"]),2) if pd.notna(r["next_close_price"]) else None,
            })

        print(f"[FEAT] Computed features for {len(out)} records")
        return out

    @task(task_id="store_engineered_features")
    def store_engineered_features(features: list):
        """UPSERT computed features into engineered_features table."""
        if not features:
            print("[FEAT] Nothing to store — skipping")
            return "EMPTY"

        conn = _db_conn()
        try:
            cur = conn.cursor()
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
            conn.commit()

            rows = [(f["timestamp"], f["close_price"], f["rsi_14"], f["macd"],
                     f["macd_signal"], f["ma_short"], f["ma_long"],
                     f["target_label"], f["next_close_price"])
                    for f in features]

            psycopg2.extras.execute_batch(cur, """
                INSERT INTO engineered_features
                    (timestamp, close_price, rsi_14, macd, macd_signal,
                     ma_short, ma_long, target_label, next_close_price)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (timestamp) DO UPDATE SET
                    close_price      = EXCLUDED.close_price,
                    rsi_14           = EXCLUDED.rsi_14,
                    macd             = EXCLUDED.macd,
                    macd_signal      = EXCLUDED.macd_signal,
                    ma_short         = EXCLUDED.ma_short,
                    ma_long          = EXCLUDED.ma_long,
                    target_label     = EXCLUDED.target_label,
                    next_close_price = EXCLUDED.next_close_price;
            """, rows, page_size=200)
            conn.commit()
            print(f"[FEAT] Stored {len(rows)} engineered feature rows")
        except Exception as e:
            print(f"[FEAT] Store failed: {e}")
            raise
        finally:
            conn.close()

    # ── DAG Flow ──────────────────────────────────────────────────────────────
    # Model training runs on its OWN schedule (every 3h) — not triggered here
    raw      = fetch_raw_ticks()
    features = compute_features(raw)
    store_engineered_features(features)
