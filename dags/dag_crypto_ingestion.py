"""
CryptoPulse - Ingestion DAG
Every 1 min: fetch BTC/USDT OHLC -> store Neon DB -> trigger feature engineering.
"""
import os
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def _db_url() -> str:
    """Get DB URL from Airflow Variable -> env var -> fallback."""
    url = ""
    try:
        url = Variable.get("NEON_DATABASE_URL", default_var="")
    except Exception:
        pass
    if not url:
        url = os.environ.get("NEON_DATABASE_URL", "")
    if not url:
        raise ValueError("NEON_DATABASE_URL is required")
    return url.replace("&channel_binding=require", "").replace("?channel_binding=require&", "?").replace("?channel_binding=require", "")


def _latest_closed_from_klines(klines: list) -> dict:
    if not klines:
        raise ValueError("empty kline response")
    k = klines[-2] if len(klines) >= 2 else klines[-1]
    return {
        "timestamp": datetime.utcfromtimestamp(k[0] / 1000).isoformat(),
        "open_price": float(k[1]),
        "high_price": float(k[2]),
        "low_price": float(k[3]),
        "close_price": float(k[4]),
        "volume": float(k[5]),
        "trades_count": int(k[8]) if len(k) > 8 else 0,
    }


default_args = {
    "owner": "cryptopulse",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 20),
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=45),
}

with DAG(
    dag_id="cryptopulse_live_ingestion",
    default_args=default_args,
    description="BTC/USDT 1-min candle -> Neon DB -> trigger feature engineering",
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["cryptopulse", "ingestion"],
) as dag:

    @task(task_id="fetch_binance_data")
    def fetch_binance_data():
        """Multi-source BTC fetch with host rotation and soft-skip."""

        def _binance_com():
            hosts = [
                "https://api.binance.com",
                "https://api1.binance.com",
                "https://api2.binance.com",
                "https://api3.binance.com",
            ]
            last_err = None
            for host in hosts:
                try:
                    r = requests.get(
                        f"{host}/api/v3/klines",
                        params={"symbol": "BTCUSDT", "interval": "1m", "limit": 2},
                        timeout=10,
                    )
                    r.raise_for_status()
                    rec = _latest_closed_from_klines(r.json())
                    rec["source"] = host.replace("https://", "")
                    return rec
                except Exception as e:
                    print(f"[FETCH] {host} failed: {e}")
                    last_err = e
            raise RuntimeError(f"All Binance hosts failed. Last: {last_err}")

        def _binance_us():
            r = requests.get(
                "https://api.binance.us/api/v3/klines",
                params={"symbol": "BTCUSD", "interval": "1m", "limit": 2},
                timeout=10,
            )
            r.raise_for_status()
            rec = _latest_closed_from_klines(r.json())
            rec["source"] = "binance.us"
            return rec

        def _cryptocompare():
            r = requests.get(
                "https://min-api.cryptocompare.com/data/histominute",
                params={"fsym": "BTC", "tsym": "USD", "limit": 2},
                timeout=10,
            )
            r.raise_for_status()
            d = r.json()
            if d.get("Response") != "Success":
                raise ValueError(d.get("Message", "cryptocompare error"))
            candles = d["Data"]["Data"]
            c = candles[-2] if len(candles) >= 2 else candles[-1]
            return {
                "timestamp": datetime.utcfromtimestamp(c["time"]).isoformat(),
                "open_price": float(c["open"]),
                "high_price": float(c["high"]),
                "low_price": float(c["low"]),
                "close_price": float(c["close"]),
                "volume": float(c.get("volumefrom", 0)),
                "trades_count": 0,
                "source": "cryptocompare",
            }

        last_err = None
        for name, fn in [
            ("binance.com", _binance_com),
            ("binance.us", _binance_us),
            ("cryptocompare", _cryptocompare),
        ]:
            try:
                print(f"[FETCH] Trying {name}...")
                rec = fn()
                print(f"[FETCH] OK from {name} - close=${rec['close_price']}")
                return rec
            except Exception as e:
                print(f"[FETCH] {name} failed: {e}")
                last_err = e

        # Soft skip to avoid long red streaks from transient provider/network outages.
        print(f"[FETCH] All providers failed. Soft-skip this run. Last: {last_err}")
        return None

    @task(task_id="store_in_neon_db")
    def store_in_neon_db(record: dict):
        """Insert BTC tick into raw_market_ticks."""
        if not record:
            print("[DB] No fresh record fetched. Skipping DB write.")
            return {"status": "skipped", "reason": "no_record"}

        db_url = _db_url()
        print(f"[DB] Storing tick | source={record.get('source')} | close=${record['close_price']}")
        conn = None
        try:
            conn = psycopg2.connect(db_url, connect_timeout=15)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_market_ticks (
                    id           SERIAL PRIMARY KEY,
                    timestamp    TIMESTAMP NOT NULL,
                    open_price   NUMERIC(20,8),
                    high_price   NUMERIC(20,8),
                    low_price    NUMERIC(20,8),
                    close_price  NUMERIC(20,8),
                    volume       NUMERIC(30,8),
                    trades_count INTEGER DEFAULT 0,
                    created_at   TIMESTAMP DEFAULT NOW(),
                    UNIQUE(timestamp)
                );
                """
            )
            cur.execute(
                """
                ALTER TABLE raw_market_ticks
                ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'binance.com';
                """
            )
            conn.commit()
            cur.execute(
                """
                INSERT INTO raw_market_ticks
                    (timestamp, open_price, high_price, low_price, close_price, volume, trades_count, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (timestamp) DO UPDATE SET
                    open_price   = EXCLUDED.open_price,
                    high_price   = EXCLUDED.high_price,
                    low_price    = EXCLUDED.low_price,
                    close_price  = EXCLUDED.close_price,
                    volume       = EXCLUDED.volume,
                    trades_count = EXCLUDED.trades_count,
                    source       = EXCLUDED.source;
                """,
                (
                    record["timestamp"],
                    record["open_price"],
                    record["high_price"],
                    record["low_price"],
                    record["close_price"],
                    record["volume"],
                    record["trades_count"],
                    record.get("source", "unknown"),
                ),
            )
            conn.commit()
            print(f"[DB] Tick stored at {record['timestamp']}")
            return {"status": "ok", "timestamp": record["timestamp"]}
        except Exception as e:
            print(f"[DB] ERROR: {e}")
            raise
        finally:
            if conn and not conn.closed:
                conn.close()

    fetched = fetch_binance_data()
    stored = store_in_neon_db(fetched)

    trigger = TriggerDagRunOperator(
        task_id="trigger_feature_engineering",
        trigger_dag_id="cryptopulse_feature_engineering",
        wait_for_completion=False,
        reset_dag_run=False,
    )
    stored >> trigger
