"""
CryptoPulse Dashboard — Flask + SocketIO
Live BTC/USDT from Binance WebSocket → push to browser
All predictions from Neon DB
"""
import os, json, threading, time
import psycopg2, psycopg2.extras
from datetime import datetime, timezone
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

app      = Flask(__name__)
app.config["SECRET_KEY"] = "cp-x9k2m-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=False, engineio_logger=False)

DB_URL = (
    os.environ.get("NEON_DATABASE_URL") or
    "postgresql://neondb_owner:YOUR_NEON_DATABASE_PASSWORD@ep-flat-shadow-aq6optjf-pooler.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require"
)
DB_URL = DB_URL.replace("&channel_binding=require", "").replace("?channel_binding=require", "")

DAGSHUB_USER  = os.environ.get("DAGSHUB_USERNAME",  "Tanmay-Mirgal")
DAGSHUB_TOKEN = os.environ.get("DAGSHUB_TOKEN",     "YOUR_DAGSHUB_TOKEN")
MLFLOW_URI    = "https://dagshub.com/Tanmay-Mirgal/CryptoPulse.mlflow"

# ─── DB ───────────────────────────────────────────────────────────────────────
def db():
    return psycopg2.connect(DB_URL, connect_timeout=10,
                            cursor_factory=psycopg2.extras.RealDictCursor)

# ─── Binance WebSocket Thread ──────────────────────────────────────────────────
def _binance_thread():
    import websocket
    def on_msg(ws, msg):
        try:
            d = json.loads(msg)
            socketio.emit("ticker", {
                "price":  float(d.get("c", 0)),
                "chg24":  float(d.get("P", 0)),
                "high":   float(d.get("h", 0)),
                "low":    float(d.get("l", 0)),
                "vol":    float(d.get("v", 0)),
                "ts":     datetime.now(timezone.utc).strftime("%H:%M:%S"),
            })
        except Exception as e:
            print(f"[WS parse] {e}")

    def on_error(ws, err): print(f"[WS error] {err}")
    def on_close(ws, *a):
        print("[WS closed] reconnecting in 5s...")
        time.sleep(5); _connect()

    def _connect():
        ws = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/ws/btcusdt@miniTicker",
            on_message=on_msg, on_error=on_error, on_close=on_close)
        ws.run_forever(ping_interval=25, ping_timeout=10)

    while True:
        try: _connect()
        except Exception as e:
            print(f"[WS thread] {e}"); time.sleep(10)

def start_binance():
    t = threading.Thread(target=_binance_thread, daemon=True)
    t.start()
    print("[WS] Binance thread started OK")

# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/live")
def api_live():
    try:
        conn = db(); cur = conn.cursor()

        # Latest features
        cur.execute("""
            SELECT timestamp, close_price, rsi_14, macd, macd_signal,
                   ma_short, ma_long, target_label, next_close_price
            FROM engineered_features
            WHERE rsi_14 IS NOT NULL AND macd IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1;
        """)
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({"ok": False, "error": "No engineered data yet — DAG running soon"}), 200

        # Chart: last 60
        cur.execute("""
            SELECT timestamp, close_price, rsi_14, macd, macd_signal,
                   ma_short, ma_long, target_label, next_close_price
            FROM engineered_features
            WHERE rsi_14 IS NOT NULL
            ORDER BY timestamp DESC LIMIT 60;
        """)
        chart_rows = list(reversed(cur.fetchall()))

        # Counts
        cur.execute("SELECT COUNT(*) as c FROM raw_market_ticks;")
        raw_cnt = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM engineered_features WHERE rsi_14 IS NOT NULL;")
        feat_cnt = cur.fetchone()["c"]
        cur.execute("SELECT MAX(timestamp) as t FROM raw_market_ticks;")
        last_tick = cur.fetchone()["t"]

        # Model metrics
        cur.execute("SELECT accuracy, f1_score, model_version, evaluation_time FROM model_metrics ORDER BY evaluation_time DESC LIMIT 1;")
        mrow = cur.fetchone()
        conn.close()

        price   = float(row["close_price"])
        rsi     = float(row["rsi_14"])
        macd_v  = float(row["macd"])
        macd_s  = float(row["macd_signal"])
        ma_s    = float(row["ma_short"])
        ma_l    = float(row["ma_long"])
        reg_p   = float(row["next_close_price"]) if row["next_close_price"] else price

        # Signal logic
        score = 0
        if rsi < 40:        score += 2
        elif rsi > 60:      score -= 2
        if macd_v > macd_s: score += 1
        else:               score -= 1
        if ma_s > ma_l:     score += 1
        else:               score -= 1

        if score >= 2:    sig, conf = "BUY",  min(55 + score * 5, 92)
        elif score <= -2: sig, conf = "SELL", min(55 + abs(score) * 5, 92)
        else:             sig, conf = "HOLD", 52

        diff     = round(reg_p - price, 2)
        diff_pct = round((diff / price) * 100, 4) if price else 0

        # 1h change
        chg1h = 0
        if len(chart_rows) >= 2:
            fp = float(chart_rows[0]["close_price"])
            lp = float(chart_rows[-1]["close_price"])
            chg1h = round(((lp - fp) / fp) * 100, 3) if fp else 0

        chart = [{
            "t":   r["timestamp"].strftime("%H:%M"),
            "p":   float(r["close_price"]),
            "ms":  float(r["ma_short"])    if r["ma_short"]    else None,
            "ml":  float(r["ma_long"])     if r["ma_long"]     else None,
            "rsi": float(r["rsi_14"])      if r["rsi_14"]      else None,
            "mc":  float(r["macd"])        if r["macd"]        else None,
            "mcs": float(r["macd_signal"]) if r["macd_signal"] else None,
            "rp":  float(r["next_close_price"]) if r["next_close_price"] else None,
            "sig": r["target_label"],
        } for r in chart_rows]

        return jsonify({
            "ok": True,
            "ts":    row["timestamp"].isoformat(),
            "price": price,
            "chg1h": chg1h,
            "ind": {
                "rsi":     round(rsi, 2),
                "macd":    round(macd_v, 4),
                "macd_s":  round(macd_s, 4),
                "ma_s":    round(ma_s, 2),
                "ma_l":    round(ma_l, 2),
            },
            "clf": {
                "signal": sig, "conf": conf, "score": score,
                "rsi_zone":  "OVERSOLD" if rsi < 40 else "OVERBOUGHT" if rsi > 60 else "NEUTRAL",
                "macd_bull": macd_v > macd_s,
                "ma_up":     ma_s > ma_l,
            },
            "reg": {
                "price":    round(reg_p, 2),
                "diff":     diff,
                "diff_pct": diff_pct,
            },
            "model": {
                "acc":        round(float(mrow["accuracy"]) * 100, 1) if mrow else None,
                "f1":         round(float(mrow["f1_score"]) * 100, 1) if mrow else None,
                "run_id":     str(mrow["model_version"])[:14] if mrow else None,
                "trained_at": mrow["evaluation_time"].strftime("%d %b %H:%M") if mrow else None,
            },
            "db": {
                "raw":       raw_cnt,
                "feat":      feat_cnt,
                "last_tick": last_tick.strftime("%H:%M:%S") if last_tick else "N/A",
            },
            "chart": chart,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/history")
def api_history():
    try:
        conn = db(); cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, close_price, rsi_14, macd, ma_short, ma_long,
                   target_label, next_close_price
            FROM engineered_features
            WHERE rsi_14 IS NOT NULL
            ORDER BY timestamp DESC LIMIT 100;
        """)
        rows = cur.fetchall(); conn.close()
        return jsonify({"ok": True, "count": len(rows), "rows": [{
            "time":   r["timestamp"].strftime("%m/%d %H:%M"),
            "price":  float(r["close_price"]),
            "rsi":    round(float(r["rsi_14"]), 2) if r["rsi_14"] else None,
            "macd":   round(float(r["macd"]), 4)   if r["macd"]   else None,
            "ma_s":   round(float(r["ma_short"]), 0) if r["ma_short"] else None,
            "ma_l":   round(float(r["ma_long"]), 0)  if r["ma_long"]  else None,
            "sig":    "BUY" if r["target_label"] == 1 else "SELL",
            "pred":   round(float(r["next_close_price"]), 2) if r["next_close_price"] else None,
        } for r in rows]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


cached_runs = []
cached_runs_lock = threading.Lock()

def _mlflow_fetcher_thread():
    global cached_runs
    import requests as req
    auth = (DAGSHUB_USER, DAGSHUB_TOKEN)
    base = MLFLOW_URI + "/api/2.0/mlflow"
    
    while True:
        try:
            er = req.get(f"{base}/experiments/get-by-name",
                         params={"experiment_name": "CryptoPulse-XGBoost-Trading"},
                         auth=auth, timeout=12)
            if er.status_code == 200:
                eid = er.json()["experiment"]["experiment_id"]
                rr = req.post(f"{base}/runs/search",
                              json={"experiment_ids": [eid], "max_results": 20,
                                    "order_by": ["attribute.start_time DESC"]},
                              auth=auth, timeout=15)
                if rr.status_code == 200:
                    out = []
                    for r in rr.json().get("runs", []):
                        m_data = r["data"].get("metrics", [])
                        p_data = r["data"].get("params", [])
                        
                        if isinstance(m_data, list):
                            m = {item["key"]: float(item["value"]) for item in m_data if isinstance(item, dict) and "key" in item and "value" in item}
                        elif isinstance(m_data, dict):
                            m = m_data
                        else:
                            m = {}
                            
                        if isinstance(p_data, list):
                            p = {item["key"]: str(item["value"]) for item in p_data if isinstance(item, dict) and "key" in item and "value" in item}
                        elif isinstance(p_data, dict):
                            p = p_data
                        else:
                            p = {}
                            
                        t = p.get("model_type", "classifier")
                        
                        def to_float(val, default=0.0):
                            try: return float(val) if val is not None else default
                            except: return default

                        out.append({
                            "id":   r["info"]["run_id"][:10],
                            "name": r["info"].get("run_name", "run"),
                            "type": t,
                            "date": datetime.fromtimestamp(r["info"]["start_time"]/1000).strftime("%m/%d %H:%M"),
                            "acc":  round(to_float(m.get("accuracy", 0)) * 100, 1),
                            "f1":   round(to_float(m.get("f1_score", 0)) * 100, 1),
                            "auc":  round(to_float(m.get("auc_roc", 0)), 3),
                            "mae":  round(to_float(m.get("mae", 0)), 2),
                            "mape": round(to_float(m.get("mape", 0)), 2),
                            "r2":   round(to_float(m.get("r2", 0)), 4),
                        })
                    
                    with cached_runs_lock:
                        cached_runs = out
                    print(f"[MLflow Cache] Updated cache with {len(out)} runs successfully.")
                else:
                    print(f"[MLflow Cache Warning] Runs search failed with status {rr.status_code}")
            else:
                print(f"[MLflow Cache Warning] Experiment fetch failed with status {er.status_code}")
        except Exception as e:
            print(f"[MLflow Cache Error] {e}")
        
        time.sleep(60)

def start_mlflow_cache():
    t = threading.Thread(target=_mlflow_fetcher_thread, daemon=True)
    t.start()
    print("[MLflow] Background fetcher thread started OK")


@app.route("/api/runs")
def api_runs():
    with cached_runs_lock:
        runs = list(cached_runs)
    return jsonify({"ok": True, "runs": runs})


@app.route("/api/pipeline")
def api_pipeline():
    try:
        conn = db(); cur = conn.cursor()
        cur.execute("SELECT MAX(timestamp) as t, COUNT(*) as c FROM raw_market_ticks;")
        r = cur.fetchone()
        cur.execute("SELECT MAX(timestamp) as t, COUNT(*) as c FROM engineered_features;")
        f = cur.fetchone()
        cur.execute("SELECT MAX(evaluation_time) as t, COUNT(*) as c FROM model_metrics;")
        m = cur.fetchone()
        conn.close()

        def fmt(row):
            if not row or not row["t"]:
                return {"ts": "Never", "count": 0, "status": "idle", "age": 999}
            age = (datetime.utcnow() - row["t"].replace(tzinfo=None)).total_seconds() / 60
            return {"ts": row["t"].strftime("%H:%M:%S"), "count": int(row["c"]),
                    "status": "live" if age < 3 else "warn" if age < 15 else "idle",
                    "age": round(age, 1)}

        return jsonify({"ok": True,
                        "ingestion": fmt(r), "features": fmt(f), "model": fmt(m)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@socketio.on("connect")
def on_connect(): print("[WS] client connected")

if __name__ == "__main__":
    start_mlflow_cache()
    start_binance()
    port = int(os.environ.get("PORT", 5050))
    socketio.run(app, debug=False, host="0.0.0.0", port=port, allow_unsafe_werkzeug=True)
