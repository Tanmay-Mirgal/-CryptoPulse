import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

url = os.environ.get("NEON_DATABASE_URL", "")
if not url:
    raise RuntimeError("NEON_DATABASE_URL is required")
conn = psycopg2.connect(url)
cur  = conn.cursor()

cur.execute("""
    SELECT timestamp, close_price, rsi_14, macd, macd_signal, ma_short, ma_long
    FROM engineered_features
    WHERE rsi_14 IS NOT NULL AND macd IS NOT NULL
    ORDER BY timestamp DESC LIMIT 1;
""")
r = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM raw_market_ticks;")
raw = cur.fetchone()[0]

cur.execute("SELECT accuracy, f1_score, model_version FROM model_metrics ORDER BY evaluation_time DESC LIMIT 1;")
m = cur.fetchone()
conn.close()

if r:
    ts, price, rsi, macd, macd_sig, ma_s, ma_l = r
    price = float(price)
    rsi   = float(rsi)
    macd  = float(macd)
    ms    = float(macd_sig)
    ma_s  = float(ma_s)
    ma_l  = float(ma_l)

    print("=== LATEST DATA (" + str(ts) + ") ===")
    print("Price   : " + str(round(price, 2)))
    print("RSI(14) : " + str(round(rsi, 2)))
    print("MACD    : " + str(round(macd, 4)) + "  Signal: " + str(round(ms, 4)))
    print("MA Short: " + str(round(ma_s, 2)) + "  MA Long: " + str(round(ma_l, 2)))
    print("")

    score   = 0
    reasons = []
    if rsi < 40:
        score += 2; reasons.append("RSI=" + str(round(rsi,1)) + " OVERSOLD -> BUY signal")
    elif rsi > 60:
        score -= 2; reasons.append("RSI=" + str(round(rsi,1)) + " OVERBOUGHT -> SELL signal")
    else:
        reasons.append("RSI=" + str(round(rsi,1)) + " NEUTRAL zone")

    if macd > ms:
        score += 1; reasons.append("MACD > Signal -> Bullish momentum")
    else:
        score -= 1; reasons.append("MACD < Signal -> Bearish momentum")

    if ma_s > ma_l:
        score += 1; reasons.append("MA Short > MA Long -> Uptrend")
    else:
        score -= 1; reasons.append("MA Short < MA Long -> Downtrend")

    if score >= 2:
        sig  = "BUY"
        conf = min(55 + score * 5, 90)
    elif score <= -2:
        sig  = "SELL"
        conf = min(55 + abs(score) * 5, 90)
    else:
        sig  = "HOLD"
        conf = 50

    direction    = 1 if sig == "BUY" else (-1 if sig == "SELL" else 0)
    target_price = round(price * (1 + direction * 0.003), 2)

    print("=== PREDICTION: " + sig + " ===")
    print("Confidence  : " + str(conf) + "%")
    print("Target Price: " + str(target_price))
    print("Score       : " + str(score) + " / 4")
    print("")
    print("Reasoning:")
    for reason in reasons:
        print("  -> " + reason)

print("")
if m:
    print("=== MODEL PERFORMANCE ===")
    print("Accuracy : " + str(round(float(m[0]) * 100, 1)) + "%")
    print("F1 Score : " + str(round(float(m[1]) * 100, 1)) + "%")
    print("Run ID   : " + str(m[2])[:20])
print("Raw Ticks: " + str(raw))
