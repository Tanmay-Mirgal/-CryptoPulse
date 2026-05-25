import os
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

url = os.environ.get("NEON_DATABASE_URL", "postgresql://neondb_owner:YOUR_NEON_DATABASE_PASSWORD@ep-flat-shadow-aq6optjf-pooler.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
conn = psycopg2.connect(url, connect_timeout=10, cursor_factory=psycopg2.extras.RealDictCursor)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) as c FROM raw_market_ticks;"); print("raw_ticks:", cur.fetchone()["c"])
cur.execute("SELECT COUNT(*) as c FROM engineered_features WHERE rsi_14 IS NOT NULL;"); print("feat_rows:", cur.fetchone()["c"])
cur.execute("SELECT COUNT(*) as c FROM engineered_features WHERE next_close_price IS NOT NULL;"); print("with_reg:", cur.fetchone()["c"])
cur.execute("SELECT timestamp,close_price,rsi_14,macd,macd_signal,ma_short,ma_long,target_label,next_close_price FROM engineered_features WHERE rsi_14 IS NOT NULL ORDER BY timestamp DESC LIMIT 1;")
r = cur.fetchone(); print("latest:", dict(r))
cur.execute("SELECT accuracy,f1_score FROM model_metrics ORDER BY evaluation_time DESC LIMIT 1;")
m = cur.fetchone(); print("model:", dict(m) if m else "NO MODEL YET")
conn.close()
