import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

url = os.environ.get("NEON_DATABASE_URL", "postgresql://neondb_owner:YOUR_NEON_DATABASE_PASSWORD@ep-flat-shadow-aq6optjf-pooler.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
conn = psycopg2.connect(url)
cur  = conn.cursor()

# Check columns
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='engineered_features' ORDER BY ordinal_position;")
cols = cur.fetchall()
print("COLUMNS:", cols)

# Check data
cur.execute("SELECT COUNT(*) FROM engineered_features WHERE target_label IS NOT NULL;")
cnt = cur.fetchone()[0]
print("Labeled rows:", cnt)

# Check if next_close_price column exists
col_names = [c[0] for c in cols]
print("Has next_close_price:", "next_close_price" in col_names)

# Add column if missing
if "next_close_price" not in col_names:
    cur.execute("ALTER TABLE engineered_features ADD COLUMN next_close_price NUMERIC(20,8);")
    conn.commit()
    print("Added next_close_price column!")

    # Populate it
    cur.execute("""
        UPDATE engineered_features ef
        SET next_close_price = (
            SELECT ef2.close_price
            FROM engineered_features ef2
            WHERE ef2.timestamp > ef.timestamp
            ORDER BY ef2.timestamp ASC
            LIMIT 1
        );
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM engineered_features WHERE next_close_price IS NOT NULL;")
    filled = cur.fetchone()[0]
    print("Populated next_close_price for", filled, "rows")
else:
    cur.execute("SELECT COUNT(*) FROM engineered_features WHERE next_close_price IS NOT NULL;")
    filled = cur.fetchone()[0]
    print("next_close_price already exists, filled rows:", filled)

conn.close()
print("DONE")
