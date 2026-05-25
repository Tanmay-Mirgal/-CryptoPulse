import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_database():
    """
    Connects to Neon Serverless PostgreSQL and initializes all tables required for the CryptoPulse pipeline.
    """
    db_url = os.getenv("NEON_DATABASE_URL")
    
    if not db_url or "YOUR_NEON_USER" in db_url:
        print("[ERROR] Neon Database URL is not configured or is using placeholders in .env file.")
        print("[INFO] Please edit the .env file and replace placeholders with your actual Neon database credentials.")
        return False
        
    conn = None
    try:
        print("Connecting to Neon PostgreSQL to initialize schemas...")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # 1. Raw tick table to store 1-minute candlesticks from Binance
        print("Creating table: raw_market_ticks...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_market_ticks (
            timestamp TIMESTAMP PRIMARY KEY,
            open_price NUMERIC(18, 8),
            high_price NUMERIC(18, 8),
            low_price NUMERIC(18, 8),
            close_price NUMERIC(18, 8),
            volume NUMERIC(18, 8),
            trades_count INT
        );
        """)
        
        # 2. Processed features table
        print("Creating table: engineered_features...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS engineered_features (
            timestamp TIMESTAMP PRIMARY KEY,
            close_price NUMERIC(18, 8),
            rsi_14 NUMERIC(10, 4),
            macd NUMERIC(10, 4),
            macd_signal NUMERIC(10, 4),
            ma_short NUMERIC(18, 8),
            ma_long NUMERIC(18, 8),
            target_label INT
        );
        """)
        
        # 3. Live predictions log
        print("Creating table: predictions_log...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS predictions_log (
            prediction_id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            input_price NUMERIC(18, 8),
            predicted_signal INT,
            confidence NUMERIC(5, 4),
            actual_signal INT DEFAULT NULL
        );
        """)
        
        # 4. Drift & Model performance metrics
        print("Creating table: model_metrics...")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS model_metrics (
            evaluation_time TIMESTAMP PRIMARY KEY,
            model_version VARCHAR(50),
            accuracy NUMERIC(5, 4),
            f1_score NUMERIC(5, 4),
            data_drift_psi NUMERIC(5, 4)
        );
        """)
        
        conn.commit()
        print("[SUCCESS] All database tables initialized successfully in Neon DB!")
        cur.close()
        return True
        
    except Exception as e:
        print(f"[ERROR] Database schema initialization failed: {e}")
        print("[INFO] Please verify your database connection credentials and network/IP settings in your Neon console.")
        return False
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_database()
