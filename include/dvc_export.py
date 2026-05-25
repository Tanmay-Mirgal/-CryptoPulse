"""
CryptoPulse - DVC Data Export Script
=====================================
Yeh script Neon DB se data export karke DVC ke zariye
DagsHub pe data versioning karta hai.

Usage:
    .venv/Scripts/python.exe include/dvc_export.py

Kya karta hai:
  1. raw_market_ticks → data/raw_market_ticks.csv
  2. engineered_features → data/engineered_features.csv
  3. DVC add → tracking files banata hai
  4. Git + DVC commit aur push DagsHub pe
"""

import os
import subprocess
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ.get("NEON_DATABASE_URL", "")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def run(cmd: str, cwd: str = None) -> str:
    """Shell command run karo aur output return karo."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr and result.returncode != 0:
        print(f"[STDERR] {result.stderr.strip()}")
    return result.stdout.strip()


def export_table(conn, table: str, filename: str) -> int:
    """DB table ko CSV mein export karo."""
    df = pd.read_sql(f"SELECT * FROM {table} ORDER BY timestamp ASC;", conn)
    filepath = os.path.join(DATA_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"[INFO] Exported {len(df)} rows → {filepath}")
    return len(df)


def main():
    if not DB_URL:
        print("[ERROR] NEON_DATABASE_URL not set in .env")
        return

    project_root = os.path.dirname(os.path.dirname(__file__))

    # ── 1. Data directory banao ──
    os.makedirs(DATA_DIR, exist_ok=True)

    # ── 2. DB se export karo ──
    print("\n📦 Exporting data from Neon DB...")
    conn = psycopg2.connect(DB_URL)
    raw_count   = export_table(conn, "raw_market_ticks",    "raw_market_ticks.csv")
    feat_count  = export_table(conn, "engineered_features", "engineered_features.csv")
    conn.close()

    # ── 3. DVC add ──
    print("\n🔒 Adding data files to DVC...")
    run("dvc add data/raw_market_ticks.csv", cwd=project_root)
    run("dvc add data/engineered_features.csv", cwd=project_root)

    # ── 4. Git commit ──
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    print("\n📝 Git commit...")
    run("git add .", cwd=project_root)
    run(f'git commit -m "data: Export {raw_count} raw ticks + {feat_count} features [{timestamp}]"', cwd=project_root)

    # ── 5. DVC push (DagsHub S3-compatible storage) ──
    print("\n☁️  DVC push to DagsHub...")
    run("dvc push", cwd=project_root)

    # ── 6. Git push ──
    print("\n🚀 Git push to DagsHub...")
    run("git push origin main", cwd=project_root)

    print(f"\n✅ DagsHub Data Versioning Complete!")
    print(f"   raw_market_ticks:    {raw_count} rows")
    print(f"   engineered_features: {feat_count} rows")
    print(f"   View at: https://dagshub.com/Tanmay-Mirgal/CryptoPulse")


if __name__ == "__main__":
    main()
