from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_no_hardcoded_neon_database_connection_string():
    disallowed_fragments = (
        "postgresql://neondb_owner:",
        "ep-flat-shadow-aq6optjf",
    )

    scanned_files = [
        REPO_ROOT / "dashboard" / "app.py",
        REPO_ROOT / "dashboard" / "check_pred.py",
        REPO_ROOT / "dashboard" / "chk.py",
        REPO_ROOT / "dashboard" / "db_migrate.py",
        REPO_ROOT / "dags" / "dag_crypto_ingestion.py",
        REPO_ROOT / "dags" / "dag_feature_engineering.py",
        REPO_ROOT / "dags" / "dag_model_training.py",
        REPO_ROOT / "README.md",
        REPO_ROOT / "DEPLOY.md",
        REPO_ROOT / "airflow_settings.yaml",
    ]

    for file_path in scanned_files:
        content = file_path.read_text(encoding="utf-8")
        for fragment in disallowed_fragments:
            assert fragment not in content, f"Found hardcoded fragment '{fragment}' in {file_path}"
