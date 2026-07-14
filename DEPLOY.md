# 🚀 Astronomer Cloud Deployment Guide

## Deploy Airflow DAGs to Astronomer Cloud

### Step 1 — Astronomer CLI Install
```powershell
winget install Astronomer.Astro
```

### Step 2 — Login to Astronomer Cloud
```bash
astro login
# Browser mein login karo
```

### Step 3 — Deploy (from project root)
```bash
astro deploy
# Select your Deployment from the list
```

### Step 4 — Environment Variables (Astronomer UI mein set karo)
Astronomer Cloud → Deployments → Your Deployment → Variables:

| Key | Value |
|-----|-------|
| `NEON_DATABASE_URL` | `postgresql://<user>:<password>@<host>:5432/<database>?sslmode=require` |
| `DAGSHUB_USERNAME` | `Tanmay-Mirgal` |
| `DAGSHUB_TOKEN` | `302e28608...` |
| `DAGSHUB_REPO_OWNER` | `Tanmay-Mirgal` |
| `DAGSHUB_REPO_NAME` | `CryptoPulse` |

### Step 5 — Flask Dashboard (Local — always runs locally)
```bash
python dashboard/app.py
# Visit: http://localhost:5050
```

> Flask dashboard locally chalega aur Neon DB se data fetch karega.
> Airflow DAGs Astronomer Cloud pe run honge.
> Dono ek hi Neon DB use karte hain — so everything stays in sync!

---

## Pipeline Architecture

```
[Astronomer Cloud]                    [Local]
 Every 1 min:                         Flask Dashboard
 dag_crypto_ingestion                 → WebSocket (Binance Live)
   └─ TriggerDagRun ──────────▶       → /api/live (Neon DB)
 dag_feature_engineering              → /api/mlflow-runs (DagsHub)
                                       → /api/pipeline-status
 Every 6 hrs:
 dag_model_training
   └─ Classifier (BUY/SELL)
   └─ Regressor (Price)
   └─ MLflow → DagsHub
```

---

## Local Development (Docker)
```bash
astro dev start   # Airflow UI: http://localhost:8080
python dashboard/app.py  # Dashboard: http://localhost:5050
```

---

## ☁️ Free Python App Deployment (Render.com)

You can deploy the **Flask + Socket.IO Dashboard** for free on **Render.com** by following these steps:

### Step 1 — Create a Render Account
1. Go to [Render.com](https://render.com) and sign up for a free account.
2. Connect your GitHub account.

### Step 2 — Create a Web Service
1. Click the **New** button and select **Web Service**.
2. Connect your public GitHub repository: `https://github.com/Tanmay-Mirgal/-CryptoPulse.git`.

### Step 3 — Configure Build & Start Settings
Configure your service with the following settings:
- **Runtime**: `Python`
- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `gunicorn --worker-class eventlet -w 1 dashboard.app:app`
- **Instance Type**: `Free`

### Step 4 — Configure Environment Variables
To keep your secrets safe, **never** hardcode them. Add them securely via Render's **Environment** dashboard:
1. Under the **Environment** tab, click **Add Environment Variable**.
2. Add the following keys exactly as they appear in your local `.env` file:

| Key | Value | Description |
|---|---|---|
| `NEON_DATABASE_URL` | `postgresql://<user>:<password>@<host>:5432/<database>?sslmode=require` | Your Neon database connection string |
| `DAGSHUB_USERNAME` | `Tanmay-Mirgal` | Your DagsHub username |
| `DAGSHUB_TOKEN` | `your_dagshub_token` | Your DagsHub access token |
| `PORT` | `5050` | The port your Flask app runs on (dynamically resolved) |

### Step 5 — Deploy!
Click **Deploy Web Service**. Once the build finishes, your dashboard will be live at `https://your-app-name.onrender.com`!
