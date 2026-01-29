# Financial Data Pipeline

Automated stock price collection for 153 tickers using Apache Airflow and PostgreSQL.

## 1. Install Required Software

Two programs need to be installed:

**1. Docker Desktop** (runs the project)
- Download: https://www.docker.com/products/docker-desktop/
- Install and open it
- Wait until the whale icon in the menu bar stops animating

**2. TablePlus** (view your data — optional but recommended)
- Download: https://tableplus.com/download
- Install it (free version works fine)

## 2. Run the Project

Open Terminal and run these commands one at a time:

**Step 1** — Go to the project folder:
```bash
cd /path/to/your/project/folder
```
*(replace with your actual project path)*

**Step 2** — Build the Docker image (takes 2-5 minutes):
```bash
docker build -t extending-airflow:3.1.1 .
```

**Step 3** — Start the project:
```bash
docker-compose up -d
```

**Step 4** — Wait 2 minutes, then check it's running:
```bash
docker ps
```

You should see 6 containers. Wait until STATUS shows "healthy" for all.

## 3. Initial Setup (First Time Only)

These steps create tables and load 1 year of historical data. **You only do this once, ever.** After this, everything is automatic.

**Step 1** — Open Airflow in your browser:
- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

**Step 2** — Trigger the setup DAGs:
- Find `market_data_create_wide` in the list
- Click the **play button** on the right
- Select **Single run**
- Click **Trigger**
- Repeat for `market_data_create_adjusted`
- Wait for both to finish (green circle = success, takes 5-10 minutes)

**Step 3** — Enable automatic updates:
- Find `market_data_ingest_wide` → Toggle switch to **ON** (blue)
- Find `market_data_ingest_adjusted` → Toggle switch to **ON** (blue)
- Find `market_data_refresh_adjusted` → Toggle switch to **ON** (blue)

**Done!** These settings are saved permanently. You never need to repeat these steps.

## 4. Daily Usage

After initial setup, running the project is simple:

**Start:**
```bash
docker-compose up -d
```

**Stop:**
```bash
docker-compose stop
```

That's it. No manual steps needed. The DAGs automatically update prices every minute during market hours (Mon-Fri, 8:30 AM - 3:00 PM Mexico City).

## 5. View Your Data in TablePlus

**Step 1** — Open TablePlus

**Step 2** — Create a new connection:
- Click **Create a new connection** (or press Cmd+N)
- Select **PostgreSQL**

**Step 3** — Enter these settings EXACTLY:

| Field | Value |
|-------|-------|
| Name | Market Data (or anything) |
| Host | localhost |
| Port | **5434** (NOT 5432!) |
| User | airflow |
| Password | airflow |
| Database | airflow |

**Step 4** — Test and connect:
- Click **Test** (should show "OK")
- Click **Connect**

**Step 5** — Find your tables:
- In the left sidebar, find the dropdown that says `public`
- Change it to **`market_data`**
- You will see 6 tables with your stock data

## 6. Troubleshooting

| Problem | Fix |
|---------|-----|
| "Cannot connect to Docker daemon" | Open Docker Desktop and wait 30 seconds |
| Containers not showing "healthy" | Wait 2-3 minutes, Docker is starting up |
| Tables are empty in TablePlus | Trigger `market_data_create_wide` DAG in Airflow UI |
| Can't connect to port 5434 | Run `docker ps` to check containers are running |
| "repository does not exist" error | Run `docker build -t extending-airflow:3.1.1 .` |
| DAGs not updating prices | Check they are unpaused (toggle ON) in Airflow UI |