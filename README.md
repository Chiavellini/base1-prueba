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

You should see 7 containers. Wait until STATUS shows "healthy" for all.

## 3. Initial Setup (First Time Only)

These steps create the database tables and load 1 year of historical data. You only need to do this once.

**Step 1** — Open Airflow in your browser:
- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

**Step 2** — Trigger the setup DAGs:
- Find `market_data_create_wide` in the list
- Click the **play button** on the right
- Click **Trigger DAG**
- Repeat for `market_data_create_adjusted`
- Wait for both to finish (green circle = success, takes 5-10 minutes)

**Step 3** — Enable automatic updates:
- Find `market_data_ingest_wide` in the list
- Toggle the switch to **ON** (blue)
- Repeat for `market_data_ingest_adjusted`

Done! From now on, prices update automatically every minute during market hours.

## 4. After Initial Setup (Normal Usage)

Once you've completed the initial setup, you never need to do it again.

**To start the project:**
```bash
docker-compose up -d
```

The ingest DAGs will automatically resume updating prices during market hours (Mon-Fri, 8:30 AM - 3:00 PM Mexico City). No manual steps needed.

**To stop the project:**
```bash
docker-compose stop
```

Your data is saved. The project will continue where it left off when you start it again.

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
| Tables disappeared after renaming/moving folder | See Section 7 below |

## 7. Recovering Data After Renaming the Project Folder

If you rename or move the project folder, your tables may appear empty. The data is NOT lost — it's still in the old Docker volume.

**Step 1** — Find the old volume:
```bash
docker volume ls
```
Look for a volume with your **old folder name**, like `old-folder-name_postgres-db-volume`.

**Step 2** — Copy the data to the current volume:
```bash
docker-compose stop
```
```bash
docker run --rm \
  -v OLD_VOLUME_NAME:/source:ro \
  -v airflow_market_data_postgres:/dest \
  alpine sh -c "cp -av /source/. /dest/"
```
Replace `OLD_VOLUME_NAME` with the actual name from Step 1.

**Step 3** — Restart and verify:
```bash
docker-compose up -d
```
Wait 2 minutes, then check your tables in TablePlus. All data should be back.