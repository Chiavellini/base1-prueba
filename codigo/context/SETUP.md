# Setup & Run Guide

Complete instructions for setting up and running the Apache Airflow Financial Data Pipeline.

## Prerequisites

- Docker Desktop installed and running
- ~4GB RAM available for containers
- Terminal access (macOS Terminal, iTerm, etc.)

## Initial Setup (One-Time)

### Step 1: Navigate to Project Directory

```bash
cd /Users/bernardodelrio/Desktop/Actinver/Apache-Airflow-3.1.1-for-HOCV-Stocks-with-Yahoo-Finance-API-main
```

### Step 2: Build the Custom Airflow Image

```bash
docker build -t extending-airflow:3.1.1 .
```

This creates a custom image with:
- Apache Airflow 3.1.1
- yfinance, pandas, psycopg2-binary
- All required providers

### Step 3: Start All Containers

```bash
docker-compose up -d
```

### Step 4: Wait for Healthy Status

```bash
docker-compose ps
```

Wait until all 7 containers show `(healthy)` status (typically 1-2 minutes):
- `airflow-scheduler`
- `airflow-apiserver`
- `airflow-dag-processor`
- `airflow-worker`
- `airflow-triggerer`
- `postgres`
- `redis`

### Step 5: Create the Closing Prices Table

```bash
docker-compose exec -T postgres psql -U airflow -d airflow -f /opt/airflow/scripts/create_closing_prices.sql
```

Or via Airflow (unpause and trigger):
```bash
docker-compose exec airflow-scheduler airflow dags unpause market_data_create_wide
docker-compose exec airflow-scheduler airflow dags trigger market_data_create_wide
```

### Step 6: Enable the Ingest DAG

```bash
docker-compose exec airflow-scheduler airflow dags unpause market_data_ingest_wide
```

The DAG will automatically run every 30 minutes during NYSE market hours (14:30-21:00 UTC, weekdays).

## Verifying the Setup

### Check DAG Status

```bash
docker-compose exec airflow-scheduler airflow dags list | grep wide
```

Expected output:
```
market_data_create_wide     | ... | False
market_data_ingest_wide     | ... | False
```

### Check Table Data

```bash
docker-compose exec postgres psql -U airflow -d airflow -c \
  "SELECT date, sp500, aapl, msft, googl FROM market_data.closing_prices ORDER BY date DESC LIMIT 5;"
```

### Airflow Web UI

| Setting  | Value                   |
|----------|-------------------------|
| URL      | http://localhost:8080   |
| Username | `airflow`               |
| Password | `airflow`               |

## Database Access

### Connection Details (for TablePlus, DBeaver, pgAdmin)

| Setting  | Value      |
|----------|------------|
| Host     | `localhost`|
| Port     | `5434`     |
| User     | `airflow`  |
| Password | `airflow`  |
| Database | `airflow`  |

**Connection string:**
```
postgresql://airflow:airflow@localhost:5434/airflow
```

### Command Line Access

```bash
docker-compose exec postgres psql -U airflow -d airflow
```

## Database Architecture

### Wide-Format Table: `market_data.closing_prices`

Single table with one row per trading day and 153 ticker columns:

```
| date       | sp500   | nasdaq100 | dow     | aapl   | msft   | ... |
|------------|---------|-----------|---------|--------|--------|-----|
| 2026-01-27 | 6981.84 | 25947.37  | 48925.66| 260.47 | 481.08 | ... |
| 2026-01-24 | 6901.55 | 25433.25  | 48123.45| 258.22 | 476.33 | ... |
```

### Useful Queries

```sql
-- View latest prices for major tickers
SELECT date, sp500, nasdaq100, dow, aapl, msft, nvda, googl, amzn, meta
FROM market_data.closing_prices
ORDER BY date DESC
LIMIT 10;

-- Get price for specific ticker and date
SELECT date, tsla
FROM market_data.closing_prices
WHERE date = '2026-01-27';

-- Check data completeness (count non-null values)
SELECT date,
       COUNT(*) FILTER (WHERE aapl IS NOT NULL) as has_data
FROM market_data.closing_prices
GROUP BY date
ORDER BY date DESC;

-- Total rows in table
SELECT COUNT(*) as total_days FROM market_data.closing_prices;
```

## DAGs Reference

| DAG ID                    | Purpose                              | Schedule         | Notes                           |
|---------------------------|--------------------------------------|------------------|---------------------------------|
| `market_data_create_wide` | Create closing_prices table          | `@once`          | Run once on initial setup       |
| `market_data_ingest_wide` | Update closing prices (all tickers)  | `*/30 * * * *`   | Only runs during market hours   |

### Market Hours Logic

- **NYSE Hours:** 9:30 AM - 4:00 PM Eastern
- **In UTC:** 14:30 - 21:00
- **Weekend Skip:** Saturday and Sunday automatically skipped

The `market_data_ingest_wide` DAG:
1. Triggers every 30 minutes (24/7)
2. First task checks if within market hours
3. If outside hours → skips all downstream tasks
4. If within hours → fetches and upserts prices for all 153 tickers

## Daily Operations

### Starting Containers

```bash
cd /Users/bernardodelrio/Desktop/Actinver/Apache-Airflow-3.1.1-for-HOCV-Stocks-with-Yahoo-Finance-API-main
docker-compose up -d
```

### Stopping Containers (Preserves Data)

```bash
docker-compose down
```

### Stopping and Deleting All Data

```bash
docker-compose down -v
```

### Manually Trigger Data Update

```bash
docker-compose exec airflow-scheduler airflow dags trigger market_data_ingest_wide
```

### View DAG Run History

```bash
docker-compose exec postgres psql -U airflow -d airflow -c \
  "SELECT dag_id, run_id, state, start_date FROM dag_run WHERE dag_id='market_data_ingest_wide' ORDER BY start_date DESC LIMIT 10;"
```

## Troubleshooting

### "extending-airflow" Image Not Found

```bash
docker build -t extending-airflow:3.1.1 .
```

### Containers Not Starting

```bash
docker-compose logs --tail=50
docker-compose down && docker-compose up -d
```

### DAG Not Visible in UI

```bash
docker-compose exec airflow-scheduler airflow dags list-import-errors
```

### No Data in Table

1. Verify DAG is unpaused:
   ```bash
   docker-compose exec airflow-scheduler airflow dags list | grep ingest_wide
   ```

2. Check if within market hours (14:30-21:00 UTC, weekdays):
   ```bash
   date -u
   ```

3. Manually trigger:
   ```bash
   docker-compose exec airflow-scheduler airflow dags trigger market_data_ingest_wide
   ```

### Check Scheduler Logs

```bash
docker-compose logs airflow-scheduler --tail=100
```

## File Structure

```
├── context/
│   ├── README.md           # Project overview (legacy)
│   └── SETUP.md            # This file
├── dags/
│   └── market_data/
│       ├── create_wide_table.py   # Creates closing_prices table
│       └── ingest_wide.py         # Updates prices every 30 min
├── plugins/
│   └── market_data_wide.py        # Core functions for wide-format table
├── data/
│   └── info/
│       └── tickers_list.csv       # 153 tracked tickers
├── scripts/
│   └── run_backfill.py            # Standalone backfill script
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```

## Version Info

- **Airflow:** 3.1.1
- **PostgreSQL:** 16
- **Python:** 3.12
- **Key packages:** yfinance, pandas, psycopg2-binary
