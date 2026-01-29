# Apache Airflow Financial Data Pipeline

## Overview

- **What:** Fetches stock prices from Yahoo Finance API
- **Storage:** PostgreSQL database with per-ticker tables
- **Schedule:** Daily EOD + 30-minute intraday updates during NYSE market hours

## Prerequisites

- Docker & Docker Compose installed
- ~4GB RAM available for containers
- (Optional) TablePlus, DBeaver, or pgAdmin for database viewing

## Quick Start

### 1. Build and Start the Project

```bash
cd /path/to/Apache-Airflow-3.1.1-for-HOCV-Stocks-with-Yahoo-Finance-API-main

# Build the custom Airflow image (required first time)
docker build -t extending-airflow .

# Start all services
docker-compose up -d
```

### 2. Verify Containers Are Healthy

```bash
docker ps
```

Wait until all 7 containers show "healthy" status (usually 1-2 minutes):
- `airflow-scheduler`
- `airflow-apiserver`
- `airflow-dag-processor`
- `airflow-worker`
- `airflow-triggerer`
- `postgres`
- `redis`

### 3. Access Airflow UI

| Setting | Value |
|---------|-------|
| URL | http://localhost:8080 |
| Username | `airflow` |
| Password | `airflow` |

### 4. Enable DAGs

New DAGs start paused. In the Airflow UI, toggle ON:

1. `market_data_create` (run once to create tables)
2. `market_data_ingest` (daily EOD data)
3. `market_data_intraday_update` (30-min updates during market hours)

## Project Structure

```
├── dags/
│   ├── market_data/
│   │   ├── create_tables.py      # One-time schema creation
│   │   ├── ingest.py             # Daily EOD ingestion (22:00 UTC)
│   │   └── intraday_update.py    # 30-min intraday updates
│   └── muestra/
│       ├── create_tables.py
│       └── ingest.py             # Excel file processing
├── plugins/
│   ├── market_data_shared.py     # Shared functions, INDEXES config
│   └── muestra_shared.py
├── data/
│   ├── info/tickers_list.csv     # Tracked tickers (153 symbols)
│   └── muestra/                  # Excel files for processing
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── context/
    └── README.md                 # This file
```

## DAGs Reference

| DAG ID | Purpose | Schedule | Notes |
|--------|---------|----------|-------|
| `market_data_create` | Create DB schema & tables | `@once` | Run first, manually |
| `market_data_ingest` | Daily EOD OHLCV data | `0 22 * * *` (22:00 UTC) | After market close |
| `market_data_intraday_update` | 30-min price updates | `*/30 * * * *` | NYSE hours only (14:30-21:00 UTC) |
| `muestra_create` | Create muestra schema | `@once` | For Excel processing |
| `muestra_ingest` | Process Excel files | `*/1 * * * *` | Scans for new files |

## Viewing the Data

### Option 1: Airflow UI

1. Go to http://localhost:8080
2. Click on a DAG → Graph view to see task status
3. Click on a task → Logs to see execution details

### Option 2: TablePlus / DBeaver / pgAdmin

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5434` |
| User | `airflow` |
| Password | `airflow` |
| Database | `airflow` |

**Connection string:**
```
postgresql://airflow:airflow@localhost:5434/airflow
```

> **Note:** Port is `5434`, not the default `5432`.

### Option 3: Command Line

```bash
# Enter PostgreSQL shell
docker-compose exec postgres psql -U airflow -d airflow

# Or run a single query
docker-compose exec postgres psql -U airflow -d airflow -c \
  "SELECT * FROM market_data.aapl ORDER BY ts DESC LIMIT 10;"
```

### Useful SQL Queries

```sql
-- List all ticker tables
SELECT tablename
FROM pg_tables
WHERE schemaname = 'market_data'
ORDER BY tablename;

-- View recent AAPL data
SELECT ts, open, high, low, close, volume
FROM market_data.aapl
ORDER BY ts DESC
LIMIT 20;

-- Check data freshness (latest timestamp per ticker)
SELECT 'aapl' as ticker, MAX(ts) as latest FROM market_data.aapl
UNION ALL
SELECT 'msft', MAX(ts) FROM market_data.msft
UNION ALL
SELECT 'googl', MAX(ts) FROM market_data.googl;

-- Count rows per table
SELECT
    schemaname,
    relname as table_name,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'market_data'
ORDER BY n_live_tup DESC
LIMIT 10;

-- Total tables in market_data schema
SELECT COUNT(*) as table_count
FROM pg_tables
WHERE schemaname = 'market_data';
```

## Market Hours Logic

The `market_data_intraday_update` DAG includes smart scheduling:

- **NYSE Regular Hours:** 9:30 AM - 4:00 PM Eastern Time
- **In UTC:** 14:30 - 21:00 (during EST) / 13:30 - 20:00 (during EDT)
- **Weekend Skip:** Saturday and Sunday are automatically skipped

**How it works:**

1. DAG triggers every 30 minutes (24/7)
2. First task `check_market_hours` evaluates current UTC time
3. If outside market hours → returns `False` → all downstream tasks skipped
4. If within market hours → returns `True` → ticker data is fetched and upserted

**Concurrency:** Limited to 16 concurrent task instances (`max_active_tis_per_dag=16`) to prevent executor overload.

## Troubleshooting

### DAG not appearing in Airflow UI

```bash
# Check for import errors
docker-compose exec airflow-scheduler airflow dags list-import-errors
```

### Tasks failing or stuck in retry

```bash
# Check task logs
docker-compose exec airflow-scheduler airflow tasks logs <dag_id> <task_id> --limit 1

# Check scheduler logs
docker logs apache-airflow-311-for-hocv-stocks-with-yahoo-finance-api-main-airflow-scheduler-1 2>&1 | tail -50
```

### No data in tables

1. Verify `market_data_create` ran successfully (creates schema)
2. Check if DAG is paused (toggle ON in UI)
3. For intraday DAG: verify current time is within market hours (14:30-21:00 UTC, weekdays)

### Containers not starting

```bash
# View logs
docker-compose logs --tail=50

# Restart everything
docker-compose down && docker-compose up -d

# If image missing, rebuild first
docker build -t extending-airflow .
docker-compose up -d
```

### "extending-airflow" image not found

```bash
# Build the custom image
docker build -t extending-airflow .
```

## Maintenance

### Add New Tickers

1. Edit `data/info/tickers_list.csv`
2. Add row: `yf_ticker,Ticker` (e.g., `TSLA,TSLA`)
3. Update `plugins/market_data_shared.py` INDEXES list
4. Run `market_data_create` DAG to create new table
5. Data will be fetched on next scheduled run

### Pause/Unpause DAGs

```bash
# Via CLI
docker-compose exec airflow-scheduler airflow dags pause <dag_id>
docker-compose exec airflow-scheduler airflow dags unpause <dag_id>

# Or use the toggle switch in Airflow UI
```

### View Logs

```bash
# Scheduler logs
docker-compose logs airflow-scheduler --tail=100

# All logs (follow mode)
docker-compose logs --tail=100 -f

# Specific container
docker logs <container-name> --tail=100
```

### Stop the Project

```bash
# Stop containers (keeps data)
docker-compose down

# Stop and DELETE all data (volumes)
docker-compose down -v
```

## Architecture Diagram

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Yahoo Finance  │────▶│  Airflow DAGs    │────▶│   PostgreSQL    │
│      API        │     │                  │     │                 │
└─────────────────┘     │  - create_tables │     │  market_data.*  │
                        │  - ingest (EOD)  │     │  ├── aapl       │
                        │  - intraday_upd  │     │  ├── msft       │
                        └──────────────────┘     │  ├── sp500      │
                                                 │  └── ... (155)  │
┌─────────────────┐     ┌──────────────────┐     │                 │
│  Excel Files    │────▶│  muestra DAGs    │────▶│  muestra.*      │
│  (data/muestra) │     │                  │     │                 │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Container Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose Stack                     │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Scheduler  │  │  API Server │  │  DAG Processor      │  │
│  │             │  │  (UI:8080)  │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Worker    │  │  Triggerer  │  │  Init (one-time)    │  │
│  │             │  │             │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│  ┌─────────────────────────────┐  ┌─────────────────────┐  │
│  │  PostgreSQL (port 5434)     │  │  Redis              │  │
│  │  - Airflow metadata         │  │  - Message broker   │  │
│  │  - market_data schema       │  │                     │  │
│  └─────────────────────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Version Info

- **Airflow:** 3.1.1
- **PostgreSQL:** 16.11
- **Python:** 3.12
- **Key packages:** yfinance, pandas, psycopg2-binary
