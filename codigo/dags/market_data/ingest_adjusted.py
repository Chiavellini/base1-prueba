"""
Adjusted Close Ingest DAG
===========================

This DAG updates market_data.adjusted_prices with current prices.
It runs every minute during NYSE market hours.

How it works:
1. Check if current time is within market hours (14:30-21:00 UTC, weekdays)
2. If yes: fetch ALL tickers using yfinance, upsert latest adjusted prices
3. If no: skip execution

Schedule: * * * * * (every 1 minute)
Market Hours: 14:30-21:00 UTC (9:30 AM - 4:00 PM ET)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

from plugins.market_data_adjusted import (
    is_market_hours,
    fetch_yahoo_data_today,
    get_adjusted_prices_stats,
    TICKERS,
)


default_args = {
    "owner": "heri",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def upsert_adjusted_task(**context):
    """
    Fetch all ticker prices and upsert today's adjusted close data.

    Downloads latest 5d window, extracts the most recent trading day,
    and upserts into adjusted_prices.
    """
    result = fetch_yahoo_data_today(
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print(f"[upsert_adjusted] Result: {result}")

    upserted = result.get("upserted", 0)
    success_rate = upserted / len(TICKERS) * 100 if TICKERS else 0
    print(f"[upsert_adjusted] Success rate: {success_rate:.1f}% "
          f"({upserted}/{len(TICKERS)} tickers)")

    return result


def log_stats_task(**context):
    """Log current adjusted_prices table statistics after update."""
    stats = get_adjusted_prices_stats(
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print(f"[stats] Row count: {stats['row_count']}")
    print(f"[stats] Date range: {stats['date_range']}")

    if stats["latest_rows"]:
        print("[stats] Latest rows:")
        for row in stats["latest_rows"]:
            adj = f"${row['adj_close']:.2f}" if row["adj_close"] else "N/A"
            print(f"        {row['ticker']} ({row['date']}): adj_close={adj}")

    return stats


with DAG(
    dag_id="market_data_ingest_adjusted",
    start_date=datetime(2025, 1, 1),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    description="Update adjusted_prices table with current prices",
    tags=["market_data", "adjusted", "ingest"],
) as dag:

    check_market_hours = ShortCircuitOperator(
        task_id="check_market_hours",
        python_callable=is_market_hours,
    )

    fetch_and_upsert = PythonOperator(
        task_id="fetch_and_upsert_adjusted",
        python_callable=upsert_adjusted_task,
        execution_timeout=timedelta(minutes=5),
    )

    log_stats = PythonOperator(
        task_id="log_adjusted_stats",
        python_callable=log_stats_task,
    )

    check_market_hours >> fetch_and_upsert >> log_stats
