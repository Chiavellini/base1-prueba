"""
Wide Format Ingest DAG
=======================

This DAG updates all three wide-format price tables with current data:
    - market_data.closing_prices (Close)
    - market_data.high_prices    (High)
    - market_data.low_prices     (Low)

A SINGLE yf.download() call fetches data for all 153 tickers.  The same
DataFrame is then used to upsert Close, High, and Low into their
respective tables.

How it works:
1. Check if current time is within market hours (14:30-21:00 UTC, weekdays)
2. If yes: one download → upsert close, high, low rows for today
3. If no: skip execution

Schedule: * * * * * (every 1 minute)
Market Hours: 14:30-21:00 UTC (9:30 AM - 4:00 PM ET)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

from plugins.market_data_wide import (
    is_market_hours,
    upsert_all_wide_prices_row,
    get_closing_prices_stats,
    get_high_prices_stats,
    get_low_prices_stats,
    TICKERS,
)


default_args = {
    "owner": "heri",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def upsert_all_prices_task(**context):
    """
    Download price data ONCE and upsert today's row into all three
    wide tables (closing_prices, high_prices, low_prices).
    """
    result = upsert_all_wide_prices_row(
        target_date=None,
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print(f"[upsert_all_prices] Result: {result}")

    for table in ("closing_prices", "high_prices", "low_prices"):
        sub = result.get(table)
        if sub and "tickers_fetched" in sub:
            rate = sub["tickers_fetched"] / len(TICKERS) * 100
            print(f"[upsert_all_prices] {table}: {rate:.1f}% "
                  f"({sub['tickers_fetched']}/{len(TICKERS)})")

    return result


def log_stats_task(**context):
    """Log statistics for all three wide-format tables."""
    for label, stats_fn in [
        ("closing_prices", get_closing_prices_stats),
        ("high_prices", get_high_prices_stats),
        ("low_prices", get_low_prices_stats),
    ]:
        stats = stats_fn(postgres_conn_id="postgres_default", database="airflow")

        print(f"[stats:{label}] Row count: {stats['row_count']}")
        print(f"[stats:{label}] Date range: {stats['date_range']}")

        if stats["latest_row"]:
            print(f"[stats:{label}] Latest ({stats['latest_row']['date']}): "
                  f"AAPL={stats['latest_row']['aapl']}, "
                  f"MSFT={stats['latest_row']['msft']}, "
                  f"S&P500={stats['latest_row']['sp500']}")

    return {"logged": ["closing_prices", "high_prices", "low_prices"]}


with DAG(
    dag_id="market_data_ingest_wide",
    start_date=datetime(2025, 1, 1),
    schedule="* * * * *",  # Every 1 minute
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    description="Update all wide-format price tables (close, high, low) with current prices",
    tags=["market_data", "ingest", "wide"],
) as dag:

    check_market_hours = ShortCircuitOperator(
        task_id="check_market_hours",
        python_callable=is_market_hours,
    )

    upsert_all_prices = PythonOperator(
        task_id="upsert_all_wide_prices",
        python_callable=upsert_all_prices_task,
        execution_timeout=timedelta(minutes=5),
    )

    log_stats = PythonOperator(
        task_id="log_table_stats",
        python_callable=log_stats_task,
    )

    check_market_hours >> upsert_all_prices >> log_stats
