"""
Create Adjusted Tables DAG
============================

This DAG creates the three adjusted-close tables and populates them
with initial data:

    market_data.securities          – master list of tracked securities
    market_data.adjusted_prices     – daily adjusted close prices
    market_data.corporate_actions   – detected corporate-action events

Task chain: create_tables -> populate_securities -> backfill_adjusted_prices -> verify_counts

Schedule: @once (manual trigger)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.market_data_adjusted import (
    generate_all_adjusted_ddl,
    populate_securities,
    backfill_adjusted_prices,
    verify_table_counts,
    TICKERS,
)


default_args = {
    "owner": "heri",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_tables_task(**context):
    """Create the three adjusted-close tables using generated DDL."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ddl = generate_all_adjusted_ddl()
    print(f"[create_tables] Creating 3 adjusted-close tables for {len(TICKERS)} tickers")
    print(f"[create_tables] DDL preview (first 500 chars):\n{ddl[:500]}...")

    hook = PostgresHook(postgres_conn_id="postgres_default", database="airflow")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

    print("[create_tables] All three tables created successfully")
    return {"tables_created": ["securities", "adjusted_prices", "corporate_actions"]}


def populate_securities_task(**context):
    """Populate securities table from CSV + yfinance info."""
    result = populate_securities(
        postgres_conn_id="postgres_default",
        database="airflow",
    )
    print(f"[populate_securities] Result: {result}")
    return result


def backfill_task(**context):
    """Backfill adjusted prices from Yahoo Finance (1 year)."""
    result = backfill_adjusted_prices(
        period="1y",
        postgres_conn_id="postgres_default",
        database="airflow",
    )
    print(f"[backfill] Result: {result}")
    return result


def verify_task(**context):
    """Verify table row counts after setup."""
    counts = verify_table_counts(
        postgres_conn_id="postgres_default",
        database="airflow",
    )
    print(f"[verify] Table counts: {counts}")
    return counts


with DAG(
    dag_id="market_data_create_adjusted",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args=default_args,
    description="Create adjusted-close tables and populate with initial data",
    tags=["market_data", "adjusted", "setup"],
) as dag:

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_task,
    )

    populate = PythonOperator(
        task_id="populate_securities",
        python_callable=populate_securities_task,
        execution_timeout=timedelta(minutes=10),
    )

    backfill = PythonOperator(
        task_id="backfill_adjusted_prices",
        python_callable=backfill_task,
        execution_timeout=timedelta(minutes=20),
    )

    verify = PythonOperator(
        task_id="verify_counts",
        python_callable=verify_task,
    )

    create_tables >> populate >> backfill >> verify
