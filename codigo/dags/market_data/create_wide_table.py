"""
Create Wide Tables DAG
=======================

This DAG creates the wide-format price tables and backfills historical data:

    market_data.closing_prices  – daily close prices
    market_data.high_prices     – daily high prices
    market_data.low_prices      – daily low prices

Each table has the same schema: date PK + 153 ticker columns.

Task chain:
    create_closing → create_high → create_low
    → backfill_closing → backfill_high → backfill_low
    → verify_all

Schedule: @once (manual trigger)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.market_data_wide import (
    generate_closing_prices_ddl,
    generate_high_prices_ddl,
    generate_low_prices_ddl,
    backfill_closing_prices_from_yahoo,
    backfill_high_prices_from_yahoo,
    backfill_low_prices_from_yahoo,
    get_closing_prices_stats,
    get_high_prices_stats,
    get_low_prices_stats,
    TICKER_COLUMNS,
)


default_args = {
    "owner": "heri",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _create_wide_table(ddl_func, table_label: str, **context):
    """Helper: execute DDL for a wide-format table."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ddl = ddl_func()
    print(f"[create_table] Creating {table_label} with {len(TICKER_COLUMNS)} ticker columns")
    print(f"[create_table] DDL preview (first 500 chars):\n{ddl[:500]}...")

    hook = PostgresHook(postgres_conn_id="postgres_default", database="airflow")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

    print(f"[create_table] Table market_data.{table_label} created successfully")
    return {"columns_created": len(TICKER_COLUMNS) + 1}


def create_closing_task(**context):
    """Create the closing_prices table."""
    return _create_wide_table(generate_closing_prices_ddl, "closing_prices", **context)


def create_high_task(**context):
    """Create the high_prices table."""
    return _create_wide_table(generate_high_prices_ddl, "high_prices", **context)


def create_low_task(**context):
    """Create the low_prices table."""
    return _create_wide_table(generate_low_prices_ddl, "low_prices", **context)


def backfill_closing_task(**context):
    """Backfill closing_prices from Yahoo Finance (1 year)."""
    result = backfill_closing_prices_from_yahoo(
        period="1y",
        postgres_conn_id="postgres_default",
        database="airflow",
    )
    print(f"[backfill_closing] Result: {result}")
    return result


def backfill_high_task(**context):
    """Backfill high_prices from Yahoo Finance (1 year)."""
    result = backfill_high_prices_from_yahoo(
        period="1y",
        postgres_conn_id="postgres_default",
        database="airflow",
    )
    print(f"[backfill_high] Result: {result}")
    return result


def backfill_low_task(**context):
    """Backfill low_prices from Yahoo Finance (1 year)."""
    result = backfill_low_prices_from_yahoo(
        period="1y",
        postgres_conn_id="postgres_default",
        database="airflow",
    )
    print(f"[backfill_low] Result: {result}")
    return result


def verify_all_task(**context):
    """Verify all three wide tables by showing statistics."""
    for label, stats_fn in [
        ("closing_prices", get_closing_prices_stats),
        ("high_prices", get_high_prices_stats),
        ("low_prices", get_low_prices_stats),
    ]:
        stats = stats_fn(postgres_conn_id="postgres_default", database="airflow")
        print(f"[verify] {label}: {stats}")

    return {"verified": ["closing_prices", "high_prices", "low_prices"]}


with DAG(
    dag_id="market_data_create_wide",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    default_args=default_args,
    description="Create wide-format price tables (close, high, low) and backfill historical data",
    tags=["market_data", "setup", "migration"],
) as dag:

    create_closing = PythonOperator(
        task_id="create_closing_prices_table",
        python_callable=create_closing_task,
    )

    create_high = PythonOperator(
        task_id="create_high_prices_table",
        python_callable=create_high_task,
    )

    create_low = PythonOperator(
        task_id="create_low_prices_table",
        python_callable=create_low_task,
    )

    backfill_closing = PythonOperator(
        task_id="backfill_closing_prices",
        python_callable=backfill_closing_task,
        execution_timeout=timedelta(minutes=20),
    )

    backfill_high = PythonOperator(
        task_id="backfill_high_prices",
        python_callable=backfill_high_task,
        execution_timeout=timedelta(minutes=20),
    )

    backfill_low = PythonOperator(
        task_id="backfill_low_prices",
        python_callable=backfill_low_task,
        execution_timeout=timedelta(minutes=20),
    )

    verify_all = PythonOperator(
        task_id="verify_all",
        python_callable=verify_all_task,
    )

    (
        create_closing
        >> create_high
        >> create_low
        >> backfill_closing
        >> backfill_high
        >> backfill_low
        >> verify_all
    )
