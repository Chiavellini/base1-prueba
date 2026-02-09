"""
Create Slope Tables DAG
========================

This DAG creates the 12 slope tables and backfills historical data.

Tables created:
    market_data.slope_10d   - 10-day slope
    market_data.slope_20d   - 20-day slope
    market_data.slope_30d   - 30-day slope
    market_data.slope_40d   - 40-day slope
    market_data.slope_50d   - 50-day slope
    market_data.slope_60d   - 60-day slope
    market_data.slope_70d   - 70-day slope
    market_data.slope_80d   - 80-day slope
    market_data.slope_90d   - 90-day slope
    market_data.slope_100d  - 100-day slope
    market_data.slope_150d  - 150-day slope
    market_data.slope_200d  - 200-day slope

Each table stores the slope (linear regression coefficient) of adjusted close
prices over the specified lookback period.

Task chain: create_tables >> backfill_tables >> verify_tables

Schedule: @once (manual trigger)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.market_data_slope import (
    create_all_slope_tables,
    backfill_all_slope_tables,
    verify_all_slope_tables,
    LOOKBACK_PERIODS,
    TABLE_NAMES,
)


default_args = {
    "owner": "heri",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_tables_task(**context):
    """Create all 12 slope tables."""
    print("=" * 70)
    print("CREATING SLOPE TABLES")
    print("=" * 70)
    print(f"Tables to create: {list(TABLE_NAMES.values())}")
    print()

    results = create_all_slope_tables(
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print()
    print("Creation Results:")
    for table_name, result in results.items():
        status = result.get("status", "unknown")
        print(f"  {table_name}: {status}")

    context["ti"].xcom_push(key="create_results", value=results)
    return results


def backfill_tables_task(**context):
    """Backfill historical data for all 12 slope tables."""
    print("=" * 70)
    print("BACKFILLING SLOPE TABLES")
    print("=" * 70)
    print("This may take several minutes...")
    print()

    results = backfill_all_slope_tables(
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print()
    print("Backfill Results:")
    tables = results.get("tables", {})
    for table_name, result in tables.items():
        rows = result.get("rows_inserted", 0)
        date_range = f"{result.get('start_date')} to {result.get('end_date')}"
        print(f"  {table_name}: {rows} rows ({date_range})")

    total = results.get("total_rows", 0)
    print()
    print(f"Total rows inserted: {total}")

    context["ti"].xcom_push(key="backfill_results", value=results)
    return results


def verify_tables_task(**context):
    """Verify all slope tables have data."""
    print("=" * 70)
    print("VERIFYING SLOPE TABLES")
    print("=" * 70)

    results = verify_all_slope_tables(
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print()
    print("Verification Results:")
    tables = results.get("tables", {})
    for table_name, stats in tables.items():
        if "error" in stats:
            print(f"  {table_name}: ERROR - {stats['error']}")
        else:
            row_count = stats.get("row_count", 0)
            date_range = stats.get("date_range", "N/A")
            print(f"  {table_name}: {row_count} rows ({date_range})")

    total = results.get("total_rows", 0)
    print()
    print(f"Total rows across all tables: {total}")
    print("=" * 70)

    # Pull backfill results for summary
    ti = context["ti"]
    backfill_results = ti.xcom_pull(task_ids="backfill_tables", key="backfill_results") or {}

    print()
    print("FINAL SUMMARY")
    print("=" * 70)
    print(f"Tables created: {len(TABLE_NAMES)}")
    print(f"Lookback periods: {LOOKBACK_PERIODS}")
    print(f"Total rows backfilled: {backfill_results.get('total_rows', 0)}")
    print(f"Total rows verified: {total}")
    print("=" * 70)

    return results


with DAG(
    dag_id="market_data_create_slope",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=120),
    default_args=default_args,
    description="Create and backfill 12 slope tables (10d to 200d lookback periods)",
    tags=["market_data", "slope", "setup"],
) as dag:

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_task,
        execution_timeout=timedelta(minutes=5),
    )

    backfill_tables = PythonOperator(
        task_id="backfill_tables",
        python_callable=backfill_tables_task,
        execution_timeout=timedelta(minutes=90),
    )

    verify_tables = PythonOperator(
        task_id="verify_tables",
        python_callable=verify_tables_task,
        execution_timeout=timedelta(minutes=5),
    )

    create_tables >> backfill_tables >> verify_tables
