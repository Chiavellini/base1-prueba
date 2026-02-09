"""
Ingest Slope DAG
=================

Daily DAG that calculates and inserts today's slope for all 12 slope tables.

Runs at 22:30 UTC (4:30 PM Mexico City) Mon-Fri, after the adjusted_prices
table has been updated by the ingest_adjusted DAG.

Slope is calculated using linear regression:
    m = Cov(x, y) / Var(x)

Where x is day index and y is adj_close prices for the lookback period.

Schedule: 30 22 * * 1-5 (10:30 PM UTC, Mon-Fri)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.market_data_slope import (
    update_slope_tables_today,
    get_trading_days_from_adjusted,
    LOOKBACK_PERIODS,
    TABLE_NAMES,
)


default_args = {
    "owner": "heri",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def update_slopes_task(**context):
    """Calculate and insert today's slope for all 12 tables."""
    print("=" * 70)
    print("UPDATING SLOPE TABLES")
    print("=" * 70)

    # Get the most recent trading day
    trading_days = get_trading_days_from_adjusted(
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    if not trading_days:
        print("No trading days found in adjusted_prices table.")
        print("Skipping slope calculation.")
        return {"status": "skipped", "reason": "no trading days"}

    latest_date = trading_days[-1]
    print(f"Latest trading day: {latest_date}")
    print(f"Total trading days available: {len(trading_days)}")
    print()

    # Update all slope tables
    results = update_slope_tables_today(
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print()
    print("Update Results:")
    tables = results.get("tables", {})
    success_count = 0
    for table_name, result in tables.items():
        status = result.get("status", "unknown")
        if status == "success":
            success_count += 1
            print(f"  {table_name}: SUCCESS")
        elif status == "skipped":
            reason = result.get("reason", "unknown")
            print(f"  {table_name}: SKIPPED ({reason})")
        else:
            error = result.get("error", "unknown")
            print(f"  {table_name}: ERROR - {error}")

    print()
    print(f"Updated {success_count}/{len(TABLE_NAMES)} tables")

    context["ti"].xcom_push(key="update_results", value=results)
    return results


def log_summary_task(**context):
    """Log summary of the slope update."""
    ti = context["ti"]
    results = ti.xcom_pull(task_ids="update_slopes", key="update_results") or {}

    print("=" * 70)
    print("SLOPE UPDATE SUMMARY")
    print("=" * 70)

    if results.get("status") == "skipped":
        print(f"Status: SKIPPED")
        print(f"Reason: {results.get('reason', 'unknown')}")
        return results

    update_date = results.get("date", "unknown")
    tables = results.get("tables", {})
    success_count = results.get("success_count", 0)

    print(f"Date: {update_date}")
    print(f"Tables updated: {success_count}/{len(TABLE_NAMES)}")
    print()

    # Group by status
    successful = [t for t, r in tables.items() if r.get("status") == "success"]
    skipped = [t for t, r in tables.items() if r.get("status") == "skipped"]
    failed = [t for t, r in tables.items() if r.get("status") == "error"]

    if successful:
        print(f"Successful ({len(successful)}):")
        for t in successful:
            print(f"  - {t}")

    if skipped:
        print(f"Skipped ({len(skipped)}):")
        for t in skipped:
            reason = tables[t].get("reason", "unknown")
            print(f"  - {t}: {reason}")

    if failed:
        print(f"Failed ({len(failed)}):")
        for t in failed:
            error = tables[t].get("error", "unknown")
            print(f"  - {t}: {error}")

    print("=" * 70)

    return {
        "date": update_date,
        "successful": len(successful),
        "skipped": len(skipped),
        "failed": len(failed),
    }


with DAG(
    dag_id="market_data_ingest_slope",
    start_date=datetime(2025, 1, 1),
    schedule="30 22 * * 1-5",  # 10:30 PM UTC Mon-Fri (after adjusted_prices update)
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args=default_args,
    description="Daily slope calculation for all 12 lookback periods",
    tags=["market_data", "slope", "ingest"],
) as dag:

    update_slopes = PythonOperator(
        task_id="update_slopes",
        python_callable=update_slopes_task,
        execution_timeout=timedelta(minutes=20),
    )

    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary_task,
    )

    update_slopes >> log_summary
