"""
Fill Gaps DAG
==============

Daily maintenance DAG that detects missing dates in price tables and backfills
only the gaps. This is more efficient than re-downloading a full year of data.

Tables checked:
    - market_data.closing_prices (wide format)
    - market_data.high_prices (wide format)
    - market_data.low_prices (wide format)
    - market_data.adjusted_prices (long format)
    - market_data.slope_10d through slope_200d (12 slope tables)

Logic:
    1. Get all trading days from Yahoo Finance for the last 30 days
    2. Compare with dates in our tables
    3. Identify missing dates
    4. Backfill only the missing dates

Schedule: 0 22 * * 1-5 (10 PM UTC Mon-Fri, after market close)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.market_data_wide import (
    fill_all_wide_gaps,
    find_missing_dates_wide,
)
from plugins.market_data_adjusted import (
    fill_adjusted_gaps,
    find_missing_dates_adjusted,
)
from plugins.market_data_slope import (
    fill_all_slope_gaps,
    TABLE_NAMES as SLOPE_TABLE_NAMES,
)


DAYS_BACK = 30  # How many days to look back for gaps

default_args = {
    "owner": "heri",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def fill_wide_gaps_task(**context):
    """Find and fill gaps in all wide-format tables."""
    results = fill_all_wide_gaps(
        days_back=DAYS_BACK,
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print(f"[fill_wide_gaps] Results: {results}")

    # Push results to XCom for summary task
    context["ti"].xcom_push(key="wide_results", value=results)

    return results


def fill_adjusted_gaps_task(**context):
    """Find and fill gaps in adjusted_prices table."""
    results = fill_adjusted_gaps(
        days_back=DAYS_BACK,
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print(f"[fill_adjusted_gaps] Results: {results}")

    # Push results to XCom for summary task
    context["ti"].xcom_push(key="adjusted_results", value=results)

    return results


def fill_slope_gaps_task(**context):
    """Find and fill gaps in all slope tables."""
    results = fill_all_slope_gaps(
        days_back=DAYS_BACK,
        postgres_conn_id="postgres_default",
        database="airflow",
    )

    print(f"[fill_slope_gaps] Results: {results}")

    # Push results to XCom for summary task
    context["ti"].xcom_push(key="slope_results", value=results)

    return results


def log_summary_task(**context):
    """Log summary of all gaps found and filled."""
    ti = context["ti"]

    wide_results = ti.xcom_pull(task_ids="fill_wide_gaps", key="wide_results") or {}
    adjusted_results = ti.xcom_pull(task_ids="fill_adjusted_gaps", key="adjusted_results") or {}
    slope_results = ti.xcom_pull(task_ids="fill_slope_gaps", key="slope_results") or {}

    print("=" * 70)
    print("GAP FILL SUMMARY")
    print("=" * 70)

    total_missing = 0
    total_filled = 0

    # Wide tables summary
    print("\nWide-format price tables:")
    tables = wide_results.get("tables", {})
    for table_name, result in tables.items():
        missing = result.get("missing_count", 0)
        filled = result.get("filled_count", 0)
        total_missing += missing
        total_filled += filled

        if missing == 0:
            status = "OK (no gaps)"
        else:
            status = f"FILLED {filled} rows for {missing} missing dates"

        print(f"  {table_name}: {status}")
        if result.get("dates"):
            print(f"    Dates: {result['dates']}")

    # Adjusted prices summary
    print("\nLong-format tables:")
    adj_missing = adjusted_results.get("missing_count", 0)
    adj_filled = adjusted_results.get("filled_count", 0)
    total_missing += adj_missing
    total_filled += adj_filled

    if adj_missing == 0:
        adj_status = "OK (no gaps)"
    else:
        adj_status = f"FILLED {adj_filled} rows for {adj_missing} missing dates"

    print(f"  adjusted_prices: {adj_status}")
    if adjusted_results.get("dates"):
        print(f"    Dates: {adjusted_results['dates']}")

    # Slope tables summary
    print("\nSlope tables:")
    slope_tables = slope_results.get("tables", {})
    slope_total_missing = slope_results.get("total_missing", 0)
    slope_total_filled = slope_results.get("total_filled", 0)

    # Group slope tables by status for cleaner output
    slope_ok = []
    slope_filled = []
    slope_error = []

    for table_name, result in slope_tables.items():
        if "error" in result:
            slope_error.append((table_name, result.get("error")))
        elif result.get("missing_count", 0) == 0:
            slope_ok.append(table_name)
        else:
            slope_filled.append((table_name, result.get("missing_count", 0), result.get("filled_count", 0)))

    if slope_ok:
        print(f"  OK (no gaps): {len(slope_ok)} tables")
        # Only list first few if many
        if len(slope_ok) <= 4:
            for t in slope_ok:
                print(f"    - {t}")
        else:
            print(f"    ({', '.join(slope_ok[:3])}, ...)")

    if slope_filled:
        print(f"  FILLED: {len(slope_filled)} tables")
        for table_name, missing, filled in slope_filled:
            print(f"    - {table_name}: {filled} rows for {missing} missing dates")

    if slope_error:
        print(f"  ERRORS: {len(slope_error)} tables")
        for table_name, error in slope_error:
            print(f"    - {table_name}: {error}")

    total_missing += slope_total_missing
    total_filled += slope_total_filled

    # Overall summary
    print("\n" + "=" * 70)
    print(f"TOTAL: {total_missing} missing dates found, {total_filled} rows inserted")
    print("=" * 70)

    return {
        "total_missing": total_missing,
        "total_filled": total_filled,
        "wide_missing": wide_results.get("total_missing", 0),
        "adjusted_missing": adj_missing,
        "slope_missing": slope_total_missing,
    }


with DAG(
    dag_id="market_data_fill_gaps",
    start_date=datetime(2025, 1, 1),
    schedule="0 22 * * 1-5",  # 10 PM UTC Mon-Fri (after market close)
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),  # Increased for slope tables
    default_args=default_args,
    description="Detect and fill missing dates in price and slope tables (last 30 days)",
    tags=["market_data", "maintenance", "gap_fill"],
) as dag:

    fill_wide = PythonOperator(
        task_id="fill_wide_gaps",
        python_callable=fill_wide_gaps_task,
        execution_timeout=timedelta(minutes=15),
    )

    fill_adjusted = PythonOperator(
        task_id="fill_adjusted_gaps",
        python_callable=fill_adjusted_gaps_task,
        execution_timeout=timedelta(minutes=15),
    )

    fill_slope = PythonOperator(
        task_id="fill_slope_gaps",
        python_callable=fill_slope_gaps_task,
        execution_timeout=timedelta(minutes=30),  # Longer for 12 tables
    )

    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary_task,
    )

    # Run wide, adjusted, and slope in parallel, then summarize
    [fill_wide, fill_adjusted, fill_slope] >> log_summary
