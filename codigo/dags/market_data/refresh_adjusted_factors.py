"""
Refresh Adjusted Factors DAG
==============================

Weekly maintenance DAG that re-downloads full history from Yahoo Finance,
recomputes adjustment factors for all rows, and detects corporate actions
(splits, dividends) by comparing consecutive adj_factors.

Task chain: refresh_adj_factors -> log_corporate_actions

Schedule: 0 6 * * 0 (Sunday 06:00 UTC)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.market_data_adjusted import (
    fetch_historical_adj_close,
    detect_and_log_corporate_actions,
)


default_args = {
    "owner": "heri",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def refresh_factors_task(**context):
    """Re-download history and recompute adj_factors for all securities."""
    result = fetch_historical_adj_close(
        period="1y",
        postgres_conn_id="postgres_default",
        database="airflow",
    )
    print(f"[refresh] Result: {result}")
    return result


def log_corporate_actions_task(**context):
    """Detect and log corporate actions from adj_factor changes."""
    result = detect_and_log_corporate_actions(
        threshold=0.01,
        postgres_conn_id="postgres_default",
        database="airflow",
    )
    print(f"[corporate_actions] Result: {result}")
    return result


with DAG(
    dag_id="market_data_refresh_adjusted",
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * 0",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args=default_args,
    description="Weekly refresh of adjustment factors and corporate action detection",
    tags=["market_data", "adjusted", "maintenance"],
) as dag:

    refresh = PythonOperator(
        task_id="refresh_adj_factors",
        python_callable=refresh_factors_task,
        execution_timeout=timedelta(minutes=20),
    )

    log_actions = PythonOperator(
        task_id="log_corporate_actions",
        python_callable=log_corporate_actions_task,
    )

    refresh >> log_actions
