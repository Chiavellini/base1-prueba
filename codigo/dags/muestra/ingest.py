"""
DAG to scan and ingest new Muestra Excel files.
It checks for new files in the specified directory,
processes them, and updates the PostgreSQL database accordingly.

Scan `/opt/airflow/data/muestra` for `Muestra_YYMMDD.xlsx`.

- Only process files not present in `muestra.files_processed`.
- For each file:
  - Fill `muestra.identity` and `muestra.index_membership`
  - Create/update one table `muestra.<ticker>` per ticker (NVDA, etc.).
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.muestra_shared import scan_and_ingest_new_files

default_args = {
    "owner": "heri",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="muestra_ingest",
    start_date=datetime(2025, 11, 10),
    schedule="*/1 * * * *",   # every 1 minute check for new files
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args=default_args,
    description=__doc__,
) as dag:

    scan_and_ingest = PythonOperator(
        task_id="scan_and_ingest",
        python_callable=scan_and_ingest_new_files,
        op_kwargs={
            "data_dir": "/opt/airflow/data/muestra",
            "postgres_conn_id": "postgres_default",
            "database": "airflow",
        },
    )
