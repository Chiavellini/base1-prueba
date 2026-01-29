"""
This dag creates the necessary tables in the Postgres database to store
market data for various tickers in data/info/ticker_list.csv, 
it add their names based on data/muestra/muestra_YYMMDD.xlsex
"""

from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from plugins.market_data_shared import INDEXES, SQL_DDL_META, per_ticker_table_sql

default_args = {
    "owner": "heri",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_data_create",
    start_date=datetime(2025, 11, 3, 0, 0),
    schedule="@once",        # run only once
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    description=__doc__
) as dag:

    create_all_tables = SQLExecuteQueryOperator(
        task_id="create_all_tables",
        conn_id="postgres_default",
        hook_params={"database": "airflow"},
        sql=[SQL_DDL_META] + [per_ticker_table_sql(cfg) for cfg in INDEXES],
    )
