"""
This DAG creates the 'muestra' schema and its associated tables in the PostgreSQL database.
It sets up the necessary structure to store identity information, index membership,
and processed files metadata.

Create schema `muestra` and base tables:
- muestra.identity
- muestra.index_membership
- muestra.files_processed
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SQL_DDL = r"""
CREATE SCHEMA IF NOT EXISTS muestra;

-- Identity: ISIN / nombre / GICS / moneda
CREATE TABLE IF NOT EXISTS muestra.identity (
    ticker      TEXT NOT NULL,
    date        DATE NOT NULL,
    isin        TEXT,
    name        TEXT,
    gics_sector TEXT,
    gics_ind    TEXT,
    gics_subind TEXT,
    currency    TEXT,
    PRIMARY KEY (ticker, date)
);

-- Pertenencia a índices y capitalización
CREATE TABLE IF NOT EXISTS muestra.index_membership (
    ticker        TEXT NOT NULL,
    date          DATE NOT NULL,
    in_dji        BOOLEAN NOT NULL,
    in_sp500      BOOLEAN NOT NULL,
    in_nasdaq100  BOOLEAN NOT NULL,
    sic           NUMERIC,
    market_cap    NUMERIC,
    cap_class     TEXT,
    PRIMARY KEY (ticker, date)
);

-- Archivos ya procesados
CREATE TABLE IF NOT EXISTS muestra.files_processed (
    filename      TEXT PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL,
    rows_ingested INTEGER
);
"""

default_args = {
    "owner": "heri",
    "retries": 0,
}

with DAG(
    dag_id="muestra_create",
    start_date=datetime(2025, 11, 10),
    schedule=None,              # run manually once
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=5),
    description=__doc__,  
) as dag:

    create_schema_and_tables = SQLExecuteQueryOperator(
        task_id="create_schema_and_tables",
        conn_id="postgres_default",
        sql=SQL_DDL,
        hook_params={"database": "airflow"},  # adjust DB name if needed
    )
