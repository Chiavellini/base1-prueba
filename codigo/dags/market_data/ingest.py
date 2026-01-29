"""
This DAG fetches minutely market data for multiple tickers from Yahoo Finance
and upserts it into corresponding PostgreSQL tables. It uses dynamic task mapping
to create one task per ticker defined in the INDEXES configuration.
"""


from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.market_data_shared import INDEXES, upsert_index_yahoo_minutely_into_table

PERIOD = "max"      # how far back to fetch data from Yahoo
INTERVAL = "1d"    # data interval
# https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html
# https://aroussi.com/post/python-yahoo-finance

default_args = {
    "owner": "heri",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_data_ingest",
    start_date=datetime(2025, 11, 3, 0, 0),
    schedule="0 22 * * *",   # every day at 22 UTC
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=2),
    default_args=default_args,
    description=__doc__
) as dag:

    # One Python task per index via dynamic mapping
    upserts = PythonOperator.partial(
        task_id="upsert_index_minutely",
        python_callable=upsert_index_yahoo_minutely_into_table,
    ).expand(op_kwargs=[
        {
            "table": cfg["table"],       # normalized DB table name, e.g. "brk_b"
            "ticker": cfg["ticker"],     # yf_ticker, e.g. "BRK-B", "^GSPC"
            "name": cfg["name"],         # Nombre from Muestra or alias
            "source": "yahoo",
            "period": PERIOD,              # how far back we ask Yahoo
            "interval": INTERVAL,          # data interval
            "postgres_conn_id": "postgres_default",
            "database": "airflow",
        }
        for cfg in INDEXES
    ])