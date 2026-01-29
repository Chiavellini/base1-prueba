"""
Market Data Shared Module
==========================

This module provides shared functionality for managing market data tables
in a PostgreSQL database using Airflow. It includes functions to create
tables, normalize ticker names, and upsert market data from Yahoo Finance.

It is used for dags/market_data/
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---------------------------------------------------------------------
# 0) Load the latest Muestra_*.xlsx and build Ticker -> Nombre mapping
# ---------------------------------------------------------------------
muestra_dir = Path("data/muestra")   # adjust if needed
muestra_files = list(muestra_dir.glob("Muestra*.xlsx"))

if not muestra_files:
    raise FileNotFoundError(f"No Muestra*.xlsx found in {muestra_dir}")

# latest by modification time
latest_muestra = max(muestra_files, key=os.path.getmtime)

# If you prefer a fixed file name, you could instead do:
# latest_muestra = muestra_dir / "Muestra.xlsx"

# Read the sheet that contains Ticker / Nombre
df_muestra = pd.read_excel(latest_muestra, sheet_name="muestra_mensual")

# Build dictionary: "Ticker" -> "Nombre"
ticker_name_map = (
    df_muestra[["Ticker", "Nombre"]]
    .dropna(subset=["Ticker", "Nombre"])
    .drop_duplicates(subset=["Ticker"])
    .set_index("Ticker")["Nombre"]
    .to_dict()
)



def normalize_table_name(alias: str) -> str:
    """
    Convert a ticker alias (e.g. 'BRK-B', 'SP500') into a valid PostgreSQL
    identifier to be used as the table name.
    """
    table = alias.lower()
    # replace characters not allowed in bare identifiers
    table = table.replace("-", "_").replace(" ", "_")

    # if it starts with a digit, prefix with 't_'
    if table and table[0].isdigit():
        table = "t_" + table

    return table

#---------------------------------------------------------------------
# 1) Build INDEXES from tickers_list.csv (+ Muestra names)
#---------------------------------------------------------------------
INDEXES: list[dict] = []

existing_tickers = {item["ticker"] for item in INDEXES}

csv_path = Path("data/info/tickers_list.csv")

with csv_path.open(newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)  # expects columns: yf_ticker, Ticker
    for row in reader:
        yf_ticker = row["yf_ticker"].strip()
        alias     = row["Ticker"].strip()   # e.g., "NVDA", "SP500", "BRK-B"

        if yf_ticker in existing_tickers:
            continue

        name = ticker_name_map.get(alias, alias)

        table_name = normalize_table_name(alias)

        INDEXES.append({
            "table": table_name,   # safe DB identifier, e.g. "brk_b"
            "ticker": yf_ticker,   # e.g. "^GSPC", "NVDA"
            "name": name,          # from Muestra or alias
            "alias": alias,        # optional, for reference
        })

        existing_tickers.add(yf_ticker)


# ---------------------------------------------------------------------
# 2) Static DDL for schema + metadata tables + per-ticker tables
# ---------------------------------------------------------------------
SQL_DDL_META = r"""
CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.names (
    ticker TEXT PRIMARY KEY,
    name   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_data.sources (
    ticker TEXT PRIMARY KEY,
    source TEXT NOT NULL
);
"""


def per_ticker_table_sql(index_cfg: dict) -> str:
    """DDL for one per-ticker OHLCV table."""
    table = index_cfg["table"]
    return f"""
    CREATE TABLE IF NOT EXISTS market_data.{table} (
        ts     TIMESTAMPTZ PRIMARY KEY,
        open   NUMERIC(18,6) NOT NULL,
        high   NUMERIC(18,6) NOT NULL,
        low    NUMERIC(18,6) NOT NULL,
        close  NUMERIC(18,6) NOT NULL,
        volume BIGINT
    );
    """




# ---------------------------------------------------------------------
# 3) Upsert function: only new minutes since max(ts)
# ---------------------------------------------------------------------
def upsert_index_yahoo_minutely_into_table(
    table: str,
    ticker: str,
    name: str,
    source: str = "yahoo",
    period: str = "1d",           # small window; we fill only missing minutes
    interval: str = "1m",
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> None:
    import yfinance as yf
    from psycopg2.extras import execute_values

    hist = yf.Ticker(ticker).history(
        period=period,
        interval=interval,
        auto_adjust=False,
    )
    if hist.empty:
        print(f"[{ticker}] no data from Yahoo (period={period} interval={interval})")
        return

    # Normalize index to UTC minute
    if hist.index.tz is not None:
        hist.index = hist.index.tz_convert("UTC")
    else:
        hist.index = hist.index.tz_localize("UTC")
    hist.index = hist.index.floor("T")

    hist = hist.rename_axis("ts").reset_index()

    hist = hist.rename(columns={
        "Open": "open",
        "High": "high",
        "Low":  "low",
        "Close": "close",
        "Volume": "volume",
    })[["ts", "open", "high", "low", "close", "volume"]]

    hist["volume"] = hist["volume"].fillna(0).astype("int64")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # metadata tables
            cur.execute(
                "INSERT INTO market_data.names (ticker, name) VALUES (%s,%s) "
                "ON CONFLICT (ticker) DO UPDATE SET name=EXCLUDED.name;",
                (ticker, name),
            )
            cur.execute(
                "INSERT INTO market_data.sources (ticker, source) VALUES (%s,%s) "
                "ON CONFLICT (ticker) DO UPDATE SET source=EXCLUDED.source;",
                (ticker, source),
            )

            # get last timestamp in per-ticker table
            cur.execute(f"SELECT max(ts) FROM market_data.{table};")
            last_ts = cur.fetchone()[0]

            if last_ts is not None:
                hist = hist[hist["ts"] > last_ts]

            if hist.empty:
                print(f"[{ticker} -> {table}] no new rows (last_ts={last_ts})")
                conn.commit()
                return

            rows = [
                (
                    pd.Timestamp(r.ts).to_pydatetime(),
                    float(r.open),
                    float(r.high),
                    float(r.low),
                    float(r.close),
                    int(r.volume),
                )
                for r in hist.itertuples(index=False)
            ]

            sql = f"""
            INSERT INTO market_data.{table}
                (ts, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (ts) DO UPDATE SET
                open  = EXCLUDED.open,
                high  = EXCLUDED.high,
                low   = EXCLUDED.low,
                close = EXCLUDED.close,
                volume= EXCLUDED.volume;
            """

            execute_values(cur, sql, rows, page_size=2000)

        conn.commit()

    print(f"[{ticker} -> {table}] inserted_or_updated={len(rows)} new rows")
