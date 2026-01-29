#!/usr/bin/env python3
"""
Standalone Yahoo Finance Backfill Script
=========================================

Run this script directly to backfill the closing_prices table
with historical data from Yahoo Finance.

Usage (inside Docker container):
    docker-compose exec airflow-worker python /opt/airflow/scripts/run_backfill.py

Or with custom parameters:
    docker-compose exec airflow-worker python /opt/airflow/scripts/run_backfill.py --period 2y

Arguments:
    --period: Yahoo Finance period (1mo, 3mo, 6mo, 1y, 2y, 5y, max)
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add plugins to path
sys.path.insert(0, str(Path(__file__).parent.parent / "plugins"))

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import yfinance as yf


def load_tickers():
    """Load tickers from CSV."""
    import csv

    csv_path = Path(__file__).parent.parent / "data" / "info" / "tickers_list.csv"

    special_cases = {"^GSPC": "sp500", "^NDX": "nasdaq100", "^DJI": "dow"}

    tickers = []
    seen = set()

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yf_ticker = row["yf_ticker"].strip()
            alias = row["Ticker"].strip()

            # Normalize column name
            if alias in special_cases:
                column = special_cases[alias]
            else:
                column = alias.lower().replace("-", "_").replace("^", "").replace(" ", "_")
                if column and column[0].isdigit():
                    column = "t_" + column

            if column in seen:
                continue

            tickers.append({
                "yf_ticker": yf_ticker,
                "alias": alias,
                "column": column,
            })
            seen.add(column)

    return tickers


def run_yahoo_backfill(host: str, port: int, user: str, password: str, database: str, period: str):
    """
    Backfill historical data from Yahoo Finance.

    Uses ON CONFLICT DO NOTHING to never overwrite existing data.
    Excludes CURRENT_DATE to preserve immutability of historical rows.
    """
    tickers = load_tickers()
    ticker_columns = [t["column"] for t in tickers]
    yf_tickers = [t["yf_ticker"] for t in tickers]

    print(f"Loaded {len(tickers)} tickers")
    print(f"Downloading historical data for period={period}...")

    today = datetime.now(timezone.utc).date()

    # Download all tickers at once
    try:
        data = yf.download(
            tickers=yf_tickers,
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=True,
            threads=True,
        )
    except Exception as e:
        print(f"ERROR: Download failed: {e}")
        return

    if data.empty:
        print("ERROR: No data returned from Yahoo Finance")
        return

    print(f"Downloaded {len(data)} trading days")

    # Extract Close prices
    close_data = data["Close"]

    # Build rows (excluding today)
    rows_to_insert = []
    for idx in close_data.index:
        row_date = idx.date()

        # CRITICAL: Skip today - only insert historical dates
        if row_date >= today:
            print(f"  Skipping {row_date} (current date)")
            continue

        row_values = {"date": row_date}

        for ticker_info in tickers:
            yf_ticker = ticker_info["yf_ticker"]
            column = ticker_info["column"]

            try:
                if len(yf_tickers) == 1:
                    price = close_data.loc[idx]
                else:
                    price = close_data.loc[idx, yf_ticker]

                if pd.notna(price):
                    row_values[column] = float(price)
                else:
                    row_values[column] = None
            except Exception:
                row_values[column] = None

        rows_to_insert.append(row_values)

    print(f"\nPrepared {len(rows_to_insert)} historical rows")

    if not rows_to_insert:
        print("No historical data to insert!")
        return

    # Connect to database
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    print(f"Connected to {host}:{port}/{database}")

    # Build INSERT with ON CONFLICT DO NOTHING (never overwrite)
    columns_list = ["date"] + ticker_columns

    insert_sql = f"""
        INSERT INTO market_data.closing_prices ({", ".join(columns_list)})
        VALUES %s
        ON CONFLICT (date) DO NOTHING
    """

    values = [
        tuple(row.get(col) for col in columns_list)
        for row in rows_to_insert
    ]

    # Execute in batches
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=100)
        rows_affected = cur.rowcount

    conn.commit()

    date_min = min(r["date"] for r in rows_to_insert)
    date_max = max(r["date"] for r in rows_to_insert)

    print(f"\n=== BACKFILL COMPLETE ===")
    print(f"Rows inserted: {rows_affected}")
    print(f"Rows attempted: {len(rows_to_insert)}")
    print(f"Date range: {date_min} to {date_max}")

    # Verify
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM market_data.closing_prices")
        total_rows = cur.fetchone()[0]
        print(f"Total rows in table: {total_rows}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill closing_prices from Yahoo Finance")
    parser.add_argument("--host", default="postgres", help="Database host (default: postgres for Docker)")
    parser.add_argument("--port", type=int, default=5432, help="Database port (default: 5432 for Docker internal)")
    parser.add_argument("--user", default="airflow", help="Database user")
    parser.add_argument("--password", default="airflow", help="Database password")
    parser.add_argument("--database", default="airflow", help="Database name")
    parser.add_argument("--period", default="1y", help="Yahoo Finance period (1mo, 3mo, 6mo, 1y, 2y, 5y, max)")

    args = parser.parse_args()

    run_yahoo_backfill(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        period=args.period,
    )
