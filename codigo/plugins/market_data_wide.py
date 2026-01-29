"""
Market Data Wide Format Module
===============================

This module provides functionality for the wide-format closing_prices table
where each row is a date and each column is a ticker's closing price.

Table structure:
    market_data.closing_prices (
        date DATE PRIMARY KEY,
        aapl NUMERIC(18,6),
        msft NUMERIC(18,6),
        ... (153 ticker columns)
    )
"""

from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---------------------------------------------------------------------
# 1) Load tickers from CSV and normalize column names
# ---------------------------------------------------------------------

def normalize_column_name(alias: str) -> str:
    """
    Convert a ticker alias to a valid PostgreSQL column name.

    Examples:
        'BRK-B' -> 'brk_b'
        '^GSPC' -> 'sp500'
        'AAPL' -> 'aapl'
    """
    # Special cases for index tickers
    special_cases = {
        "^GSPC": "sp500",
        "^NDX": "nasdaq100",
        "^DJI": "dow",
    }

    if alias in special_cases:
        return special_cases[alias]

    col = alias.lower()
    col = col.replace("-", "_").replace("^", "").replace(" ", "_")

    # If starts with digit, prefix with 't_'
    if col and col[0].isdigit():
        col = "t_" + col

    return col


def normalize_old_table_name(alias: str) -> str:
    """
    Replicate the OLD table naming convention from market_data_shared.py.
    This ensures we can find the existing tables during backfill.
    """
    table = alias.lower()
    table = table.replace("-", "_").replace(" ", "_")
    if table and table[0].isdigit():
        table = "t_" + table
    return table


def load_tickers_from_csv() -> list[dict]:
    """
    Load tickers from CSV file and return list of dicts with:
    - yf_ticker: Yahoo Finance ticker (e.g., '^GSPC', 'BRK-B')
    - alias: Original alias from CSV
    - column: Normalized PostgreSQL column name (NEW)
    - old_table: Old table name for backfill compatibility
    """
    csv_path = Path("data/info/tickers_list.csv")
    tickers = []
    seen_columns = set()

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yf_ticker = row["yf_ticker"].strip()
            alias = row["Ticker"].strip()
            column = normalize_column_name(alias)
            old_table = normalize_old_table_name(alias)

            # Skip duplicates
            if column in seen_columns:
                continue

            tickers.append({
                "yf_ticker": yf_ticker,
                "alias": alias,
                "column": column,
                "old_table": old_table,  # Explicit mapping to old table
            })
            seen_columns.add(column)

    return tickers


# Load tickers at module import time
TICKERS = load_tickers_from_csv()
TICKER_COLUMNS = [t["column"] for t in TICKERS]
YF_TO_COLUMN = {t["yf_ticker"]: t["column"] for t in TICKERS}
COLUMN_TO_YF = {t["column"]: t["yf_ticker"] for t in TICKERS}


# ---------------------------------------------------------------------
# 2) DDL Generation
# ---------------------------------------------------------------------

def generate_closing_prices_ddl() -> str:
    """
    Generate the CREATE TABLE DDL for market_data.closing_prices.
    Dynamically creates columns for all tickers in the CSV.
    """
    columns = [f"    {col} NUMERIC(18,6)" for col in TICKER_COLUMNS]
    columns_sql = ",\n".join(columns)

    ddl = f"""
CREATE SCHEMA IF NOT EXISTS market_data;

DROP TABLE IF EXISTS market_data.closing_prices CASCADE;

CREATE TABLE market_data.closing_prices (
    date DATE PRIMARY KEY,
{columns_sql}
);

-- Index for efficient date range queries
CREATE INDEX idx_closing_prices_date_desc
ON market_data.closing_prices (date DESC);

COMMENT ON TABLE market_data.closing_prices IS
'Wide-format table with one row per trading day and one column per ticker (closing prices only)';
"""
    return ddl


# ---------------------------------------------------------------------
# 3) Upsert function for daily/intraday updates
# ---------------------------------------------------------------------

def upsert_closing_prices_row(
    target_date: date | None = None,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Fetch current closing prices for ALL tickers and upsert a single row.

    This function:
    1. Downloads price data for all tickers using yfinance
    2. Extracts the closing price for the target date
    3. Upserts a single row into closing_prices table

    Args:
        target_date: Date to update (defaults to today UTC)
        postgres_conn_id: Airflow connection ID for PostgreSQL
        database: Database name

    Returns:
        Dict with stats: {tickers_fetched, tickers_failed, date}
    """
    import yfinance as yf

    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    # HISTORICAL DATA PROTECTION: Block updates to past dates
    today = datetime.now(timezone.utc).date()
    if target_date < today:
        print(f"[upsert_closing_prices_row] BLOCKED: Refusing to modify historical date {target_date}")
        return {
            "tickers_fetched": 0,
            "tickers_failed": 0,
            "date": str(target_date),
            "status": "blocked_historical",
        }

    print(f"[upsert_closing_prices_row] Fetching prices for {len(TICKERS)} tickers, date={target_date}")

    # Fetch all tickers at once using yfinance download
    yf_tickers = [t["yf_ticker"] for t in TICKERS]

    try:
        # Download with 5 day period to ensure we get data even on weekends
        data = yf.download(
            tickers=yf_tickers,
            period="5d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as e:
        print(f"[upsert_closing_prices_row] yfinance download failed: {e}")
        return {"tickers_fetched": 0, "tickers_failed": len(TICKERS), "date": str(target_date)}

    if data.empty:
        print("[upsert_closing_prices_row] No data returned from yfinance")
        return {"tickers_fetched": 0, "tickers_failed": len(TICKERS), "date": str(target_date)}

    # Extract closing prices
    prices = {}
    tickers_failed = 0

    # Handle single ticker vs multiple tickers (different DataFrame structure)
    if len(yf_tickers) == 1:
        # Single ticker: columns are just 'Open', 'High', etc.
        close_data = data["Close"]
    else:
        # Multiple tickers: columns are MultiIndex (field, ticker)
        close_data = data["Close"]

    for ticker_info in TICKERS:
        yf_ticker = ticker_info["yf_ticker"]
        column = ticker_info["column"]

        try:
            if len(yf_tickers) == 1:
                ticker_close = close_data
            else:
                ticker_close = close_data[yf_ticker]

            # Get the most recent price (might not be exactly target_date)
            if not ticker_close.empty:
                latest_price = ticker_close.dropna().iloc[-1]
                prices[column] = float(latest_price)
            else:
                prices[column] = None
                tickers_failed += 1
        except Exception as e:
            print(f"[upsert_closing_prices_row] Failed to get price for {yf_ticker}: {e}")
            prices[column] = None
            tickers_failed += 1

    tickers_fetched = len(TICKERS) - tickers_failed
    print(f"[upsert_closing_prices_row] Fetched {tickers_fetched}/{len(TICKERS)} prices")

    # Build the UPSERT SQL
    columns_list = ["date"] + TICKER_COLUMNS
    placeholders = ["%s"] * len(columns_list)

    # Build UPDATE SET clause for all ticker columns
    update_sets = [f"{col} = EXCLUDED.{col}" for col in TICKER_COLUMNS]

    sql = f"""
    INSERT INTO market_data.closing_prices ({", ".join(columns_list)})
    VALUES ({", ".join(placeholders)})
    ON CONFLICT (date) DO UPDATE SET
        {", ".join(update_sets)}
    """

    # Build values tuple
    values = [target_date] + [prices.get(col) for col in TICKER_COLUMNS]

    # Execute
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()

    print(f"[upsert_closing_prices_row] Upserted row for date={target_date}")

    return {
        "tickers_fetched": tickers_fetched,
        "tickers_failed": tickers_failed,
        "date": str(target_date),
    }


# ---------------------------------------------------------------------
# 4) Backfill function from existing tables
# ---------------------------------------------------------------------

def backfill_from_existing_tables(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
    limit_days: int | None = None,
) -> dict[str, Any]:
    """
    Backfill the closing_prices table from existing per-ticker tables.

    This function:
    1. Queries each existing market_data.{ticker} table
    2. Extracts date and closing price
    3. Pivots all data into wide format
    4. Bulk inserts into closing_prices

    Args:
        postgres_conn_id: Airflow connection ID
        database: Database name
        limit_days: If set, only backfill last N days (for testing)

    Returns:
        Dict with stats: {tables_read, rows_inserted, date_range}
    """
    from psycopg2.extras import execute_values

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    print(f"[backfill] Starting backfill from {len(TICKERS)} existing tables")
    if limit_days:
        print(f"[backfill] Limiting to last {limit_days} days")

    # Collect data from all existing tables
    all_data = []
    tables_read = 0
    tables_missing = 0
    tables_empty = 0

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for i, ticker_info in enumerate(TICKERS):
                column = ticker_info["column"]
                old_table = ticker_info["old_table"]  # Use explicit old table name

                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'market_data'
                        AND table_name = %s
                    )
                """, (old_table,))

                exists = cur.fetchone()[0]
                if not exists:
                    tables_missing += 1
                    if tables_missing <= 5:  # Only log first 5 missing
                        print(f"[backfill] Table market_data.{old_table} does not exist")
                    continue

                # Fetch date and close price
                try:
                    date_filter = ""
                    if limit_days:
                        date_filter = f"AND ts::date >= CURRENT_DATE - INTERVAL '{limit_days} days'"

                    cur.execute(f"""
                        SELECT ts::date as date, close
                        FROM market_data.{old_table}
                        WHERE close IS NOT NULL
                        {date_filter}
                        ORDER BY ts
                    """)
                    rows = cur.fetchall()

                    if not rows:
                        tables_empty += 1
                        continue

                    for row in rows:
                        all_data.append({
                            "date": row[0],
                            "column": column,
                            "close": float(row[1]),
                        })

                    tables_read += 1

                    # Progress logging
                    if tables_read % 25 == 0:
                        print(f"[backfill] Progress: {tables_read} tables read, {len(all_data)} records collected")

                except Exception as e:
                    print(f"[backfill] ERROR reading market_data.{old_table}: {e}")
                    continue

    print(f"[backfill] Summary: {tables_read} tables read, {tables_missing} missing, {tables_empty} empty")
    print(f"[backfill] Total records collected: {len(all_data)}")

    if not all_data:
        print("[backfill] No data found in existing tables")
        return {"tables_read": 0, "rows_inserted": 0, "date_range": None}

    print(f"[backfill] Read {tables_read} tables, {len(all_data)} total price records")

    # Convert to DataFrame and pivot
    df = pd.DataFrame(all_data)

    # Pivot: rows=date, columns=ticker column names, values=close
    pivot_df = df.pivot_table(
        index="date",
        columns="column",
        values="close",
        aggfunc="last",  # If multiple entries per day, take last
    ).reset_index()

    # Ensure all ticker columns exist (fill missing with None)
    for col in TICKER_COLUMNS:
        if col not in pivot_df.columns:
            pivot_df[col] = None

    # Reorder columns to match DDL
    pivot_df = pivot_df[["date"] + TICKER_COLUMNS]

    print(f"[backfill] Pivoted to {len(pivot_df)} rows (unique dates)")

    # Bulk insert
    columns_list = ["date"] + TICKER_COLUMNS

    # Build INSERT with ON CONFLICT
    placeholders = ", ".join(["%s"] * len(columns_list))
    update_sets = ", ".join([f"{col} = EXCLUDED.{col}" for col in TICKER_COLUMNS])

    insert_sql = f"""
        INSERT INTO market_data.closing_prices ({", ".join(columns_list)})
        VALUES %s
        ON CONFLICT (date) DO UPDATE SET {update_sets}
    """

    # Convert DataFrame to list of tuples
    values = [
        tuple(row[col] if pd.notna(row[col]) else None for col in columns_list)
        for _, row in pivot_df.iterrows()
    ]

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=500)
        conn.commit()

    date_min = pivot_df["date"].min()
    date_max = pivot_df["date"].max()

    print(f"[backfill] Inserted {len(values)} rows, date range: {date_min} to {date_max}")

    return {
        "tables_read": tables_read,
        "rows_inserted": len(values),
        "date_range": f"{date_min} to {date_max}",
    }


# ---------------------------------------------------------------------
# 5) Market hours check (reused from original)
# ---------------------------------------------------------------------

MARKET_OPEN_UTC_HOUR = 14
MARKET_OPEN_UTC_MINUTE = 30
MARKET_CLOSE_UTC_HOUR = 21
MARKET_CLOSE_UTC_MINUTE = 0


def is_market_hours() -> bool:
    """
    Check if current UTC time is within NYSE market hours.

    Returns True if within market hours (14:30-21:00 UTC, weekdays).
    """
    now_utc = datetime.now(timezone.utc)

    # Skip weekends
    if now_utc.weekday() >= 5:
        print(f"[market_hours_check] Skipping: weekend (weekday={now_utc.weekday()})")
        return False

    current_minutes = now_utc.hour * 60 + now_utc.minute
    market_open_minutes = MARKET_OPEN_UTC_HOUR * 60 + MARKET_OPEN_UTC_MINUTE
    market_close_minutes = MARKET_CLOSE_UTC_HOUR * 60 + MARKET_CLOSE_UTC_MINUTE

    is_open = market_open_minutes <= current_minutes < market_close_minutes

    print(f"[market_hours_check] UTC time: {now_utc.strftime('%H:%M')}, "
          f"market window: {MARKET_OPEN_UTC_HOUR:02d}:{MARKET_OPEN_UTC_MINUTE:02d}-"
          f"{MARKET_CLOSE_UTC_HOUR:02d}:{MARKET_CLOSE_UTC_MINUTE:02d}, "
          f"is_open: {is_open}")

    return is_open


# ---------------------------------------------------------------------
# 6) Utility: Get current table stats
# ---------------------------------------------------------------------

def get_closing_prices_stats(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """Get statistics about the closing_prices table."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Row count
            cur.execute("SELECT COUNT(*) FROM market_data.closing_prices")
            row_count = cur.fetchone()[0]

            # Date range
            cur.execute("""
                SELECT MIN(date), MAX(date)
                FROM market_data.closing_prices
            """)
            date_min, date_max = cur.fetchone()

            # Sample: latest row AAPL and MSFT
            cur.execute("""
                SELECT date, aapl, msft, googl, sp500
                FROM market_data.closing_prices
                ORDER BY date DESC
                LIMIT 1
            """)
            latest = cur.fetchone()

    return {
        "row_count": row_count,
        "date_range": f"{date_min} to {date_max}" if date_min else None,
        "latest_row": {
            "date": str(latest[0]) if latest else None,
            "aapl": float(latest[1]) if latest and latest[1] else None,
            "msft": float(latest[2]) if latest and latest[2] else None,
            "googl": float(latest[3]) if latest and latest[3] else None,
            "sp500": float(latest[4]) if latest and latest[4] else None,
        } if latest else None,
    }


# ---------------------------------------------------------------------
# 7) Yahoo Finance Historical Backfill
# ---------------------------------------------------------------------

def backfill_from_yahoo(
    period: str = "1y",
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Backfill historical closing prices from Yahoo Finance.

    This function:
    1. Downloads historical data for all tickers using yfinance
    2. Excludes CURRENT_DATE (only inserts past dates)
    3. Uses ON CONFLICT DO NOTHING to preserve existing data
    4. Never overwrites historical rows

    Args:
        period: Yahoo Finance period string (e.g., '1y', '2y', 'max')
        postgres_conn_id: Airflow connection ID (ignored if conn provided)
        database: Database name

    Returns:
        Dict with stats: {rows_inserted, date_range, tickers_processed}
    """
    import yfinance as yf
    from psycopg2.extras import execute_values

    today = datetime.now(timezone.utc).date()
    print(f"[backfill_from_yahoo] Starting backfill for period={period}, excluding {today}")

    # Fetch all tickers
    yf_tickers = [t["yf_ticker"] for t in TICKERS]
    print(f"[backfill_from_yahoo] Downloading {len(yf_tickers)} tickers...")

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
        print(f"[backfill_from_yahoo] Download failed: {e}")
        return {"rows_inserted": 0, "error": str(e)}

    if data.empty:
        print("[backfill_from_yahoo] No data returned")
        return {"rows_inserted": 0, "error": "No data returned"}

    print(f"[backfill_from_yahoo] Downloaded {len(data)} trading days")

    # Extract Close prices and build rows
    close_data = data["Close"]
    rows_to_insert = []

    for idx in close_data.index:
        row_date = idx.date()

        # CRITICAL: Skip today - only insert historical dates
        if row_date >= today:
            continue

        row_values = {"date": row_date}
        for ticker_info in TICKERS:
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

    print(f"[backfill_from_yahoo] Prepared {len(rows_to_insert)} historical rows")

    if not rows_to_insert:
        return {"rows_inserted": 0, "error": "No historical data to insert"}

    # Build INSERT with ON CONFLICT DO NOTHING (never overwrite)
    columns_list = ["date"] + TICKER_COLUMNS

    insert_sql = f"""
        INSERT INTO market_data.closing_prices ({", ".join(columns_list)})
        VALUES %s
        ON CONFLICT (date) DO NOTHING
    """

    values = [
        tuple(row.get(col) for col in columns_list)
        for row in rows_to_insert
    ]

    # Execute
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=100)
            rows_affected = cur.rowcount
        conn.commit()

    date_min = min(r["date"] for r in rows_to_insert)
    date_max = max(r["date"] for r in rows_to_insert)

    print(f"[backfill_from_yahoo] Inserted {rows_affected} rows, range: {date_min} to {date_max}")

    return {
        "rows_inserted": rows_affected,
        "rows_attempted": len(rows_to_insert),
        "date_range": f"{date_min} to {date_max}",
        "tickers_processed": len(TICKERS),
    }


# ---------------------------------------------------------------------
# 8) DDL Generation for High and Low prices
# ---------------------------------------------------------------------

def generate_high_prices_ddl() -> str:
    """
    Generate the CREATE TABLE DDL for market_data.high_prices.
    Identical schema to closing_prices but stores daily high prices.
    """
    columns = [f"    {col} NUMERIC(18,6)" for col in TICKER_COLUMNS]
    columns_sql = ",\n".join(columns)

    ddl = f"""
CREATE SCHEMA IF NOT EXISTS market_data;

DROP TABLE IF EXISTS market_data.high_prices CASCADE;

CREATE TABLE market_data.high_prices (
    date DATE PRIMARY KEY,
{columns_sql}
);

CREATE INDEX idx_high_prices_date_desc
ON market_data.high_prices (date DESC);

COMMENT ON TABLE market_data.high_prices IS
'Wide-format table with one row per trading day and one column per ticker (daily high prices)';
"""
    return ddl


def generate_low_prices_ddl() -> str:
    """
    Generate the CREATE TABLE DDL for market_data.low_prices.
    Identical schema to closing_prices but stores daily low prices.
    """
    columns = [f"    {col} NUMERIC(18,6)" for col in TICKER_COLUMNS]
    columns_sql = ",\n".join(columns)

    ddl = f"""
CREATE SCHEMA IF NOT EXISTS market_data;

DROP TABLE IF EXISTS market_data.low_prices CASCADE;

CREATE TABLE market_data.low_prices (
    date DATE PRIMARY KEY,
{columns_sql}
);

CREATE INDEX idx_low_prices_date_desc
ON market_data.low_prices (date DESC);

COMMENT ON TABLE market_data.low_prices IS
'Wide-format table with one row per trading day and one column per ticker (daily low prices)';
"""
    return ddl


# ---------------------------------------------------------------------
# 9) Upsert helpers for High / Low / combined
# ---------------------------------------------------------------------

def _upsert_wide_row(
    table_name: str,
    yf_column: str,
    target_date: date | None = None,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
    data: Any = None,
) -> dict[str, Any]:
    """
    Internal helper: extract *yf_column* from a yfinance DataFrame and
    upsert a single row into *market_data.<table_name>*.

    If *data* is supplied (a pre-downloaded yfinance DataFrame) the
    download step is skipped.  This allows a single ``yf.download()``
    call to feed all three wide tables.
    """
    import yfinance as yf

    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    today = datetime.now(timezone.utc).date()
    if target_date < today:
        print(f"[upsert_{table_name}] BLOCKED: Refusing to modify historical date {target_date}")
        return {
            "tickers_fetched": 0,
            "tickers_failed": 0,
            "date": str(target_date),
            "status": "blocked_historical",
        }

    print(f"[upsert_{table_name}] Processing {len(TICKERS)} tickers, date={target_date}")

    yf_tickers = [t["yf_ticker"] for t in TICKERS]

    if data is None:
        try:
            data = yf.download(
                tickers=yf_tickers,
                period="5d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=True,
            )
        except Exception as e:
            print(f"[upsert_{table_name}] yfinance download failed: {e}")
            return {"tickers_fetched": 0, "tickers_failed": len(TICKERS), "date": str(target_date)}

    if data.empty:
        print(f"[upsert_{table_name}] No data returned from yfinance")
        return {"tickers_fetched": 0, "tickers_failed": len(TICKERS), "date": str(target_date)}

    prices: dict[str, float | None] = {}
    tickers_failed = 0

    price_data = data[yf_column]

    for ticker_info in TICKERS:
        yf_ticker = ticker_info["yf_ticker"]
        column = ticker_info["column"]

        try:
            if len(yf_tickers) == 1:
                ticker_series = price_data
            else:
                ticker_series = price_data[yf_ticker]

            if not ticker_series.empty:
                latest_price = ticker_series.dropna().iloc[-1]
                prices[column] = float(latest_price)
            else:
                prices[column] = None
                tickers_failed += 1
        except Exception as e:
            print(f"[upsert_{table_name}] Failed to get price for {yf_ticker}: {e}")
            prices[column] = None
            tickers_failed += 1

    tickers_fetched = len(TICKERS) - tickers_failed
    print(f"[upsert_{table_name}] Fetched {tickers_fetched}/{len(TICKERS)} prices")

    columns_list = ["date"] + TICKER_COLUMNS
    placeholders = ["%s"] * len(columns_list)
    update_sets = [f"{col} = EXCLUDED.{col}" for col in TICKER_COLUMNS]

    sql = f"""
    INSERT INTO market_data.{table_name} ({", ".join(columns_list)})
    VALUES ({", ".join(placeholders)})
    ON CONFLICT (date) DO UPDATE SET
        {", ".join(update_sets)}
    """

    values = [target_date] + [prices.get(col) for col in TICKER_COLUMNS]

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()

    print(f"[upsert_{table_name}] Upserted row for date={target_date}")

    return {
        "tickers_fetched": tickers_fetched,
        "tickers_failed": tickers_failed,
        "date": str(target_date),
    }


def upsert_high_prices_row(
    target_date: date | None = None,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
    data: Any = None,
) -> dict[str, Any]:
    """Fetch current high prices for ALL tickers and upsert a single row."""
    return _upsert_wide_row("high_prices", "High", target_date, postgres_conn_id, database, data)


def upsert_low_prices_row(
    target_date: date | None = None,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
    data: Any = None,
) -> dict[str, Any]:
    """Fetch current low prices for ALL tickers and upsert a single row."""
    return _upsert_wide_row("low_prices", "Low", target_date, postgres_conn_id, database, data)


def upsert_all_wide_prices_row(
    target_date: date | None = None,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Download price data ONCE and upsert into all three wide tables:
    closing_prices (Close), high_prices (High), low_prices (Low).

    A single ``yf.download()`` call feeds all three upserts.
    """
    import yfinance as yf

    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    today = datetime.now(timezone.utc).date()
    if target_date < today:
        print(f"[upsert_all] BLOCKED: Refusing to modify historical date {target_date}")
        return {"status": "blocked_historical", "date": str(target_date)}

    yf_tickers = [t["yf_ticker"] for t in TICKERS]

    try:
        data = yf.download(
            tickers=yf_tickers,
            period="5d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as e:
        print(f"[upsert_all] yfinance download failed: {e}")
        return {"status": "download_failed", "error": str(e)}

    if data.empty:
        print("[upsert_all] No data returned")
        return {"status": "empty_download"}

    close_result = _upsert_wide_row(
        "closing_prices", "Close", target_date, postgres_conn_id, database, data,
    )
    high_result = _upsert_wide_row(
        "high_prices", "High", target_date, postgres_conn_id, database, data,
    )
    low_result = _upsert_wide_row(
        "low_prices", "Low", target_date, postgres_conn_id, database, data,
    )

    return {
        "closing_prices": close_result,
        "high_prices": high_result,
        "low_prices": low_result,
        "date": str(target_date),
    }


# ---------------------------------------------------------------------
# 10) Stats helpers for High and Low prices
# ---------------------------------------------------------------------

def _get_wide_prices_stats(
    table_name: str,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """Internal helper: get statistics about a wide-format prices table."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM market_data.{table_name}")
            row_count = cur.fetchone()[0]

            cur.execute(f"""
                SELECT MIN(date), MAX(date)
                FROM market_data.{table_name}
            """)
            date_min, date_max = cur.fetchone()

            cur.execute(f"""
                SELECT date, aapl, msft, googl, sp500
                FROM market_data.{table_name}
                ORDER BY date DESC
                LIMIT 1
            """)
            latest = cur.fetchone()

    return {
        "row_count": row_count,
        "date_range": f"{date_min} to {date_max}" if date_min else None,
        "latest_row": {
            "date": str(latest[0]) if latest else None,
            "aapl": float(latest[1]) if latest and latest[1] else None,
            "msft": float(latest[2]) if latest and latest[2] else None,
            "googl": float(latest[3]) if latest and latest[3] else None,
            "sp500": float(latest[4]) if latest and latest[4] else None,
        } if latest else None,
    }


def get_high_prices_stats(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """Get statistics about the high_prices table."""
    return _get_wide_prices_stats("high_prices", postgres_conn_id, database)


def get_low_prices_stats(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """Get statistics about the low_prices table."""
    return _get_wide_prices_stats("low_prices", postgres_conn_id, database)


# ---------------------------------------------------------------------
# 11) Yahoo Finance Historical Backfill for High and Low prices
# ---------------------------------------------------------------------

def _backfill_wide_from_yahoo(
    table_name: str,
    yf_column: str,
    period: str = "1y",
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Internal helper: backfill a wide-format table from Yahoo Finance.

    Identical to ``backfill_from_yahoo()`` except the target table and
    the yfinance column extracted are parameterised.
    """
    import yfinance as yf
    from psycopg2.extras import execute_values

    today = datetime.now(timezone.utc).date()
    print(f"[backfill_{table_name}] Starting backfill for period={period}, excluding {today}")

    yf_tickers = [t["yf_ticker"] for t in TICKERS]
    print(f"[backfill_{table_name}] Downloading {len(yf_tickers)} tickers...")

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
        print(f"[backfill_{table_name}] Download failed: {e}")
        return {"rows_inserted": 0, "error": str(e)}

    if data.empty:
        print(f"[backfill_{table_name}] No data returned")
        return {"rows_inserted": 0, "error": "No data returned"}

    print(f"[backfill_{table_name}] Downloaded {len(data)} trading days")

    price_data = data[yf_column]
    rows_to_insert = []

    for idx in price_data.index:
        row_date = idx.date()

        if row_date >= today:
            continue

        row_values: dict[str, Any] = {"date": row_date}
        for ticker_info in TICKERS:
            yf_ticker = ticker_info["yf_ticker"]
            column = ticker_info["column"]

            try:
                if len(yf_tickers) == 1:
                    price = price_data.loc[idx]
                else:
                    price = price_data.loc[idx, yf_ticker]

                if pd.notna(price):
                    row_values[column] = float(price)
                else:
                    row_values[column] = None
            except Exception:
                row_values[column] = None

        rows_to_insert.append(row_values)

    print(f"[backfill_{table_name}] Prepared {len(rows_to_insert)} historical rows")

    if not rows_to_insert:
        return {"rows_inserted": 0, "error": "No historical data to insert"}

    columns_list = ["date"] + TICKER_COLUMNS

    insert_sql = f"""
        INSERT INTO market_data.{table_name} ({", ".join(columns_list)})
        VALUES %s
        ON CONFLICT (date) DO NOTHING
    """

    values = [
        tuple(row.get(col) for col in columns_list)
        for row in rows_to_insert
    ]

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=100)
            rows_affected = cur.rowcount
        conn.commit()

    date_min = min(r["date"] for r in rows_to_insert)
    date_max = max(r["date"] for r in rows_to_insert)

    print(f"[backfill_{table_name}] Inserted {rows_affected} rows, range: {date_min} to {date_max}")

    return {
        "rows_inserted": rows_affected,
        "rows_attempted": len(rows_to_insert),
        "date_range": f"{date_min} to {date_max}",
        "tickers_processed": len(TICKERS),
    }


def backfill_high_prices_from_yahoo(
    period: str = "1y",
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """Backfill historical daily high prices from Yahoo Finance."""
    return _backfill_wide_from_yahoo("high_prices", "High", period, postgres_conn_id, database)


def backfill_low_prices_from_yahoo(
    period: str = "1y",
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """Backfill historical daily low prices from Yahoo Finance."""
    return _backfill_wide_from_yahoo("low_prices", "Low", period, postgres_conn_id, database)


def backfill_closing_prices_from_yahoo(
    period: str = "1y",
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """Backfill historical daily closing prices from Yahoo Finance."""
    return _backfill_wide_from_yahoo("closing_prices", "Close", period, postgres_conn_id, database)
