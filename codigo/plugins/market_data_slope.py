"""
Market Data Slope Module
=========================

This module calculates and stores the slope of adjusted prices over different
lookback periods. Slope is calculated using linear regression:

    m = Cov(x, y) / Var(x)

Where:
    x = day index (0, 1, 2, ..., n-1)
    y = adj_close prices for those days

Tables created (wide format, same schema as closing_prices):
    market_data.slope_10d   - 10-day slope
    market_data.slope_20d   - 20-day slope
    market_data.slope_30d   - 30-day slope
    market_data.slope_40d   - 40-day slope
    market_data.slope_50d   - 50-day slope
    market_data.slope_60d   - 60-day slope
    market_data.slope_70d   - 70-day slope
    market_data.slope_80d   - 80-day slope
    market_data.slope_90d   - 90-day slope
    market_data.slope_100d  - 100-day slope
    market_data.slope_150d  - 150-day slope
    market_data.slope_200d  - 200-day slope

Each table has:
    - date (PRIMARY KEY)
    - One NUMERIC column per ticker (153 tickers)
    - Values are slope coefficients (can be positive or negative)
"""

from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from typing import Any

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =============================================================================
# CONSTANTS
# =============================================================================

LOOKBACK_PERIODS = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200]

TABLE_NAMES = {period: f"slope_{period}d" for period in LOOKBACK_PERIODS}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def normalize_column_name(ticker: str) -> str:
    """
    Convert a ticker to a valid PostgreSQL column name.

    Examples:
        'AAPL' -> 'aapl'
        'BRK-B' -> 'brk_b'
        '^GSPC' -> 'sp500'
    """
    special_cases = {
        "^GSPC": "sp500",
        "^NDX": "nasdaq100",
        "^DJI": "dow",
    }

    if ticker in special_cases:
        return special_cases[ticker]

    col = ticker.lower()
    col = col.replace("-", "_").replace("^", "").replace(".", "_").replace(" ", "_")

    if col and col[0].isdigit():
        col = "t_" + col

    return col


# =============================================================================
# SLOPE CALCULATION
# =============================================================================

def calculate_slope(prices: list | np.ndarray) -> float | None:
    """
    Calculate slope using linear regression: m = Cov(x,y) / Var(x)

    Args:
        prices: list or array of adj_close values (oldest to newest)

    Returns:
        float: slope value, or None if insufficient data or zero variance
    """
    if prices is None or len(prices) < 2:
        return None

    try:
        x = np.arange(len(prices), dtype=float)
        y = np.array(prices, dtype=float)

        # Remove NaN values
        mask = ~np.isnan(y)
        if mask.sum() < 2:
            return None

        x = x[mask]
        y = y[mask]

        var_x = np.var(x, ddof=0)
        if var_x == 0:
            return None

        cov_xy = np.cov(x, y, ddof=0)[0, 1]
        slope = cov_xy / var_x

        return float(slope)

    except Exception as e:
        print(f"[calculate_slope] Error: {e}")
        return None


# =============================================================================
# TICKER AND COLUMN MAPPING
# =============================================================================

def get_tickers_and_columns(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> list[tuple[str, str, int]]:
    """
    Get list of tickers, their normalized column names, and security_ids.

    Returns:
        list of (yf_ticker, column_name, security_id) tuples
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT yf_ticker, column_name, security_id
                FROM market_data.securities
                WHERE is_active = TRUE
                ORDER BY security_id
            """)
            rows = cur.fetchall()

    result = []
    for yf_ticker, column_name, security_id in rows:
        # Use the column_name from the database if available
        col = column_name if column_name else normalize_column_name(yf_ticker)
        result.append((yf_ticker, col, security_id))

    print(f"[get_tickers_and_columns] Found {len(result)} tickers")
    return result


def get_security_id_to_column_map(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[int, str]:
    """
    Get mapping from security_id to column_name.

    Returns:
        dict: {security_id: column_name, ...}
    """
    tickers = get_tickers_and_columns(postgres_conn_id, database)
    return {security_id: column_name for _, column_name, security_id in tickers}


def get_column_names(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> list[str]:
    """
    Get list of column names for all tickers.

    Returns:
        list of column names
    """
    tickers = get_tickers_and_columns(postgres_conn_id, database)
    return [column_name for _, column_name, _ in tickers]


# =============================================================================
# DDL GENERATION
# =============================================================================

def generate_slope_table_ddl(
    lookback: int,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> str:
    """
    Generate CREATE TABLE SQL for a slope table.

    Args:
        lookback: int (10, 20, 30, etc.)
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        str: CREATE TABLE SQL statement
    """
    table_name = TABLE_NAMES[lookback]
    column_names = get_column_names(postgres_conn_id, database)

    columns = [f"    {col} NUMERIC(18,10)" for col in column_names]
    columns_sql = ",\n".join(columns)

    ddl = f"""
CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.{table_name} (
    date DATE PRIMARY KEY,
{columns_sql}
);

CREATE INDEX IF NOT EXISTS idx_{table_name}_date_desc
ON market_data.{table_name} (date DESC);

COMMENT ON TABLE market_data.{table_name} IS
'{lookback}-day slope of adjusted close prices for all tickers';
"""
    return ddl


# =============================================================================
# TABLE CREATION
# =============================================================================

def create_slope_table(
    lookback: int,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Create a single slope table if it doesn't exist.

    Args:
        lookback: int (10, 20, 30, etc.)
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        dict with status
    """
    table_name = TABLE_NAMES[lookback]
    print(f"[create_slope_table] Creating {table_name}...")

    ddl = generate_slope_table_ddl(lookback, postgres_conn_id, database)

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

    print(f"[create_slope_table] {table_name} created successfully")
    return {"table": table_name, "status": "created"}


def create_all_slope_tables(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Create all 12 slope tables.

    Returns:
        dict with results for each table
    """
    results = {}

    for lookback in LOOKBACK_PERIODS:
        try:
            result = create_slope_table(lookback, postgres_conn_id, database)
            results[TABLE_NAMES[lookback]] = result
        except Exception as e:
            print(f"[create_all_slope_tables] Error creating {TABLE_NAMES[lookback]}: {e}")
            results[TABLE_NAMES[lookback]] = {"status": "error", "error": str(e)}

    print(f"[create_all_slope_tables] Created {len(results)} tables")
    return results


# =============================================================================
# DATA RETRIEVAL
# =============================================================================

def get_trading_days_from_adjusted(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> list[date]:
    """
    Get all unique trading days from adjusted_prices table.

    Returns:
        list: sorted list of date objects (oldest to newest)
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT date
                FROM market_data.adjusted_prices
                ORDER BY date ASC
            """)
            dates = [row[0] for row in cur.fetchall()]

    print(f"[get_trading_days] Found {len(dates)} trading days")
    return dates


def get_adj_close_for_date_range(
    start_date: date,
    end_date: date,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[int, dict[date, float]]:
    """
    Get adj_close data from adjusted_prices for all securities in date range.

    Args:
        start_date: start of date range (inclusive)
        end_date: end of date range (inclusive)
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        dict: {security_id: {date: adj_close, ...}, ...}
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT security_id, date, adj_close
                FROM market_data.adjusted_prices
                WHERE date >= %s AND date <= %s
                  AND adj_close IS NOT NULL
                ORDER BY security_id, date
            """, (start_date, end_date))
            rows = cur.fetchall()

    # Organize by security_id
    result: dict[int, dict[date, float]] = {}
    for security_id, row_date, adj_close in rows:
        if security_id not in result:
            result[security_id] = {}
        result[security_id][row_date] = float(adj_close)

    return result


def get_prices_for_lookback(
    target_date: date,
    lookback: int,
    trading_days: list[date],
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[int, list[float]]:
    """
    Get the last `lookback` trading days of prices ending on target_date.

    Args:
        target_date: the date we're calculating slope for
        lookback: number of days to look back
        trading_days: list of all trading days (sorted ascending)
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        dict: {security_id: [prices from oldest to newest], ...}
    """
    # Find the index of target_date in trading_days
    try:
        target_idx = trading_days.index(target_date)
    except ValueError:
        print(f"[get_prices_for_lookback] {target_date} not in trading days")
        return {}

    # Need at least `lookback` days before target_date (inclusive)
    if target_idx < lookback - 1:
        print(f"[get_prices_for_lookback] Not enough history for {target_date} with lookback={lookback}")
        return {}

    # Get the date range for this lookback period
    start_idx = target_idx - lookback + 1
    lookback_dates = trading_days[start_idx:target_idx + 1]
    start_date = lookback_dates[0]
    end_date = lookback_dates[-1]

    # Fetch the data
    data = get_adj_close_for_date_range(start_date, end_date, postgres_conn_id, database)

    # Convert to ordered list of prices for each security
    result: dict[int, list[float]] = {}
    for security_id, date_prices in data.items():
        prices = []
        for d in lookback_dates:
            if d in date_prices:
                prices.append(date_prices[d])
            else:
                prices.append(np.nan)
        result[security_id] = prices

    return result


# =============================================================================
# SLOPE CALCULATION FOR ROWS
# =============================================================================

def calculate_slope_row(
    target_date: date,
    lookback: int,
    trading_days: list[date] | None = None,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, float | None]:
    """
    Calculate slope for all tickers for a given date and lookback period.

    Args:
        target_date: date to calculate slope for
        lookback: number of days to look back
        trading_days: optional pre-fetched list of trading days
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        dict: {column_name: slope_value, ...}
    """
    # Get trading days if not provided
    if trading_days is None:
        trading_days = get_trading_days_from_adjusted(postgres_conn_id, database)

    # Get security_id to column mapping
    sec_to_col = get_security_id_to_column_map(postgres_conn_id, database)

    # Get prices for the lookback period
    prices_by_security = get_prices_for_lookback(
        target_date, lookback, trading_days, postgres_conn_id, database
    )

    # Calculate slope for each security
    result: dict[str, float | None] = {}
    for security_id, prices in prices_by_security.items():
        column_name = sec_to_col.get(security_id)
        if column_name:
            slope = calculate_slope(prices)
            result[column_name] = slope

    # Fill in None for securities without data
    for security_id, column_name in sec_to_col.items():
        if column_name not in result:
            result[column_name] = None

    return result


# =============================================================================
# UPSERT OPERATIONS
# =============================================================================

def upsert_slope_row(
    lookback: int,
    target_date: date,
    values: dict[str, float | None],
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> bool:
    """
    Insert or update a row in a slope table.

    Args:
        lookback: int (determines table name)
        target_date: date for the row
        values: dict of {column_name: slope_value}
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        bool: True if successful
    """
    table_name = TABLE_NAMES[lookback]
    column_names = get_column_names(postgres_conn_id, database)

    # Build column list and values
    columns_list = ["date"] + column_names
    placeholders = ", ".join(["%s"] * len(columns_list))
    update_sets = ", ".join([f"{col} = EXCLUDED.{col}" for col in column_names])

    sql = f"""
        INSERT INTO market_data.{table_name} ({", ".join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (date) DO UPDATE SET
            {update_sets}
    """

    # Build values tuple
    row_values = [target_date] + [values.get(col) for col in column_names]

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, row_values)
        conn.commit()

    return True


# =============================================================================
# BACKFILL OPERATIONS
# =============================================================================

def backfill_slope_table(
    lookback: int,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
    batch_size: int = 50,
) -> dict[str, Any]:
    """
    Backfill historical data for a single slope table.

    Args:
        lookback: int (10, 20, 30, etc.)
        postgres_conn_id: Airflow connection ID
        database: Database name
        batch_size: number of rows to commit at once

    Returns:
        dict with stats: {rows_inserted, start_date, end_date}
    """
    table_name = TABLE_NAMES[lookback]
    print(f"[backfill_slope] Starting backfill for {table_name}...")

    # Get all trading days
    trading_days = get_trading_days_from_adjusted(postgres_conn_id, database)
    if len(trading_days) < lookback:
        print(f"[backfill_slope] Not enough trading days for lookback={lookback}")
        return {"rows_inserted": 0, "error": "insufficient data"}

    # Start date: first date with enough history
    start_idx = lookback - 1
    valid_dates = trading_days[start_idx:]

    print(f"[backfill_slope] {table_name}: {len(valid_dates)} dates to process "
          f"({valid_dates[0]} to {valid_dates[-1]})")

    # Get column names and security mapping (once)
    column_names = get_column_names(postgres_conn_id, database)
    sec_to_col = get_security_id_to_column_map(postgres_conn_id, database)

    # Pre-fetch all adjusted prices data (more efficient)
    all_data = get_adj_close_for_date_range(
        trading_days[0], trading_days[-1], postgres_conn_id, database
    )

    # Calculate slopes for all dates
    rows_to_insert = []
    for target_date in valid_dates:
        target_idx = trading_days.index(target_date)
        start_idx = target_idx - lookback + 1
        lookback_dates = trading_days[start_idx:target_idx + 1]

        row_values = {"date": target_date}
        for security_id, col_name in sec_to_col.items():
            if security_id in all_data:
                prices = [all_data[security_id].get(d) for d in lookback_dates]
                # Filter out None values and calculate slope
                prices = [p for p in prices if p is not None]
                if len(prices) >= 2:
                    slope = calculate_slope(prices)
                    row_values[col_name] = slope
                else:
                    row_values[col_name] = None
            else:
                row_values[col_name] = None

        rows_to_insert.append(row_values)

        if len(rows_to_insert) % 50 == 0:
            print(f"[backfill_slope] {table_name}: Calculated {len(rows_to_insert)}/{len(valid_dates)} rows")

    # Bulk insert
    print(f"[backfill_slope] {table_name}: Inserting {len(rows_to_insert)} rows...")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    columns_list = ["date"] + column_names
    placeholders = ", ".join(["%s"] * len(columns_list))
    update_sets = ", ".join([f"{col} = EXCLUDED.{col}" for col in column_names])

    sql = f"""
        INSERT INTO market_data.{table_name} ({", ".join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (date) DO UPDATE SET
            {update_sets}
    """

    rows_inserted = 0
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for i, row in enumerate(rows_to_insert):
                values = [row["date"]] + [row.get(col) for col in column_names]
                cur.execute(sql, values)
                rows_inserted += 1

                # Commit in batches
                if (i + 1) % batch_size == 0:
                    conn.commit()
                    print(f"[backfill_slope] {table_name}: Committed {i + 1}/{len(rows_to_insert)} rows")

            conn.commit()

    print(f"[backfill_slope] {table_name}: Backfill complete, {rows_inserted} rows inserted")

    return {
        "table": table_name,
        "rows_inserted": rows_inserted,
        "start_date": str(valid_dates[0]) if valid_dates else None,
        "end_date": str(valid_dates[-1]) if valid_dates else None,
    }


def backfill_all_slope_tables(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Backfill all 12 slope tables.

    Returns:
        dict: {table_name: result, ...}
    """
    results = {}
    total_rows = 0

    for lookback in LOOKBACK_PERIODS:
        try:
            result = backfill_slope_table(lookback, postgres_conn_id, database)
            results[TABLE_NAMES[lookback]] = result
            total_rows += result.get("rows_inserted", 0)
        except Exception as e:
            print(f"[backfill_all_slope] Error with {TABLE_NAMES[lookback]}: {e}")
            results[TABLE_NAMES[lookback]] = {"status": "error", "error": str(e)}

    print(f"[backfill_all_slope] Total rows inserted across all tables: {total_rows}")
    return {"tables": results, "total_rows": total_rows}


# =============================================================================
# GAP DETECTION AND FILLING
# =============================================================================

def get_existing_dates_slope(
    lookback: int,
    days_back: int = 30,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> list[date]:
    """
    Get dates that exist in a slope table for the last N days.

    Returns:
        list of date objects
    """
    table_name = TABLE_NAMES[lookback]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT DISTINCT date
                FROM market_data.{table_name}
                WHERE date >= %s
                ORDER BY date
            """, (cutoff,))
            dates = [row[0] for row in cur.fetchall()]

    return dates


def find_missing_dates_slope(
    lookback: int,
    days_back: int = 30,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> list[date]:
    """
    Find missing dates in a slope table (last N days).

    Args:
        lookback: int (10, 20, 30, etc.)
        days_back: number of days to look back
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        list: missing date objects
    """
    table_name = TABLE_NAMES[lookback]

    # Get trading days from adjusted_prices (our source of truth)
    trading_days = get_trading_days_from_adjusted(postgres_conn_id, database)

    # Filter to last N days
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()
    recent_trading_days = [d for d in trading_days if d >= cutoff]

    # Also need to ensure enough history exists for the lookback
    if len(trading_days) >= lookback:
        first_valid_date = trading_days[lookback - 1]
        recent_trading_days = [d for d in recent_trading_days if d >= first_valid_date]

    # Get existing dates in the slope table
    existing_dates = get_existing_dates_slope(lookback, days_back, postgres_conn_id, database)
    existing_set = set(existing_dates)

    # Find missing
    missing = [d for d in recent_trading_days if d not in existing_set]

    print(f"[find_missing_slope] {table_name}: Recent trading days={len(recent_trading_days)}, "
          f"Existing={len(existing_dates)}, Missing={len(missing)}")

    return missing


def backfill_missing_dates_slope(
    lookback: int,
    missing_dates: list[date],
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Backfill only the missing dates for a slope table.

    Args:
        lookback: int (10, 20, 30, etc.)
        missing_dates: list of date objects to backfill
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        dict with stats
    """
    table_name = TABLE_NAMES[lookback]

    if not missing_dates:
        print(f"[backfill_missing_slope] {table_name}: No missing dates")
        return {"rows_inserted": 0}

    print(f"[backfill_missing_slope] {table_name}: Backfilling {len(missing_dates)} dates")

    # Get trading days
    trading_days = get_trading_days_from_adjusted(postgres_conn_id, database)

    rows_inserted = 0
    for target_date in sorted(missing_dates):
        try:
            values = calculate_slope_row(target_date, lookback, trading_days, postgres_conn_id, database)
            upsert_slope_row(lookback, target_date, values, postgres_conn_id, database)
            rows_inserted += 1
        except Exception as e:
            print(f"[backfill_missing_slope] Error for {target_date}: {e}")

    print(f"[backfill_missing_slope] {table_name}: Inserted {rows_inserted} rows")

    return {
        "table": table_name,
        "rows_inserted": rows_inserted,
        "dates": [str(d) for d in missing_dates],
    }


def fill_slope_gaps(
    lookback: int,
    days_back: int = 30,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Find and fill gaps for a single slope table.

    Returns:
        dict with results
    """
    missing = find_missing_dates_slope(lookback, days_back, postgres_conn_id, database)
    result = backfill_missing_dates_slope(lookback, missing, postgres_conn_id, database)

    return {
        "table": TABLE_NAMES[lookback],
        "missing_count": len(missing),
        "filled_count": result.get("rows_inserted", 0),
    }


def fill_all_slope_gaps(
    days_back: int = 30,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Find and fill gaps in all 12 slope tables.

    Returns:
        dict with results for each table
    """
    results = {}
    total_missing = 0
    total_filled = 0

    for lookback in LOOKBACK_PERIODS:
        try:
            result = fill_slope_gaps(lookback, days_back, postgres_conn_id, database)
            results[TABLE_NAMES[lookback]] = result
            total_missing += result.get("missing_count", 0)
            total_filled += result.get("filled_count", 0)
        except Exception as e:
            print(f"[fill_all_slope_gaps] Error with {TABLE_NAMES[lookback]}: {e}")
            results[TABLE_NAMES[lookback]] = {"status": "error", "error": str(e)}

    print(f"[fill_all_slope_gaps] Total: {total_missing} missing, {total_filled} filled")

    return {
        "tables": results,
        "total_missing": total_missing,
        "total_filled": total_filled,
    }


# =============================================================================
# DAILY UPDATE
# =============================================================================

def update_slope_tables_today(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Calculate and insert today's slope for all 12 tables.

    Returns:
        dict: {table_name: success/failure, ...}
    """
    # Get the most recent trading day from adjusted_prices
    trading_days = get_trading_days_from_adjusted(postgres_conn_id, database)
    if not trading_days:
        print("[update_slope_today] No trading days found")
        return {"error": "no trading days"}

    today = trading_days[-1]
    print(f"[update_slope_today] Updating slopes for {today}")

    results = {}

    for lookback in LOOKBACK_PERIODS:
        table_name = TABLE_NAMES[lookback]

        # Check if we have enough history
        if len(trading_days) < lookback:
            print(f"[update_slope_today] {table_name}: Not enough history")
            results[table_name] = {"status": "skipped", "reason": "insufficient history"}
            continue

        try:
            values = calculate_slope_row(today, lookback, trading_days, postgres_conn_id, database)
            upsert_slope_row(lookback, today, values, postgres_conn_id, database)
            results[table_name] = {"status": "success", "date": str(today)}
        except Exception as e:
            print(f"[update_slope_today] {table_name}: Error - {e}")
            results[table_name] = {"status": "error", "error": str(e)}

    success_count = sum(1 for r in results.values() if r.get("status") == "success")
    print(f"[update_slope_today] Updated {success_count}/{len(LOOKBACK_PERIODS)} tables")

    return {"date": str(today), "tables": results, "success_count": success_count}


# =============================================================================
# STATISTICS AND VERIFICATION
# =============================================================================

def get_slope_table_stats(
    lookback: int,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Get statistics for a single slope table.

    Returns:
        dict with row count, date range, sample values
    """
    table_name = TABLE_NAMES[lookback]

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Row count
            cur.execute(f"SELECT COUNT(*) FROM market_data.{table_name}")
            row_count = cur.fetchone()[0]

            # Date range
            cur.execute(f"""
                SELECT MIN(date), MAX(date)
                FROM market_data.{table_name}
            """)
            date_min, date_max = cur.fetchone()

            # Sample: latest row with some ticker values
            cur.execute(f"""
                SELECT date, aapl, msft, googl, sp500
                FROM market_data.{table_name}
                ORDER BY date DESC
                LIMIT 1
            """)
            latest = cur.fetchone()

    return {
        "table": table_name,
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


def verify_all_slope_tables(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Verify all slope tables exist and have data.

    Returns:
        dict with stats for each table
    """
    results = {}

    for lookback in LOOKBACK_PERIODS:
        try:
            stats = get_slope_table_stats(lookback, postgres_conn_id, database)
            results[TABLE_NAMES[lookback]] = stats
        except Exception as e:
            results[TABLE_NAMES[lookback]] = {"error": str(e)}

    total_rows = sum(r.get("row_count", 0) for r in results.values() if "row_count" in r)
    print(f"[verify_all_slope] Total rows across all tables: {total_rows}")

    return {"tables": results, "total_rows": total_rows}
