"""
Market Data Adjusted Close Module
==================================

This module provides functionality for normalised adjusted-close prices
stored in a relational (long) schema with three tables:

    market_data.securities          – master list of tracked securities
    market_data.adjusted_prices     – daily adjusted close prices
    market_data.corporate_actions   – detected corporate-action events

The module mirrors the structural conventions of market_data_wide.py
(same CSV source, same normalize_column_name helper) but targets a
different set of tables and a fundamentally different storage layout.
"""

from __future__ import annotations

import csv
import time
from datetime import datetime, timezone
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
        'AAPL'  -> 'aapl'
    """
    special_cases = {
        "^GSPC": "sp500",
        "^NDX": "nasdaq100",
        "^DJI": "dow",
    }

    if alias in special_cases:
        return special_cases[alias]

    col = alias.lower()
    col = col.replace("-", "_").replace("^", "").replace(" ", "_")

    if col and col[0].isdigit():
        col = "t_" + col

    return col


def load_tickers_from_csv() -> list[dict]:
    """
    Load tickers from CSV file and return list of dicts with:
    - yf_ticker: Yahoo Finance ticker (e.g., '^GSPC', 'BRK-B')
    - alias: Original alias from CSV
    - column: Normalized PostgreSQL column name
    """
    csv_path = Path("data/info/tickers_list.csv")
    tickers = []
    seen_columns: set[str] = set()

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yf_ticker = row["yf_ticker"].strip()
            alias = row["Ticker"].strip()
            column = normalize_column_name(alias)

            if column in seen_columns:
                continue

            tickers.append({
                "yf_ticker": yf_ticker,
                "alias": alias,
                "column": column,
            })
            seen_columns.add(column)

    return tickers


# Load tickers at module import time
TICKERS = load_tickers_from_csv()
TICKER_COLUMNS = [t["column"] for t in TICKERS]
YF_TO_COLUMN = {t["yf_ticker"]: t["column"] for t in TICKERS}


# ---------------------------------------------------------------------
# 2) DDL Generation
# ---------------------------------------------------------------------

def generate_securities_ddl() -> str:
    """Generate CREATE TABLE DDL for market_data.securities."""
    return """
CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.securities (
    security_id   SERIAL PRIMARY KEY,
    yf_ticker     VARCHAR(20) NOT NULL UNIQUE,
    column_name   VARCHAR(50) NOT NULL UNIQUE,
    company_name  TEXT DEFAULT NULL,
    exchange      VARCHAR(20) DEFAULT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    listed_date   DATE DEFAULT NULL,
    delisted_date DATE DEFAULT NULL,
    notes         TEXT DEFAULT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def generate_adjusted_prices_ddl() -> str:
    """Generate CREATE TABLE DDL for market_data.adjusted_prices."""
    return """
CREATE TABLE IF NOT EXISTS market_data.adjusted_prices (
    security_id  INTEGER NOT NULL REFERENCES market_data.securities(security_id),
    date         DATE    NOT NULL,
    raw_close    NUMERIC(18,6),
    adj_factor   NUMERIC(18,10),
    adj_close    NUMERIC(18,6),
    volume       BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (security_id, date)
);

CREATE INDEX IF NOT EXISTS idx_adjusted_prices_date_desc
    ON market_data.adjusted_prices (date DESC);

CREATE INDEX IF NOT EXISTS idx_adjusted_prices_security
    ON market_data.adjusted_prices (security_id);

COMMENT ON TABLE market_data.adjusted_prices IS
'Daily adjusted close prices with adjustment factors per security';
"""


def generate_corporate_actions_ddl() -> str:
    """Generate CREATE TABLE DDL for market_data.corporate_actions."""
    return """
CREATE TABLE IF NOT EXISTS market_data.corporate_actions (
    action_id    SERIAL PRIMARY KEY,
    security_id  INTEGER NOT NULL REFERENCES market_data.securities(security_id),
    action_date  DATE NOT NULL,
    action_type  VARCHAR(50) NOT NULL,
    factor       NUMERIC(18,10),
    description  TEXT,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_corporate_actions_security_date
    ON market_data.corporate_actions (security_id, action_date DESC);

COMMENT ON TABLE market_data.corporate_actions IS
'Detected corporate actions (splits, dividends) based on adjustment factor changes';
"""


def generate_all_adjusted_ddl() -> str:
    """Return combined DDL for all three adjusted-close tables."""
    return (
        generate_securities_ddl()
        + generate_adjusted_prices_ddl()
        + generate_corporate_actions_ddl()
    )


# ---------------------------------------------------------------------
# 3) Securities population
# ---------------------------------------------------------------------

def populate_securities(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Populate market_data.securities from the CSV ticker list.

    For each ticker this function:
      1. Attempts to fetch company info from yfinance (.info)
      2. Falls back to the CSV alias if the info call fails
      3. Upserts into market_data.securities

    Returns dict with counts of inserted / updated / failed tickers.
    """
    import yfinance as yf

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    inserted = 0
    updated = 0
    failed = 0

    print(f"[populate_securities] Processing {len(TICKERS)} tickers")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for i, t in enumerate(TICKERS):
                yf_ticker = t["yf_ticker"]
                column = t["column"]
                alias = t["alias"]

                company_name = None
                exchange = None

                try:
                    info = yf.Ticker(yf_ticker).info
                    company_name = info.get("longName") or info.get("shortName")
                    exchange = info.get("exchange")
                except Exception as e:
                    print(f"[populate_securities] yfinance info failed for {yf_ticker}: {e}")
                    failed += 1

                cur.execute(
                    """
                    INSERT INTO market_data.securities
                        (yf_ticker, column_name, company_name, exchange)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (yf_ticker) DO UPDATE SET
                        column_name  = EXCLUDED.column_name,
                        company_name = COALESCE(EXCLUDED.company_name, market_data.securities.company_name),
                        exchange     = COALESCE(EXCLUDED.exchange, market_data.securities.exchange),
                        updated_at   = NOW()
                    RETURNING (xmax = 0) AS is_insert
                    """,
                    (yf_ticker, column, company_name, exchange),
                )
                is_insert = cur.fetchone()[0]
                if is_insert:
                    inserted += 1
                else:
                    updated += 1

                if (i + 1) % 25 == 0:
                    print(f"[populate_securities] Progress: {i + 1}/{len(TICKERS)}")

                # Rate-limit yfinance info calls
                time.sleep(0.5)

        conn.commit()

    print(f"[populate_securities] Done: inserted={inserted}, updated={updated}, failed_info={failed}")
    return {"inserted": inserted, "updated": updated, "failed_info": failed}


# ---------------------------------------------------------------------
# 4) Security ID map
# ---------------------------------------------------------------------

def get_security_id_map(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, int]:
    """Return {yf_ticker: security_id} mapping from the securities table."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT yf_ticker, security_id FROM market_data.securities")
            rows = cur.fetchall()

    return {row[0]: row[1] for row in rows}


# ---------------------------------------------------------------------
# 5) Backfill adjusted prices (historical)
# ---------------------------------------------------------------------

def backfill_adjusted_prices(
    period: str = "1y",
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Backfill market_data.adjusted_prices from Yahoo Finance history.

    Downloads Close, Adj Close, and Volume for *period*, computes
    adj_factor = Adj Close / Close, and bulk-inserts with
    ON CONFLICT DO NOTHING (never overwrites existing rows).
    """
    import yfinance as yf
    from psycopg2.extras import execute_values

    sec_map = get_security_id_map(postgres_conn_id, database)
    if not sec_map:
        print("[backfill_adjusted] No securities found – run populate_securities first")
        return {"rows_inserted": 0, "error": "no securities"}

    yf_tickers = [t["yf_ticker"] for t in TICKERS]
    print(f"[backfill_adjusted] Downloading {len(yf_tickers)} tickers, period={period}")

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
        print(f"[backfill_adjusted] Download failed: {e}")
        return {"rows_inserted": 0, "error": str(e)}

    if data.empty:
        print("[backfill_adjusted] No data returned")
        return {"rows_inserted": 0, "error": "empty download"}

    print(f"[backfill_adjusted] Downloaded {len(data)} trading days")

    single = len(yf_tickers) == 1
    close_data = data["Close"]
    adj_close_data = data["Adj Close"]
    volume_data = data["Volume"]

    rows_to_insert: list[tuple] = []

    for idx in close_data.index:
        row_date = idx.date()
        for t in TICKERS:
            yf_t = t["yf_ticker"]
            sid = sec_map.get(yf_t)
            if sid is None:
                continue

            try:
                raw = float(close_data.loc[idx] if single else close_data.loc[idx, yf_t])
                adj = float(adj_close_data.loc[idx] if single else adj_close_data.loc[idx, yf_t])
                vol_val = volume_data.loc[idx] if single else volume_data.loc[idx, yf_t]
                vol = int(vol_val) if pd.notna(vol_val) else None
            except Exception:
                continue

            if pd.isna(raw) or pd.isna(adj) or raw == 0:
                continue

            factor = adj / raw
            rows_to_insert.append((sid, row_date, round(raw, 6), round(factor, 10), round(adj, 6), vol))

    print(f"[backfill_adjusted] Prepared {len(rows_to_insert)} rows")

    if not rows_to_insert:
        return {"rows_inserted": 0}

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO market_data.adjusted_prices
                    (security_id, date, raw_close, adj_factor, adj_close, volume)
                VALUES %s
                ON CONFLICT (security_id, date) DO NOTHING
            """
            execute_values(cur, sql, rows_to_insert, page_size=2000)
            rows_affected = cur.rowcount
        conn.commit()

    print(f"[backfill_adjusted] Inserted {rows_affected} rows")
    return {"rows_inserted": rows_affected, "rows_attempted": len(rows_to_insert)}


# ---------------------------------------------------------------------
# 6) Daily ingest (today's data)
# ---------------------------------------------------------------------

def fetch_yahoo_data_today(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Fetch the latest adjusted-close data and upsert into adjusted_prices.

    Downloads a 5-day window to guarantee coverage across weekends, then
    extracts only the most recent trading day.  ON CONFLICT updates
    adj_factor, adj_close, and volume but leaves raw_close immutable.
    """
    import yfinance as yf

    sec_map = get_security_id_map(postgres_conn_id, database)
    if not sec_map:
        print("[fetch_today] No securities found")
        return {"upserted": 0, "error": "no securities"}

    yf_tickers = [t["yf_ticker"] for t in TICKERS]
    print(f"[fetch_today] Downloading 5d data for {len(yf_tickers)} tickers")

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
        print(f"[fetch_today] Download failed: {e}")
        return {"upserted": 0, "error": str(e)}

    if data.empty:
        print("[fetch_today] No data returned")
        return {"upserted": 0, "error": "empty download"}

    single = len(yf_tickers) == 1
    close_data = data["Close"]
    adj_close_data = data["Adj Close"]
    volume_data = data["Volume"]

    # Take the latest available date
    latest_idx = close_data.index[-1]
    latest_date = latest_idx.date()
    print(f"[fetch_today] Latest date in download: {latest_date}")

    upserted = 0
    failed = 0

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for t in TICKERS:
                yf_t = t["yf_ticker"]
                sid = sec_map.get(yf_t)
                if sid is None:
                    continue

                try:
                    raw = float(close_data.loc[latest_idx] if single else close_data.loc[latest_idx, yf_t])
                    adj = float(adj_close_data.loc[latest_idx] if single else adj_close_data.loc[latest_idx, yf_t])
                    vol_val = volume_data.loc[latest_idx] if single else volume_data.loc[latest_idx, yf_t]
                    vol = int(vol_val) if pd.notna(vol_val) else None
                except Exception:
                    failed += 1
                    continue

                if pd.isna(raw) or pd.isna(adj) or raw == 0:
                    failed += 1
                    continue

                factor = adj / raw

                cur.execute(
                    """
                    INSERT INTO market_data.adjusted_prices
                        (security_id, date, raw_close, adj_factor, adj_close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (security_id, date) DO UPDATE SET
                        adj_factor = EXCLUDED.adj_factor,
                        adj_close  = EXCLUDED.adj_close,
                        volume     = EXCLUDED.volume,
                        updated_at = NOW()
                    """,
                    (sid, latest_date, round(raw, 6), round(factor, 10), round(adj, 6), vol),
                )
                upserted += 1

        conn.commit()

    print(f"[fetch_today] Upserted {upserted}, failed {failed}")
    return {"upserted": upserted, "failed": failed, "date": str(latest_date)}


# ---------------------------------------------------------------------
# 7) Historical adj-factor refresh
# ---------------------------------------------------------------------

def fetch_historical_adj_close(
    period: str = "1y",
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Re-download full history and recompute adj_factor for all rows.

    Uses UPDATE ... FROM (VALUES %s) to bulk-update existing rows.
    """
    import yfinance as yf
    from psycopg2.extras import execute_values

    sec_map = get_security_id_map(postgres_conn_id, database)
    if not sec_map:
        print("[refresh_adj] No securities found")
        return {"rows_updated": 0, "error": "no securities"}

    yf_tickers = [t["yf_ticker"] for t in TICKERS]
    print(f"[refresh_adj] Downloading {len(yf_tickers)} tickers, period={period}")

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
        print(f"[refresh_adj] Download failed: {e}")
        return {"rows_updated": 0, "error": str(e)}

    if data.empty:
        print("[refresh_adj] No data returned")
        return {"rows_updated": 0, "error": "empty download"}

    single = len(yf_tickers) == 1
    close_data = data["Close"]
    adj_close_data = data["Adj Close"]
    volume_data = data["Volume"]

    update_rows: list[tuple] = []

    for idx in close_data.index:
        row_date = idx.date()
        for t in TICKERS:
            yf_t = t["yf_ticker"]
            sid = sec_map.get(yf_t)
            if sid is None:
                continue

            try:
                raw = float(close_data.loc[idx] if single else close_data.loc[idx, yf_t])
                adj = float(adj_close_data.loc[idx] if single else adj_close_data.loc[idx, yf_t])
                vol_val = volume_data.loc[idx] if single else volume_data.loc[idx, yf_t]
                vol = int(vol_val) if pd.notna(vol_val) else None
            except Exception:
                continue

            if pd.isna(raw) or pd.isna(adj) or raw == 0:
                continue

            factor = adj / raw
            update_rows.append((sid, row_date, round(raw, 6), round(factor, 10), round(adj, 6), vol))

    print(f"[refresh_adj] Prepared {len(update_rows)} rows for upsert")

    if not update_rows:
        return {"rows_updated": 0}

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO market_data.adjusted_prices
                    (security_id, date, raw_close, adj_factor, adj_close, volume)
                VALUES %s
                ON CONFLICT (security_id, date) DO UPDATE SET
                    adj_factor = EXCLUDED.adj_factor,
                    adj_close  = EXCLUDED.adj_close,
                    volume     = EXCLUDED.volume,
                    updated_at = NOW()
            """
            execute_values(cur, sql, update_rows, page_size=2000)
            rows_affected = cur.rowcount
        conn.commit()

    print(f"[refresh_adj] Upserted {rows_affected} rows")
    return {"rows_updated": rows_affected, "rows_attempted": len(update_rows)}


# ---------------------------------------------------------------------
# 8) Corporate action detection
# ---------------------------------------------------------------------

def detect_and_log_corporate_actions(
    threshold: float = 0.01,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Compare consecutive adj_factors per security and log significant
    changes (> threshold) into market_data.corporate_actions.

    A change in the adjustment factor between two consecutive trading
    days indicates a split, reverse-split, or dividend distribution.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    actions_logged = 0

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Find consecutive-day factor changes per security
            cur.execute("""
                WITH ordered AS (
                    SELECT
                        security_id,
                        date,
                        adj_factor,
                        LAG(adj_factor) OVER (
                            PARTITION BY security_id ORDER BY date
                        ) AS prev_factor,
                        LAG(date) OVER (
                            PARTITION BY security_id ORDER BY date
                        ) AS prev_date
                    FROM market_data.adjusted_prices
                    WHERE adj_factor IS NOT NULL
                )
                SELECT security_id, date, adj_factor, prev_factor, prev_date
                FROM ordered
                WHERE prev_factor IS NOT NULL
                  AND ABS(adj_factor - prev_factor) / prev_factor > %s
            """, (threshold,))

            changes = cur.fetchall()
            print(f"[detect_actions] Found {len(changes)} factor changes > {threshold*100:.1f}%")

            for security_id, action_date, factor, prev_factor, prev_date in changes:
                ratio = factor / prev_factor
                if ratio > 1:
                    action_type = "dividend_adjustment"
                    description = f"Factor increased from {prev_factor:.10f} to {factor:.10f} (ratio {ratio:.6f})"
                else:
                    action_type = "split_adjustment"
                    description = f"Factor decreased from {prev_factor:.10f} to {factor:.10f} (ratio {ratio:.6f})"

                cur.execute(
                    """
                    INSERT INTO market_data.corporate_actions
                        (security_id, action_date, action_type, factor, description)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (security_id, action_date, action_type, round(ratio, 10), description),
                )
                actions_logged += 1

        conn.commit()

    print(f"[detect_actions] Logged {actions_logged} corporate actions")
    return {"actions_logged": actions_logged}


# ---------------------------------------------------------------------
# 9) Market hours check
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
# 10) Stats / verification utilities
# ---------------------------------------------------------------------

def get_adjusted_prices_stats(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """Return statistics about the adjusted_prices table."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM market_data.adjusted_prices")
            row_count = cur.fetchone()[0]

            cur.execute("""
                SELECT MIN(date), MAX(date)
                FROM market_data.adjusted_prices
            """)
            date_min, date_max = cur.fetchone()

            cur.execute("""
                SELECT s.yf_ticker, ap.date, ap.raw_close, ap.adj_close, ap.adj_factor
                FROM market_data.adjusted_prices ap
                JOIN market_data.securities s ON s.security_id = ap.security_id
                ORDER BY ap.date DESC, s.yf_ticker
                LIMIT 5
            """)
            latest_rows = cur.fetchall()

    return {
        "row_count": row_count,
        "date_range": f"{date_min} to {date_max}" if date_min else None,
        "latest_rows": [
            {
                "ticker": r[0],
                "date": str(r[1]),
                "raw_close": float(r[2]) if r[2] else None,
                "adj_close": float(r[3]) if r[3] else None,
                "adj_factor": float(r[4]) if r[4] else None,
            }
            for r in latest_rows
        ],
    }


def verify_table_counts(
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, int]:
    """Return row counts for all three adjusted-close tables."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    counts: dict[str, int] = {}
    tables = ["securities", "adjusted_prices", "corporate_actions"]

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM market_data.{table}")
                counts[table] = cur.fetchone()[0]

    print(f"[verify] Table counts: {counts}")
    return counts


# =============================================================================
# 11) GAP DETECTION AND BACKFILL FUNCTIONS
# =============================================================================

def get_trading_days_from_yahoo_adj(days_back: int = 30) -> list:
    """
    Download SPY data for last N days to get valid trading days.
    SPY is used as a proxy for market open days.

    Args:
        days_back: Number of trading days to look back

    Returns:
        List of date objects representing valid trading days
    """
    import yfinance as yf
    from datetime import timedelta

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back + 10)  # Extra buffer for weekends/holidays

    try:
        data = yf.download("SPY", start=start, end=end, progress=False)
    except Exception as e:
        print(f"[get_trading_days_adj] Failed to download SPY data: {e}")
        return []

    if data.empty:
        print("[get_trading_days_adj] No SPY data returned")
        return []

    trading_days = [d.date() for d in data.index]
    # Return only the last N days
    result = sorted(trading_days)[-days_back:] if len(trading_days) > days_back else sorted(trading_days)
    print(f"[get_trading_days_adj] Found {len(result)} trading days in last {days_back} days")
    return result


def get_existing_dates_adjusted(
    days_back: int = 30,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> list:
    """
    Get dates that exist in adjusted_prices for the last N days.

    Args:
        days_back: Number of days to look back
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        List of date objects that exist in the table
    """
    from datetime import timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()

    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT date FROM market_data.adjusted_prices WHERE date >= %s ORDER BY date",
                (cutoff,)
            )
            dates = [row[0] for row in cur.fetchall()]

    print(f"[get_existing_dates_adj] Found {len(dates)} existing dates in adjusted_prices")
    return dates


def find_missing_dates_adjusted(
    days_back: int = 30,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> list:
    """
    Compare trading days vs existing dates in adjusted_prices.

    Args:
        days_back: Number of days to look back
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        List of missing date objects
    """
    trading_days = get_trading_days_from_yahoo_adj(days_back)
    existing_dates = get_existing_dates_adjusted(days_back, postgres_conn_id, database)

    existing_set = set(existing_dates)
    missing = [d for d in trading_days if d not in existing_set]

    print(f"[find_missing_adj] adjusted_prices: Trading days={len(trading_days)}, "
          f"Existing={len(existing_dates)}, Missing={len(missing)}")

    if missing:
        print(f"[find_missing_adj] Missing dates: {missing}")

    return missing


def backfill_missing_dates_adjusted(
    missing_dates: list,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Download and insert adjusted prices for only the missing dates.

    Args:
        missing_dates: List of date objects to backfill
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        Dict with stats: {rows_inserted, dates_processed}
    """
    import yfinance as yf
    from datetime import timedelta
    from psycopg2.extras import execute_values

    if not missing_dates:
        print("[backfill_missing_adj] No missing dates to backfill")
        return {"rows_inserted": 0, "dates_processed": 0}

    print(f"[backfill_missing_adj] Backfilling {len(missing_dates)} dates: {missing_dates}")

    # Get security ID mapping
    sec_map = get_security_id_map(postgres_conn_id, database)
    if not sec_map:
        print("[backfill_missing_adj] No securities found – run populate_securities first")
        return {"rows_inserted": 0, "error": "no securities"}

    # Download data for the date range
    start_date = min(missing_dates) - timedelta(days=1)
    end_date = max(missing_dates) + timedelta(days=2)

    yf_tickers = [t["yf_ticker"] for t in TICKERS]

    try:
        data = yf.download(
            tickers=yf_tickers,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as e:
        print(f"[backfill_missing_adj] Download failed: {e}")
        return {"rows_inserted": 0, "error": str(e)}

    if data.empty:
        print("[backfill_missing_adj] No data returned from Yahoo")
        return {"rows_inserted": 0, "error": "empty download"}

    single = len(yf_tickers) == 1
    close_data = data["Close"]
    adj_close_data = data["Adj Close"]
    volume_data = data["Volume"]

    rows_to_insert: list[tuple] = []

    for missing_date in missing_dates:
        # Find the index matching this date
        matching_idx = [idx for idx in close_data.index if idx.date() == missing_date]
        if not matching_idx:
            print(f"[backfill_missing_adj] No data for {missing_date}")
            continue

        row_idx = matching_idx[0]

        for t in TICKERS:
            yf_ticker = t["yf_ticker"]
            sid = sec_map.get(yf_ticker)
            if sid is None:
                continue

            try:
                raw = float(close_data.loc[row_idx] if single else close_data.loc[row_idx, yf_ticker])
                adj = float(adj_close_data.loc[row_idx] if single else adj_close_data.loc[row_idx, yf_ticker])
                vol_val = volume_data.loc[row_idx] if single else volume_data.loc[row_idx, yf_ticker]
                vol = int(vol_val) if pd.notna(vol_val) else None
            except Exception:
                continue

            if pd.isna(raw) or pd.isna(adj) or raw == 0:
                continue

            factor = adj / raw
            rows_to_insert.append((sid, missing_date, round(raw, 6), round(factor, 10), round(adj, 6), vol))

    if not rows_to_insert:
        print("[backfill_missing_adj] No rows to insert")
        return {"rows_inserted": 0, "dates_processed": 0}

    # Bulk insert
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO market_data.adjusted_prices
                    (security_id, date, raw_close, adj_factor, adj_close, volume)
                VALUES %s
                ON CONFLICT (security_id, date) DO NOTHING
            """
            execute_values(cur, sql, rows_to_insert, page_size=2000)
            rows_affected = cur.rowcount
        conn.commit()

    print(f"[backfill_missing_adj] Inserted {rows_affected} rows for {len(missing_dates)} dates")

    return {
        "rows_inserted": rows_affected,
        "dates_processed": len(missing_dates),
        "dates": [str(d) for d in missing_dates],
    }


def fill_adjusted_gaps(
    days_back: int = 30,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
) -> dict[str, Any]:
    """
    Find and fill gaps in adjusted_prices table.

    Args:
        days_back: Days to look back for gaps
        postgres_conn_id: Airflow connection ID
        database: Database name

    Returns:
        Dict with results: {missing_count, filled_count, dates}
    """
    missing = find_missing_dates_adjusted(days_back, postgres_conn_id, database)
    result = backfill_missing_dates_adjusted(missing, postgres_conn_id, database)

    return {
        "table": "adjusted_prices",
        "missing_count": len(missing),
        "filled_count": result.get("rows_inserted", 0),
        "dates": [str(d) for d in missing],
    }
