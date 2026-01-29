"""
Intraday Market Data Update DAG
================================

This DAG fetches 5-minute intraday data for all tickers during NYSE market hours.
It runs every 30 minutes but uses a ShortCircuitOperator to skip execution
outside of market hours.

NYSE Market Hours (Eastern Time):
- Regular session: 9:30 AM - 4:00 PM ET
- In UTC: 14:30 - 21:00 (during EST, Nov-Mar)
         13:30 - 20:00 (during EDT, Mar-Nov)

For simplicity, this DAG uses fixed UTC hours (14:30-21:00).
A future enhancement could use pytz to handle DST dynamically.

Schedule: */30 * * * * (every 30 minutes)
Data: period="1d", interval="5m" (5-minute bars for the current day)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

from plugins.market_data_shared import INDEXES, upsert_index_yahoo_minutely_into_table


# ---------------------------------------------------------------------
# Market Hours Configuration (UTC)
# ---------------------------------------------------------------------
# NYSE regular hours: 9:30 AM - 4:00 PM Eastern
# During EST (Nov-Mar): UTC-5 -> 14:30 - 21:00 UTC
# During EDT (Mar-Nov): UTC-4 -> 13:30 - 20:00 UTC
#
# We use the wider window (EST hours) to ensure we capture all trading.
# This means during EDT we may run a few extra times outside market hours,
# but the ShortCircuit will skip those runs harmlessly.

MARKET_OPEN_UTC_HOUR = 14
MARKET_OPEN_UTC_MINUTE = 30
MARKET_CLOSE_UTC_HOUR = 21
MARKET_CLOSE_UTC_MINUTE = 0


def is_market_hours() -> bool:
    """
    Check if current UTC time is within NYSE market hours.

    Returns True if we should proceed with data fetching,
    False if we should skip (outside market hours).

    Note: This does not account for market holidays or weekends.
    A production system should integrate a market calendar (e.g., pandas_market_calendars).
    """
    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc)

    # Skip weekends (Saturday=5, Sunday=6)
    if now_utc.weekday() >= 5:
        print(f"[market_hours_check] Skipping: weekend (weekday={now_utc.weekday()})")
        return False

    # Convert current time to minutes since midnight for easier comparison
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
# Intraday data configuration
# ---------------------------------------------------------------------
PERIOD = "1d"       # Fetch today's data only
INTERVAL = "5m"     # 5-minute bars


default_args = {
    "owner": "heri",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="market_data_intraday_update",
    start_date=datetime(2025, 11, 3, 0, 0),
    schedule="*/30 * * * *",  # Every 30 minutes
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args=default_args,
    description=__doc__,
    tags=["market_data", "intraday"],
) as dag:

    # Task 1: Check if we're within market hours
    # ShortCircuitOperator skips all downstream tasks if this returns False
    check_market_hours = ShortCircuitOperator(
        task_id="check_market_hours",
        python_callable=is_market_hours,
    )

    # Task 2: Fetch and upsert intraday data for each ticker
    # Uses dynamic task mapping to create one task per ticker
    upsert_intraday = PythonOperator.partial(
        task_id="upsert_intraday_5m",
        python_callable=upsert_index_yahoo_minutely_into_table,
    ).expand(op_kwargs=[
        {
            "table": cfg["table"],
            "ticker": cfg["ticker"],
            "name": cfg["name"],
            "source": "yahoo",
            "period": PERIOD,
            "interval": INTERVAL,
            "postgres_conn_id": "postgres_default",
            "database": "airflow",
        }
        for cfg in INDEXES
    ])

    # Set task dependencies
    check_market_hours >> upsert_intraday
