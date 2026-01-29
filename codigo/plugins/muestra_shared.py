import os
import re
from datetime import datetime, timezone

def _parse_date_from_filename(file_path: str):
    """
    Extract YYMMDD from 'Muestra_251119.xlsx' -> date(2025-11-19)
    """
    base = os.path.basename(file_path)
    m = re.search(r"(\d{6})", base)
    if not m:
        raise ValueError(f"Cannot extract YYMMDD from filename: {base}")
    code = m.group(1)
    dt = datetime.strptime(code, "%y%m%d").date()
    return dt


def _nn(x, pd):
    """NaN -> None helper."""
    return None if pd.isna(x) else x


def ingest_one_muestra_file(conn, file_path: str) -> int:
    import pandas as pd

    snapshot_date = _parse_date_from_filename(file_path)

    df = pd.read_excel(file_path, sheet_name="muestra_mensual")

    # Normalize column names
    df = df.rename(
        columns={
            "ISIN": "isin",
            "Ticker": "ticker",
            "Nombre": "name",
            "GICS Sector ES": "gics_sector",
            "GICS Ind Name ES": "gics_ind",
            "GICS SubInd Name ES": "gics_subind",
            "Curncy": "currency",
            "Market Cap:D-1": "market_cap",
            "Shares Outstanding": "shares_outstanding",
            "BEst Target Px:D-1": "best_target",
            "Tot Buy:D-1": "total_buy",
            "Tot Sell:D-1": "total_sell",
            "Tot Hold:D-1": "total_hold",
            "Div Yield": "div_yield",
            "EV / EBITDA Adj": "ev_ebitda",
            "P/E:D-1": "pe",
            "P/B:D-1": "pb",
            "DJI": "dji",
            "SP500": "sp500",
            "NASDAQ100": "nasdaq100",
            "SIC": "sic",
            "Clasificacion_Capitalizacion": "cap_class",
        }
    )

    # Force numeric columns; strings like " " -> NaN
    numeric_cols = [
        "market_cap",
        "shares_outstanding",
        "best_target",
        "total_buy",
        "total_hold",
        "total_sell",
        "div_yield",
        "ev_ebitda",
        "pe",
        "pb",
        "sic",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # <<< END ADDED BLOCK >>>

    with conn.cursor() as cur:
        for row in df.itertuples(index=False):
            ticker = str(row.ticker).strip()
            if not ticker:
                continue

            # 1) identity
            cur.execute(
                """
                INSERT INTO muestra.identity
                    (ticker, date, isin, name, gics_sector, gics_ind, gics_subind, currency)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) DO UPDATE SET
                    isin        = EXCLUDED.isin,
                    name        = EXCLUDED.name,
                    gics_sector = EXCLUDED.gics_sector,
                    gics_ind    = EXCLUDED.gics_ind,
                    gics_subind = EXCLUDED.gics_subind,
                    currency    = EXCLUDED.currency;
                """,
                (
                    ticker,
                    snapshot_date,
                    _nn(row.isin, pd),
                    _nn(row.name, pd),
                    _nn(row.gics_sector, pd),
                    _nn(row.gics_ind, pd),
                    _nn(row.gics_subind, pd),
                    _nn(row.currency, pd),
                ),
            )

            # 2) index_membership
            in_dji = bool(row.dji) if not pd.isna(row.dji) else False
            in_sp500 = bool(row.sp500) if not pd.isna(row.sp500) else False
            in_nasdaq100 = bool(row.nasdaq100) if not pd.isna(row.nasdaq100) else False

            cur.execute(
                """
                INSERT INTO muestra.index_membership
                    (ticker, date, in_dji, in_sp500, in_nasdaq100,
                     sic, market_cap, cap_class)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) DO UPDATE SET
                    in_dji       = EXCLUDED.in_dji,
                    in_sp500     = EXCLUDED.in_sp500,
                    in_nasdaq100 = EXCLUDED.in_nasdaq100,
                    sic          = EXCLUDED.sic,
                    market_cap   = EXCLUDED.market_cap,
                    cap_class    = EXCLUDED.cap_class;
                """,
                (
                    ticker,
                    snapshot_date,
                    in_dji,
                    in_sp500,
                    in_nasdaq100,
                    _nn(row.sic, pd),
                    _nn(row.market_cap, pd),
                    _nn(row.cap_class, pd),
                ),
            )

            # 3) Per-ticker table: muestra.<ticker_normalized>
            safe_table = re.sub(r"[^a-zA-Z0-9_]", "_", ticker).lower()

            create_ticker_table_sql = f"""
            CREATE TABLE IF NOT EXISTS muestra.{safe_table} (
                date               DATE PRIMARY KEY,
                market_cap         NUMERIC,
                shares_outstanding NUMERIC,
                best_target        NUMERIC,
                total_buy          NUMERIC,
                total_hold         NUMERIC,
                total_sell         NUMERIC,
                div_yield          NUMERIC,
                ev_ebitda          NUMERIC,
                pe                 NUMERIC,
                pb                 NUMERIC
            );
            """
            cur.execute(create_ticker_table_sql)

            insert_ticker_sql = f"""
            INSERT INTO muestra.{safe_table}
                (date, market_cap, shares_outstanding, best_target,
                 total_buy, total_hold, total_sell,
                 div_yield, ev_ebitda, pe, pb)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                market_cap         = EXCLUDED.market_cap,
                shares_outstanding = EXCLUDED.shares_outstanding,
                best_target        = EXCLUDED.best_target,
                total_buy          = EXCLUDED.total_buy,
                total_hold         = EXCLUDED.total_hold,
                total_sell         = EXCLUDED.total_sell,
                div_yield          = EXCLUDED.div_yield,
                ev_ebitda          = EXCLUDED.ev_ebitda,
                pe                 = EXCLUDED.pe,
                pb                 = EXCLUDED.pb;
            """
            cur.execute(
                insert_ticker_sql,
                (
                    snapshot_date,
                    _nn(row.market_cap, pd),
                    _nn(row.shares_outstanding, pd),
                    _nn(row.best_target, pd),
                    _nn(row.total_buy, pd),
                    _nn(row.total_hold, pd),
                    _nn(row.total_sell, pd),
                    _nn(row.div_yield, pd),
                    _nn(row.ev_ebitda, pd),
                    _nn(row.pe, pd),
                    _nn(row.pb, pd),
                ),
            )

    return len(df)

from airflow.providers.postgres.hooks.postgres import PostgresHook

def scan_and_ingest_new_files(
    data_dir: str,
    postgres_conn_id: str = "postgres_default",
    database: str = "airflow",
    **context,
) -> None:
    """
    Look for Muestra_*.xlsx in data_dir.
    For each filename not in muestra.files_processed:
      - ingest it
      - mark as processed
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, database=database)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT filename FROM muestra.files_processed;")
            processed = {row[0] for row in cur.fetchall()}

        all_files = [
            f
            for f in os.listdir(data_dir)
            if re.match(r"Muestra_\d{6}\.xlsx$", f)
        ]
        all_files.sort()

        new_files = [f for f in all_files if f not in processed]

        if not new_files:
            print("No new Muestra files found.")
            return

        print(f"New files to ingest: {new_files}")

        for fname in new_files:
            full_path = os.path.join(data_dir, fname)
            rows = ingest_one_muestra_file(conn, full_path)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO muestra.files_processed (filename, processed_at, rows_ingested)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (filename) DO NOTHING;
                    """,
                    (
                        fname,
                        datetime.now(timezone.utc),
                        rows,
                    ),
                )

        conn.commit()

    print(f"Ingested {len(new_files)} new files from {data_dir}")
