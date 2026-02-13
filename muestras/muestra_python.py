"""
Muestra: Como conectarte y graficar datos del proyecto Actinver
===============================================================

Requisitos:
    Movernos a la carpeta muestras:
    cd muestras
    Crear un entorno virtual:
    python3 -m venv .venv
    source .venv/bin/activate
    pip install psycopg2-binary pandas matplotlib
    pip install sqlalchemy


Conexion:
    La base PostgreSQL corre en Docker (ver codigo/docker-compose.yaml).
    Desde tu maquina local, el puerto expuesto es 5434.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine

# ── 1. Conexion a la base de datos ──────────────────────────────────────────
#    Host: localhost  |  Puerto: 5434  |  DB: airflow  |  User/Pass: airflow
ENGINE = create_engine(
    "postgresql+psycopg2://airflow:airflow@localhost:5434/airflow")


# ── 2. Consultas de ejemplo ─────────────────────────────────────────────────

def precios_cierre(tickers: list[str], dias: int = 252) -> pd.DataFrame:
    """Obtiene precios de cierre de los tickers indicados (ultimos N dias)."""
    cols = ", ".join(tickers)
    query = f"""
        SELECT date, {cols}
        FROM market_data.closing_prices
        ORDER BY date DESC
        LIMIT {dias}
    """
    df = pd.read_sql(query, ENGINE, parse_dates=["date"])
    return df.sort_values("date").set_index("date")


def precios_ajustados(ticker_yf: str, dias: int = 252) -> pd.DataFrame:
    """Obtiene precios ajustados (formato largo) para un ticker de Yahoo Finance."""
    query = """
        SELECT ap.date, ap.adj_close, ap.volume, s.yf_ticker
        FROM market_data.adjusted_prices ap
        JOIN market_data.securities s ON s.security_id = ap.security_id
        WHERE s.yf_ticker = %(ticker)s
        ORDER BY ap.date DESC
        LIMIT %(dias)s
    """
    df = pd.read_sql(query, ENGINE, params={"ticker": ticker_yf, "dias": dias},
                     parse_dates=["date"])
    return df.sort_values("date").set_index("date")


def slopes(periodos: list[str], ticker: str, dias: int = 252) -> pd.DataFrame:
    """
    Obtiene las pendientes (slopes) de distintos periodos para un ticker.
    periodos: lista como ["10d", "50d", "200d"]
    ticker:   nombre de columna (ej. "aapl", "msft", "sp500")
    """
    partes = []
    for p in periodos:
        q = f"""
            SELECT date, {ticker} AS slope_{p}
            FROM market_data.slope_{p}
            ORDER BY date DESC
            LIMIT {dias}
        """
        partes.append(pd.read_sql(
            q, ENGINE, parse_dates=["date"]).set_index("date"))
    df = pd.concat(partes, axis=1).sort_index()
    return df


# ── 3. Grafica 1: Precios de cierre comparados ─────────────────────────────

def grafica_precios_cierre():
    """Compara precios de cierre de AAPL, MSFT y GOOGL (ultimo anio)."""
    df = precios_cierre(["aapl", "msft", "googl"], dias=252)

    # Normalizar a base 100 para comparar
    df_norm = df / df.iloc[0] * 100

    fig, ax = plt.subplots(figsize=(12, 6))
    for col in df_norm.columns:
        ax.plot(df_norm.index, df_norm[col], linewidth=1.5, label=col.upper())

    ax.set_title("Precio de cierre normalizado (base 100) - Ultimo anio")
    ax.set_ylabel("Valor (base 100)")
    ax.set_xlabel("Fecha")
    ax.legend()
    ax.grid(alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("grafica_precios_cierre.png", dpi=150)
    plt.show()
    print("-> Guardada: grafica_precios_cierre.png")


# ── 4. Grafica 2: Precio ajustado + volumen ─────────────────────────────────

def grafica_precio_volumen():
    """Precio ajustado y volumen de AAPL (ultimo anio)."""
    df = precios_ajustados("AAPL", dias=252)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True,
                                   gridspec_kw={"height_ratios": [3, 1]})

    # Panel superior: precio ajustado
    ax1.plot(df.index, df["adj_close"], color="steelblue", linewidth=1.5)
    ax1.set_title("AAPL - Precio ajustado y volumen (ultimo anio)")
    ax1.set_ylabel("Precio ajustado (USD)")
    ax1.grid(alpha=0.3)

    # Panel inferior: volumen
    ax2.bar(df.index, df["volume"], color="gray", alpha=0.6, width=1)
    ax2.set_ylabel("Volumen")
    ax2.set_xlabel("Fecha")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))

    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("grafica_precio_volumen.png", dpi=150)
    plt.show()
    print("-> Guardada: grafica_precio_volumen.png")


# ── 5. Grafica 3: Slopes multi-periodo ──────────────────────────────────────

def grafica_slopes():
    """Compara la pendiente de AAPL a 20, 50 y 200 dias."""
    df = slopes(["20d", "50d", "200d"], "aapl", dias=252)

    fig, ax = plt.subplots(figsize=(12, 5))
    colores = {"slope_20d": "tab:green",
               "slope_50d": "tab:orange", "slope_200d": "tab:red"}
    for col, color in colores.items():
        ax.plot(df.index, df[col], linewidth=1.2, label=col.replace("slope_", "Slope "),
                color=color)

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_title("AAPL - Pendiente (slope) de precio ajustado")
    ax.set_ylabel("Slope (USD / dia)")
    ax.set_xlabel("Fecha")
    ax.legend()
    ax.grid(alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("grafica_slopes.png", dpi=150)
    plt.show()
    print("-> Guardada: grafica_slopes.png")


# ── 6. Grafica 4: Rango diario (high - low) ─────────────────────────────────

def grafica_rango_diario():
    """Muestra el rango intradiario (high - low) de TSLA los ultimos 60 dias."""
    query = """
        SELECT h.date, h.tsla AS high, l.tsla AS low
        FROM market_data.high_prices h
        JOIN market_data.low_prices l ON h.date = l.date
        ORDER BY h.date DESC
        LIMIT 60
    """
    df = pd.read_sql(query, ENGINE, parse_dates=["date"]).sort_values(
        "date").set_index("date")
    df["rango"] = df["high"] - df["low"]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(df.index, df["rango"], color="coral", alpha=0.8, width=1)
    ax.set_title("TSLA - Rango intradiario (High - Low) ultimos 60 dias")
    ax.set_ylabel("Rango (USD)")
    ax.set_xlabel("Fecha")
    ax.grid(alpha=0.3, axis="y")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("grafica_rango_diario.png", dpi=150)
    plt.show()
    print("-> Guardada: grafica_rango_diario.png")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Muestras de graficas - Proyecto Actinver")
    print("  Base: PostgreSQL en localhost:5434")
    print("  Schema: market_data")
    print("=" * 60)

    print("\n[1/4] Precios de cierre normalizados (AAPL, MSFT, GOOGL)...")
    grafica_precios_cierre()

    print("\n[2/4] Precio ajustado + volumen (AAPL)...")
    grafica_precio_volumen()

    print("\n[3/4] Slopes multi-periodo (AAPL)...")
    grafica_slopes()

    print("\n[4/4] Rango intradiario (TSLA)...")
    grafica_rango_diario()

    print("\nListo. Se generaron 4 graficas en el directorio actual.")
