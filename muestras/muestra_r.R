# =============================================================================
# Muestra: Como conectarte y graficar datos del proyecto Actinver (R)
# =============================================================================
#
# Requisitos:
#   install.packages(c("RPostgres", "DBI", "dplyr", "ggplot2", "tidyr", "scales"))
#
# Conexion:
#   La base PostgreSQL corre en Docker (ver codigo/docker-compose.yaml).
#   Desde tu maquina local, el puerto expuesto es 5434.
# =============================================================================

library(RPostgres)
library(DBI)
library(dplyr)
library(ggplot2)
library(tidyr)
library(scales)

# -- 1. Conexion a la base de datos -------------------------------------------
#    Host: localhost  |  Puerto: 5434  |  DB: airflow  |  User/Pass: airflow

con <- dbConnect(
  Postgres(),
  host     = "localhost",
  port     = 5434,
  dbname   = "airflow",
  user     = "airflow",
  password = "airflow"
)

cat("Conexion exitosa a PostgreSQL\n")


# -- 2. Funciones auxiliares ---------------------------------------------------

precios_cierre <- function(tickers, dias = 252) {
  # Obtiene precios de cierre de los tickers indicados (ultimos N dias)
  cols <- paste(tickers, collapse = ", ")
  query <- sprintf(
    "SELECT date, %s FROM market_data.closing_prices ORDER BY date DESC LIMIT %d",
    cols, dias
  )
  df <- dbGetQuery(con, query)
  df$date <- as.Date(df$date)
  df <- df[order(df$date), ]
  return(df)
}

precios_ajustados <- function(ticker_yf, dias = 252) {
  # Obtiene precios ajustados (formato largo) para un ticker de Yahoo Finance
  query <- sprintf(
    "SELECT ap.date, ap.adj_close, ap.volume, s.yf_ticker
     FROM market_data.adjusted_prices ap
     JOIN market_data.securities s ON s.security_id = ap.security_id
     WHERE s.yf_ticker = '%s'
     ORDER BY ap.date DESC
     LIMIT %d",
    ticker_yf, dias
  )
  df <- dbGetQuery(con, query)
  df$date <- as.Date(df$date)
  df <- df[order(df$date), ]
  return(df)
}

slopes_df <- function(periodos, ticker, dias = 252) {
  # Obtiene las pendientes (slopes) de distintos periodos para un ticker
  # periodos: vector como c("10d", "50d", "200d")
  # ticker:   nombre de columna (ej. "aapl", "msft", "sp500")
  partes <- lapply(periodos, function(p) {
    query <- sprintf(
      "SELECT date, %s AS slope_%s FROM market_data.slope_%s ORDER BY date DESC LIMIT %d",
      ticker, p, p, dias
    )
    df <- dbGetQuery(con, query)
    df$date <- as.Date(df$date)
    return(df)
  })

  df <- partes[[1]]
  if (length(partes) > 1) {
    for (i in 2:length(partes)) {
      df <- merge(df, partes[[i]], by = "date", all = TRUE)
    }
  }
  df <- df[order(df$date), ]
  return(df)
}


# -- 3. Grafica 1: Precios de cierre normalizados -----------------------------

grafica_precios_cierre <- function() {
  cat("[1/4] Precios de cierre normalizados (AAPL, MSFT, GOOGL)...\n")
  df <- precios_cierre(c("aapl", "msft", "googl"), dias = 252)

  # Normalizar a base 100
  df$aapl  <- df$aapl  / df$aapl[1]  * 100
  df$msft  <- df$msft  / df$msft[1]  * 100
  df$googl <- df$googl / df$googl[1] * 100

  # Pivotar a formato largo para ggplot
  df_long <- df %>%
    pivot_longer(cols = c(aapl, msft, googl), names_to = "ticker", values_to = "valor") %>%
    mutate(ticker = toupper(ticker))

  p <- ggplot(df_long, aes(x = date, y = valor, color = ticker)) +
    geom_line(linewidth = 0.8) +
    labs(
      title = "Precio de cierre normalizado (base 100) - Ultimo anio",
      x = "Fecha", y = "Valor (base 100)", color = "Ticker"
    ) +
    scale_x_date(labels = date_format("%b %Y")) +
    theme_minimal() +
    theme(legend.position = "bottom")

  ggsave("grafica_precios_cierre_r.png", p, width = 12, height = 6, dpi = 150)
  print(p)
  cat("-> Guardada: grafica_precios_cierre_r.png\n")
}


# -- 4. Grafica 2: Precio ajustado + volumen -----------------------------------

grafica_precio_volumen <- function() {
  cat("[2/4] Precio ajustado + volumen (AAPL)...\n")
  df <- precios_ajustados("AAPL", dias = 252)

  # Panel de precio
  p1 <- ggplot(df, aes(x = date, y = adj_close)) +
    geom_line(color = "steelblue", linewidth = 0.8) +
    labs(title = "AAPL - Precio ajustado (ultimo anio)", y = "Precio (USD)") +
    scale_x_date(labels = date_format("%b %Y")) +
    theme_minimal()

  # Panel de volumen
  p2 <- ggplot(df, aes(x = date, y = volume)) +
    geom_col(fill = "gray60", alpha = 0.7) +
    labs(x = "Fecha", y = "Volumen") +
    scale_x_date(labels = date_format("%b %Y")) +
    scale_y_continuous(labels = label_comma()) +
    theme_minimal()

  # Guardar ambos paneles con patchwork (si esta disponible) o individualmente
  if (requireNamespace("patchwork", quietly = TRUE)) {
    library(patchwork)
    p <- p1 / p2 + plot_layout(heights = c(3, 1))
    ggsave("grafica_precio_volumen_r.png", p, width = 12, height = 8, dpi = 150)
    print(p)
  } else {
    ggsave("grafica_precio_r.png", p1, width = 12, height = 5, dpi = 150)
    ggsave("grafica_volumen_r.png", p2, width = 12, height = 3, dpi = 150)
    print(p1)
    print(p2)
    cat("   (instala 'patchwork' para combinar ambos paneles en una sola imagen)\n")
  }
  cat("-> Guardada: grafica_precio_volumen_r.png\n")
}


# -- 5. Grafica 3: Slopes multi-periodo ----------------------------------------

grafica_slopes <- function() {
  cat("[3/4] Slopes multi-periodo (AAPL)...\n")
  df <- slopes_df(c("20d", "50d", "200d"), "aapl", dias = 252)

  df_long <- df %>%
    pivot_longer(
      cols = starts_with("slope_"),
      names_to = "periodo",
      values_to = "slope"
    ) %>%
    mutate(periodo = gsub("slope_", "Slope ", periodo))

  p <- ggplot(df_long, aes(x = date, y = slope, color = periodo)) +
    geom_line(linewidth = 0.7) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "black", linewidth = 0.5) +
    labs(
      title = "AAPL - Pendiente (slope) de precio ajustado",
      x = "Fecha", y = "Slope (USD / dia)", color = "Periodo"
    ) +
    scale_color_manual(values = c("Slope 20d" = "green3", "Slope 50d" = "orange",
                                  "Slope 200d" = "red3")) +
    scale_x_date(labels = date_format("%b %Y")) +
    theme_minimal() +
    theme(legend.position = "bottom")

  ggsave("grafica_slopes_r.png", p, width = 12, height = 5, dpi = 150)
  print(p)
  cat("-> Guardada: grafica_slopes_r.png\n")
}


# -- 6. Grafica 4: Rango diario (High - Low) -----------------------------------

grafica_rango_diario <- function() {
  cat("[4/4] Rango intradiario (TSLA)...\n")
  query <- "
    SELECT h.date, h.tsla AS high, l.tsla AS low
    FROM market_data.high_prices h
    JOIN market_data.low_prices l ON h.date = l.date
    ORDER BY h.date DESC
    LIMIT 60
  "
  df <- dbGetQuery(con, query)
  df$date  <- as.Date(df$date)
  df$rango <- df$high - df$low
  df <- df[order(df$date), ]

  p <- ggplot(df, aes(x = date, y = rango)) +
    geom_col(fill = "coral", alpha = 0.8) +
    labs(
      title = "TSLA - Rango intradiario (High - Low) ultimos 60 dias",
      x = "Fecha", y = "Rango (USD)"
    ) +
    scale_x_date(labels = date_format("%d %b")) +
    theme_minimal()

  ggsave("grafica_rango_diario_r.png", p, width = 12, height = 5, dpi = 150)
  print(p)
  cat("-> Guardada: grafica_rango_diario_r.png\n")
}


# -- Main ----------------------------------------------------------------------

cat(strrep("=", 60), "\n")
cat("  Muestras de graficas - Proyecto Actinver\n")
cat("  Base: PostgreSQL en localhost:5434\n")
cat("  Schema: market_data\n")
cat(strrep("=", 60), "\n\n")

grafica_precios_cierre()
grafica_precio_volumen()
grafica_slopes()
grafica_rango_diario()

# Cerrar conexion
dbDisconnect(con)
cat("\nListo. Se generaron 4 graficas en el directorio actual.\n")
cat("Conexion cerrada.\n")
