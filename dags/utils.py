cat > dags/utils.py << 'EOF'

"""
utils.py

Shared helper functions for the ETL DAG:
    Postgres connection
    Extractors for weather and finance API
    simple transform steps
    Loader with idempotent upset
"""

import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text

# Build a Postgres connection URL from enviroment variables
# Values come from .env file and are injected into the airflower container by docker-compose
PG_USER = os.getenv("POSTGRES_USER", "etl")
PG_PW = os.getenv("POSTGRES_PASSWORD", "etl_password")
PG_DB = os.getenv("POSTGRES_DB", "warehouse")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")

#service hostname matches docker-compose service name
PG_URL = F"postgresql+psycopg2://{PG_USER}:{PG_PW}@postgres:{PG_PORT}/{PG_DB}"

#create SQLAlcehmy engine 
ENGINE = create_engine(PG_URL, pool_pre_ping=True)

#extract functions

def fetch_weather(lat: float, lon: float) -> pd.DataFrame:
    """
    Pull hourly weather from Open-Meteo, Return a DataFrame with columns:
        ts(timestamp, temperature_c, windspeed_ms
    """

    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?hourly=temperature_2m,windspeed_10m"
        f"&latitude={lat}&longitude={lon}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    hourly = r.json()["hourly"]


    df = pd.DataFrame({
        "ts": pd.to_datetime(hourly["time"]),
        "temperature_c": hourly["temperature_2m"],
        "windspeed_ms": hourly["windspeed_10m"],
    })
    return df

def fetch_finance_daily(symbol: str, api_key: str) -> pd.DataFrame:
    """
    fetch daily adjusted finance data from alpha vantage
    """
    url: (
        "https://www.alphavantage.co/query"
        "?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={symbol}&apikey={api_key}"
    )

    r = requests.get(url, timeout = 30)
    r.raise_for_status()

    #Alphavantage returns a nested json, guard if the key is missing
    data = r.json().get("Time Series (Daily)", {})
    rows = []
    for d, v in data.items():
        
        rows.append({
            "trade_date": pd.to_datetime(d).date(),
            "symbol": symbol,
            "open": float(v["1. open"]),
            "high": float(v["2. high"]),
            "low": float(v["3. low"]),
            "close": float(v["4. close"]),
            "volume": int(float(v["6. volume"])),
        })
    return pd.DataFrame(rows)



# transform functions

def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
        """ 
        basic data quality
        deduplicate by timestamp, sort chronologically
        """
        return (
            df
            .drop_duplicates(subset=["ts"])
            .sort_values("ts")
            .reset_index(drop=True)
        )

def transform_finance(df: pd.DataFrame) -> pd.DataFrame:
        """
        Data quality for finance
        drop any NULL/NA Rows
        deduplicate
        sort by date ascending
        """
        return (
            df
            .dropna()
            .drop_duplicates(subset=["trade_date"])
            .sort_values("trade_date")
            .reset_index(drop=True)
        )

#load helpers

def ensure_tables():
    """

    """
    ddl_path = "/opt/airflower/includes/sql/create_tables.sql"
    with open(ddl_path, "r", encoding = "utf-8") as f:
        ddl = f.read()

    with ENGINE.begin() as conn:
        for stmt in dll.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def load_df(df: pd.DataFrame, table: str, index: bool = False):
    """

    """

    temp = f"tmp_{table}"

    with ENGINE.begin() as conn:

        df.to_sql(tmp, con= conn, if_exists = "replace", index=index)

        pk = "ts" if table == "weather_hourly" else "trade_date"
        conn.execute(text(f"""
            DELETE FROM {table}
            USING {tmp}
            WHERE {table}.{pk} = {tmp}.{pk};
            INSERT INTO {table} SELECT * FROM {tmp};
            DROP TABLE {tmp};
            """))

EOF
