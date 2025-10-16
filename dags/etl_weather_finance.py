"""
etl_weather_finance.py

airflow DAG that orchestrats a simple ETL:
    ensures postgres table exists
    extract weather + finance data
    transform with Pandas
    load into Postgres

Schedule: daily, no backfill (catchup = False)
"""

from datetime import datetime, timedelta
import os

#airflow core imports
from airflow import DAG
from airflow.operators.python import PythonOperator

#helper function import
from utils import(
        fetch_weather, fetch_finance_daily,
        transform_weather, transform_finance,
        ensure_tables,  load_df
)

#read configs from enviro variables (.env)
# pass into Airflow containers by docker-compose
LAT = float(og.getenv("WEATHER_LAT", "34.10"))
