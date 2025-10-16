#Airflow ETL Project - Weather & Finance Data

This personal project is a automated Extract Transform Load Pipeline
    -Built with:
            Apache Airflow
            PostgreSQL
            Pandas
    -Purpose
    Extract real-time data, transforms it, and loads it into a Postgres Database


Tech Stack:
    Python
    Apache Airflow
    PostgreSQL
    Pandas
    Docker Compose

 ┌───────────────────┐        ┌────────────────────┐
 │   Weather API     │        │   Finance API       │
 │ (Open-Meteo)      │        │ (Alpha Vantage)    │
 └────────┬──────────┘        └─────────┬───────────┘
          │                             │
          ▼                             ▼
 ┌───────────────────┐       ┌──────────────────────┐
 │  Extract Task     │       │  Extract Task        │
 │ (Airflow Python)  │       │ (Airflow Python)     │
 └────────┬──────────┘       └─────────┬────────────┘
          ▼                             ▼
 ┌────────────────────────────────────────────┐
 │         Transform Task (Pandas)           │
 │   - clean data                           │
 │   - sort / dedupe                         │
 │   - convert formats                        │
 └─────────────────────┬──────────────────────┘
                       ▼
             ┌──────────────────┐
             │   Load Task      │
             │ (SQL upsert)     │
             └────────┬─────────┘
                      ▼
             ┌──────────────────┐
             │ PostgreSQL DB    │
             │ weather + finance│
             └────────┬─────────┘
                      ▼
             ┌──────────────────┐
             │ Dashboard / BI   │  
             └──────────────────



#HOW TO:

    Clone Repo
    Create ENV file
    """
    
    """


    Start Airflow + Postgres with Docker
    docker compose up airflow-init --build
    docker compose up -d p


    then go to localhost:8080
