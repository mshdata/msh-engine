import msh_engine
import pandas as pd
from dlt.sources.sql_database import sql_database

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'psycopg2-binary']
    )

    # 1. Extract: Define Postgres Source
    # This reads from the external Postgres database
    source = sql_database(
        schema="public",
        table_names=["users"]
    )

    # 2. Transform: Convert to DataFrame
    # This brings the data into our local execution environment (DuckDB/Pandas)
    df = msh_engine.api_to_df(source)
    
    # We return the DataFrame, which dbt-duckdb saves as a local table 'stg_postgres_users'
    return df
