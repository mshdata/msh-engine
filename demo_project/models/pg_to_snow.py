import dbt_bridge
import pandas as pd
import dlt
from dlt.sources.sql_database import sql_database

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'psycopg2-binary', 'snowflake-connector-python']
    )

    # 1. Extract: Define Postgres Source
    # Credentials are auto-loaded from secrets.toml
    source = sql_database(
        schema="public",
        table_names=["users"]
    )

    # 2. Transform: Convert to DataFrame
    df = dbt_bridge.api_to_df(source)
    
    # Normalize email addresses
    if 'email' in df.columns:
        df['email'] = df['email'].str.lower()
    
    # Filter for active users only
    if 'status' in df.columns:
        df_filtered = df[df['status'] == 'active']
    else:
        df_filtered = df

    # 3. Load: Define Snowflake Destination
    destination = dlt.destinations.snowflake()

    # 4. Transfer
    return dbt_bridge.transfer(
        dbt=dbt,
        source_data=df_filtered,
        target_destination=destination,
        dataset_name="raw_postgres",
        table_name="active_users_synced",
        write_disposition="replace",
        dbt_source_ref=("postgres_prod", "users")
    )
