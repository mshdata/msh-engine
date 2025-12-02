import msh_engine
import dlt
from dlt.sources.sql_database import sql_database

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'psycopg2-binary']
    )

    # Extract from Postgres
    dbt.source("postgres_prod", "users")
    source = sql_database(
        schema="public",
        table_names=["users"]
    )

    # Load to Snowflake with MERGE (upsert)
    destination = dlt.destinations.snowflake()
    
    return msh_engine.transfer(
        dbt=dbt,
        source_data=source,
        target_destination=destination,
        dataset_name="raw_postgres",
        table_name="users_synced",
        write_disposition="merge",  # Upsert mode
        primary_key="id"  # Column to use for deduplication
    )
