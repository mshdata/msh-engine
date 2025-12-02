import msh_engine
import dlt
from dlt.sources.sql_database import sql_database

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'psycopg2-binary', 'snowflake-connector-python']
    )

    # Extract from Postgres
    # Note: We skip dbt.source() here because it tries to read from local DuckDB
    # The lineage will be registered via dbt_source_ref parameter instead
    
    source = sql_database(
        schema="public",
        table_names=["users"]
    )

    # Load to Snowflake with MERGE (incremental upsert)
    destination = dlt.destinations.snowflake()
    
    return msh_engine.transfer(
        dbt=dbt,
        source_data=source,
        target_destination=destination,
        dataset_name="raw_postgres",
        table_name="users_incremental",
        write_disposition="merge",
        primary_key="id",  # Deduplicates on this column
        dbt_source_ref=("postgres_prod", "users")  # For lineage
    )
