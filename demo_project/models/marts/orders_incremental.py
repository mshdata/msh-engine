import msh_engine
import dlt
from dlt.sources.sql_database import sql_database

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'psycopg2-binary', 'snowflake-connector-python']
    )

    # Extract orders from Postgres
    # This will read the 'orders' table we just created
    source = sql_database(
        schema="public",
        table_names=["orders"]
    )

    # Load to Snowflake with MERGE
    # This will upsert based on order_id
    destination = dlt.destinations.snowflake()
    
    return msh_engine.transfer(
        dbt=dbt,
        source_data=source,
        target_destination=destination,
        dataset_name="raw_postgres",
        table_name="orders_incremental",
        write_disposition="merge",
        primary_key="order_id",  # Upsert on this column
        dbt_source_ref=("postgres_prod", "orders")
    )
