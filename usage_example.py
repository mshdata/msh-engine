import dbt_bridge
from dlt.sources.sql_database import sql_database

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'psycopg2-binary', 'snowflake-connector-python']
    )

    # 1. Define the Source
    # Example: Reading from a Postgres database
    # Note: Credentials should be handled via dlt secrets.toml or environment variables
    source = sql_database(
        credentials={
            "drivername": "postgresql",
            "username": "db_user",
            "password": "db_password",
            "host": "db_host",
            "port": 5432,
            "database": "source_db"
        },
        schema="public",
        table_names=["users", "orders"]
    )

    # 2. Define the Destination
    # Example: Writing to Snowflake
    # Credentials are typically managed by dlt's configuration/secrets mechanism
    destination = dlt.destinations.snowflake(
        credentials={
            "database": "analytics",
            "schema": "raw_postgres",
            "username": "dbt_user",
            "password": "dbt_password",
            "host": "account.snowflakecomputing.com",
            "warehouse": "compute_wh"
        }
    )

    # 3. Perform the Transfer
    # This will read from Postgres and write to Snowflake
    # It also registers a lineage link from the dbt source "postgres_source.users"
    receipt_df = dbt_bridge.transfer(
        dbt=dbt,
        source_data=source,
        target_destination=destination,
        dataset_name="raw_postgres",
        table_name="users_synced",
        write_disposition="merge", # Merge new data
        dbt_source_ref=("postgres_source", "users")
    )

    return receipt_df
