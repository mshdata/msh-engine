import msh_engine
import pandas as pd
import dlt
from dlt.sources.helpers.rest_client import RESTClient

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'pandas', 'psycopg2-binary']
    )

    # 1. Extract: Fetch data from API
    # Using dlt's RESTClient for robust API interaction (pagination, retries)
    # Example: Fetching users from a dummy API
    client = RESTClient(base_url="https://jsonplaceholder.typicode.com")
    
    # This returns a generator/resource
    users_generator = client.paginate("/users")

    # 2. Transform: Convert to Pandas and filter in memory
    # Use dbt-bridge helper to get DataFrame
    df = msh_engine.api_to_df(users_generator)
    
    # Apply business logic
    # Example: Keep only users with ID > 5
    clean_df = df[df['id'] > 5]

    # 3. Load: Push to Destination
    # Example: Writing to Postgres
    destination = dlt.destinations.postgres(
        credentials={
            "database": "analytics",
            "schema": "raw_api",
            "username": "db_user",
            "password": "db_password",
            "host": "db_host",
            "port": 5432
        }
    )

    # Perform the transfer
    # Registers lineage to a ghost node "api_source.users"
    return msh_engine.transfer(
        dbt=dbt,
        source_data=clean_df,
        target_destination=destination,
        dataset_name="raw_api",
        table_name="users_filtered",
        write_disposition="replace",
        dbt_source_ref=("api_source", "users")
    )

    return receipt_df
