import dbt_bridge
import pandas as pd
import dlt
from dlt.sources.helpers.rest_client import RESTClient

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'pandas'] 
    )

    # 1. Extract: Fetch data from dummy API
    # We'll use a simple list for speed/reliability in this test
    data = [{'id': 1, 'name': 'Test User 1'}, {'id': 2, 'name': 'Test User 2'}]
    
    # 2. Transform
    df = dbt_bridge.api_to_df(data)
    
    # 3. Load: Write to a separate DuckDB table/file
    # This simulates an "External Destination"
    destination = dlt.destinations.duckdb("external_destination.duckdb")

    # Perform the transfer
    receipt_df = dbt_bridge.transfer(
        dbt=dbt,
        source_data=df,
        target_destination=destination,
        dataset_name="api_data",
        table_name="users",
        write_disposition="replace",
        dbt_source_ref=("api_source", "users")
    )

    return receipt_df
