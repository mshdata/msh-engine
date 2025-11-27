import dbt_bridge
from dlt.sources.helpers.rest_client import RESTClient

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'pandas']
    )

    # 1. Extract: Fetch from API
    client = RESTClient(base_url="https://jsonplaceholder.typicode.com")
    users_generator = client.paginate("/users")

    # 2. Transform: Convert to DataFrame
    # Flatten the pages (list of lists) into a single list of dicts
    # This ensures api_to_df receives a flat list of records
    all_data = []
    for page in users_generator:
        all_data.extend(page)
        
    df = dbt_bridge.api_to_df(all_data)
    
    # This model simply lands the data in our local DuckDB
    return df
