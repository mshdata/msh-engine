import msh_engine
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
    # This ensures api_to_df receives a flat list of records
    all_data = []
    for page in users_generator:
        all_data.extend(page)
        
    df = msh_engine.api_to_df(users_generator)
    
    # This model simply lands the data in our local DuckDB
    return df
