import msh_engine
import pandas as pd
import dlt
from dlt.sources.helpers.rest_client import RESTClient

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'pandas', 'snowflake-connector-python']
    )

    # 1. Extract: Fetch from API
    client = RESTClient(base_url="https://jsonplaceholder.typicode.com")
    users_generator = client.paginate("/users")

    # 2. Transform: Convert to DataFrame
    df = msh_engine.api_to_df(users_generator)
    
    # Enrich data (e.g., add a timestamp)
    df['ingested_at'] = pd.Timestamp.now()
    
    # Filter (e.g., only users with a website)
    if 'website' in df.columns:
        df_filtered = df[df['website'].notna()]
    else:
        df_filtered = df

    # 3. Load: Define Snowflake Destination
    destination = dlt.destinations.snowflake()

    # 4. Transfer
    receipt_df = msh_engine.transfer(
        dbt=dbt,
        source_data=df_filtered,
        target_destination=destination,
        dataset_name="raw_api",
        table_name="users_enriched",
        write_disposition="replace",
        dbt_source_ref=("external_api", "users")
    )
