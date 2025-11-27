import dbt_bridge
import dlt

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'pandas', 'snowflake-connector-python']
    )

    # 1. Read the Final Modeled Data
    # This reads the 'final_users' table from our local DuckDB
    # Convert to Arrow for dlt compatibility
    final_df = dbt.ref("final_users").arrow()

    # 2. Define Destination
    # We are pushing this final dataset to Snowflake
    destination = dlt.destinations.snowflake()

    # 3. Push to Snowflake
    # This creates the table 'FINAL_USERS_REPORT' in the 'ANALYTICS_PROD' dataset in Snowflake
    return dbt_bridge.transfer(
        dbt=dbt,
        source_data=final_df,
        target_destination=destination,
        dataset_name="analytics_prod",
        table_name="final_users_report",
        write_disposition="replace"
    )
