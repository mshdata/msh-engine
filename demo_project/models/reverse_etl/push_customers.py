import msh_engine
import dlt

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['dbt-bridge', 'dlt', 'pandas', 'snowflake-connector-python']
    )

    # 1. Read the Modeled Data (from local DuckDB)
    # dbt.ref returns a DuckDB relation. We need to convert it to Arrow or Pandas
    # so dlt can read it.
    customers_df = dbt.ref("customers").arrow()

    # 2. Define Destination
    destination = dlt.destinations.snowflake()

    # 3. Push to Snowflake
    return msh_engine.transfer(
        dbt=dbt,
        source_data=customers_df,
        target_destination=destination,
        dataset_name="analytics_marts",
        table_name="customers_prod",
        write_disposition="replace",
        # We don't need dbt_source_ref here because we are reading from a dbt model
        # The lineage will naturally flow: stg_users -> customers -> push_customers
    )
