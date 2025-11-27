# dbt-bridge


#### A dbt-native data movement layer powered by dlt — for cross-database sync, API ingestion, and (yes) Reverse ETL.
Do everything inside dbt Python models, with full lineage in your DAG.

dbt-bridge lets you extract, transform, and load between any sources and destinations—all inside dbt. It uses dlt for schema-aware loading and uses dbt “Ghost Sources” to keep your lineage complete.

It’s basically:
Move data anywhere → keep everything in one DAG.

## 🚀 Features

- **Cross-Database Movement**:  - Move data from Postgres → Snowflake, MySQL → BigQuery, DuckDB → S3, etc.

- **Reverse ETL (Optional, but supported)**:  - Push your modeled dbt tables into operational systems or external databases.

- **API Ingestion**:  - Pull data from REST APIs, transform using Pandas, and load it to your warehouse.

- **The “Bridge Pattern”**: Extract → Model locally (DuckDB) → Push to another destination.

- **Lineage Support**: Registers “Ghost Sources” so all upstream dependencies appear in dbt docs.

- **dbt Native**: Runs as part of dbt run, not a separate process.

## 📦 Installation

Install the package with only the connectors you need:

``` bash 
pip install "dbt-bridge[snowflake,postgres]"
```


Or install everything:

``` bash
pip install "dbt-bridge[all]"
```

### Supported Extras

- **Warehouses**: ```snowflake```, ```bigquery```, ```redshift```, ```databricks```, ```synapse```, ```fabric```

- **Databases**: ```postgres```, ```mssql```, ```duckdb```, ```trino```, ```athena```

- ***Storage / Filesystems***: ```s3```, ```gcs```, ```azure```, ```filesystem```

## 🧪 Usage Examples

### 1. Database → Database Transfer (Postgres → Snowflake)
Move a table from a source database (e.g., Postgres) to your destination (e.g., Snowflake).


``` python
import dbt_bridge
import dlt
from dlt.sources.sql_database import sql_database

def model(dbt, session):
    dbt.config(materialized='table')

    source = sql_database(schema="public", table_names=["users"])
    dbt.source("postgres_prod", "users")  # lineage

    destination = dlt.destinations.snowflake()

    return dbt_bridge.transfer(
        dbt=dbt,
        source_data=source,
        target_destination=destination,
        dataset_name="raw_postgres",
        table_name="users_synced",
    )
```
### 2. API → Warehouse (with Pandas Transform)

``` python
import dbt_bridge
from dlt.sources.helpers.rest_client import RESTClient

def model(dbt, session):
    dbt.config(materialized='table')

    client = RESTClient(base_url="https://api.example.com")
    raw = client.paginate("/users")

    df = dbt_bridge.api_to_df(raw)
    df["email"] = df["email"].str.lower()

    destination = dlt.destinations.snowflake()

    return dbt_bridge.transfer(
        dbt=dbt,
        source_data=df,
        target_destination=destination,
        dataset_name="raw_api",
        table_name="users",
    )
```


### Incremental Loading

dbt-bridge supports incremental extract → load workflows via dlt’s write_disposition modes.

**Supported Write Dispositions**

- ```replace``` – full refresh (default)

- ```append``` – insert new rows

- ```merge``` – upsert based on a primary key

**Example: Incremental Append**
``` python
return dbt_bridge.transfer(
    dbt=dbt,
    source_data=df,
    target_destination=destination,
    dataset_name="raw_api",
    table_name="users",
    write_disposition="append",
)

```
**Example: Incremental Merge (Upsert)**

``` python
return dbt_bridge.transfer(
    dbt=dbt,
    source_data=df,
    target_destination=destination,
    dataset_name="raw_api",
    table_name="users",
    write_disposition="merge",
    primary_key="user_id",
)
```
### How It Works

- dbt computes upstream changes.
- dbt-bridge converts the model to an Arrow/Pandas-compatible structure.
- dlt performs incremental loads using the configured disposition and primary key.
- Lineage remains fully visible in the dbt DAG.


### 3. The Bridge Pattern (Extract → SQL Transform → Push)

1. **Ingest (Python Model)** – Fetch and stage data locally (DuckDB).
2. **Transform (SQL Model)** – Standard dbt SQL transformations.
3. **Push (Python Model)** – Load the final result to another destination.

``` python
import dbt_bridge
import dlt

def model(dbt, session):
    dbt.config(materialized='table')

    final_df = dbt.ref("int_active_users").arrow()

    destination = dlt.destinations.snowflake()

    return dbt_bridge.transfer(
        dbt=dbt,
        source_data=final_df,
        target_destination=destination,
        dataset_name="analytics_prod",
        table_name="active_users",
    )

```


## 🔧 Configuration

dlt reads credentials from ```.dlt/secrets.toml``` in your dbt project root:

``` toml
[destination.snowflake.credentials]
username = "user"
password = "password"
database = "ANALYTICS"
host = "account_id"
warehouse = "COMPUTE_WH"

[sources.sql_database.credentials]
drivername = "postgresql"
host = "localhost"
port = 5432
database = "source_db"
username = "user"
password = "password" 
```





**License**

MIT


