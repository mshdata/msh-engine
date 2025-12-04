# msh-engine

> **The core runtime for the msh Atomic Data Engine.**

[![License](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)

This library bridges the gap between **dlt** (Ingestion) and **dbt** (Transformation), providing the runtime logic for Smart Ingest, Blue/Green Deployment, and Atomic Rollbacks.

> [!WARNING]
> **You likely do not want to install this directly.**
> This is an internal library used by the `msh` command line interface.
>
> Please install the CLI instead:
> ```bash
> pip install msh-cli
> ```

## Technical Capabilities

The engine handles the heavy lifting of the data pipeline, abstracting away the complexity of modern data engineering:

### 🧠 Smart Ingest & Optimization
*   **SQL Query Pushdown**: Analyzes transformation SQL to push column selection and filtering down to the source database, minimizing data transfer.
*   **Schema Evolution**: Automatically detects and adapts to upstream schema changes without breaking downstream models.
*   **Incremental Loading**: Supports incremental and merge strategies for efficient data updates.

### 🔄 Lifecycle Management
*   **Remote State Handling**: Manages deployment state (Blue/Green versions) in the destination warehouse, enabling stateless execution runners.
*   **Atomic Swaps**: Performs zero-downtime `CREATE OR REPLACE VIEW` swaps to ensure data consistency.
*   **Version Tracking**: Tracks asset versions using content hashes for efficient change detection.
*   **Rollback Support**: Instant rollback to previous versions without reprocessing data.

### 🔌 Core Connectivity
*   **REST API**: Generic, configurable loader for any RESTful endpoint with pagination and retry support.
*   **SQL Database**: High-performance connector for Postgres, MySQL, Snowflake, and other SQLAlchemy-supported databases.
*   **GraphQL**: Native support for querying GraphQL APIs.
*   **Universal Destinations**: Support for Snowflake, PostgreSQL, DuckDB, BigQuery, Redshift, and more.

### 🏔️ Snowflake Optimization
*   **Connection Pooling**: Efficient connection management with pre-ping and recycling.
*   **Schema Sanitization**: Automatic uppercase conversion and identifier sanitization for Snowflake compatibility.
*   **Transaction Handling**: Snowflake-specific transaction management with proper BEGIN/COMMIT/ROLLBACK.
*   **Error Handling**: Comprehensive error handling for Snowflake-specific errors (warehouse suspension, timeouts, quotas).
*   **Connection String Building**: Secure credential handling and connection string generation.

### 🔒 Security & Safety
*   **SQL Injection Prevention**: Parameterized queries and SQL validation to prevent injection attacks.
*   **Credential Management**: Secure handling of database credentials with environment variable support.
*   **Transaction Safety**: Atomic transactions with proper rollback on errors.
*   **Connection Cleanup**: Ensures all database connections are properly closed, preventing resource leaks.

### 📊 Data Quality
*   **Test Integration**: Seamless integration with dbt tests for data quality validation.
*   **Schema Validation**: Validates schemas before deployment to prevent breaking changes.
*   **Error Recovery**: Robust error handling with detailed error messages and recovery suggestions.

## Architecture

### Core Components

#### `core.py`
*   `transfer()`: Main transfer function that orchestrates ingestion and transformation
*   `api_to_df()`: Converts API responses to pandas DataFrames
*   `generic_transfer()`: Generic transfer function for various source types

#### `generic.py`
*   `generic_loader()`: dbt model function that puppets dlt for ingestion
*   Handles source verification and connection testing
*   Manages write dispositions (replace, append, merge)

#### `lifecycle.py`
*   `StateManager`: Manages Blue/Green deployment state
*   `get_active_hash()`: Retrieves current deployed version hash
*   `check_table_exists()`: Validates table existence before operations
*   Version tracking and deployment state management

#### `db_utils.py`
*   `get_connection_engine()`: Creates SQLAlchemy engines for various databases
*   `transaction_context()`: Context manager for atomic database operations
*   Snowflake-specific connection handling
*   Connection pooling and resource management

#### `snowflake_utils.py`
*   `build_snowflake_connection_string()`: Constructs Snowflake connection strings
*   `get_snowflake_credentials_from_env()`: Retrieves credentials from environment
*   `sanitize_snowflake_identifier()`: Sanitizes identifiers for Snowflake
*   `is_snowflake_error()`: Detects Snowflake-specific errors
*   `get_snowflake_error_message()`: Provides user-friendly error messages
*   `should_retry_snowflake_error()`: Determines if error is retryable

#### `sql_utils.py`
*   SQL parsing and analysis utilities
*   Column extraction from SQL queries
*   SQL security validation
*   Identifier sanitization

## Usage

### Basic Transfer

```python
import msh_engine
import dlt

def model(dbt, session):
    # Define source
    source = dlt.sources.rest_api(
        endpoint="https://api.example.com/data",
        pagination_strategy="offset"
    )
    
    # Transfer to destination
    return msh_engine.transfer(
        dbt=dbt,
        source_data=source,
        target_destination=dlt.destinations.snowflake(),
        dataset_name="raw_api",
        table_name="data",
        write_disposition="replace"
    )
```

### SQL Database Source

```python
import msh_engine
from dlt.sources.sql_database import sql_database

def model(dbt, session):
    source = sql_database(
        credentials="postgresql://user:pass@host:5432/db",
        schema="public",
        table_names=["users", "orders"]
    )
    
    return msh_engine.transfer(
        dbt=dbt,
        source_data=source,
        target_destination=dlt.destinations.snowflake(),
        dataset_name="raw_postgres",
        table_name="users",
        write_disposition="merge",
        primary_key="id"
    )
```

### Lifecycle Management

```python
from msh_engine.lifecycle import StateManager, get_active_hash

# Get current deployment state
state_manager = StateManager(
    destination="snowflake",
    dataset_name="msh_meta"
)

# Check if asset needs update
current_hash = get_active_hash(
    engine=engine,
    dataset_name="msh_meta",
    asset_name="orders"
)
```

## Database Support

### Snowflake
- Full support with optimized connection handling
- Schema name sanitization (uppercase, length validation)
- Transaction management with explicit BEGIN/COMMIT/ROLLBACK
- Error handling for warehouse suspension, timeouts, and quotas
- Connection pooling with pre-ping and recycling

### PostgreSQL
- Native SQLAlchemy support
- Connection pooling
- Transaction support with savepoints
- Parameterized queries

### DuckDB
- Local file-based database
- In-memory support
- Fast analytical queries
- Default for local development

### Other Databases
- BigQuery (via dlt)
- Redshift (via dlt)
- MySQL (via SQLAlchemy)
- SQLite (via SQLAlchemy)

## Error Handling

The engine provides comprehensive error handling:

### Snowflake-Specific Errors
- Warehouse suspension detection
- Connection timeout handling
- Quota exceeded detection
- Authentication failure handling
- User-friendly error messages with recovery suggestions

### Generic Error Handling
- Connection failures
- SQL syntax errors
- Schema validation errors
- Transaction rollback on errors
- Detailed logging for debugging

## Security Features

### SQL Injection Prevention
- Parameterized queries where supported
- SQL validation before execution
- Identifier sanitization
- Blocked dangerous SQL keywords

### Credential Management
- Environment variable support
- Secure credential storage
- No credential logging
- Read-only role support for queries

### Transaction Safety
- Atomic operations
- Automatic rollback on errors
- Connection cleanup in finally blocks
- Resource leak prevention

## Configuration

### Environment Variables

For Snowflake:
```bash
export DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE="ANALYTICS"
export DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD="secure_password"
export DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME="MSH_USER"
export DESTINATION__SNOWFLAKE__CREDENTIALS__HOST="xyz123.snowflakecomputing.com"
export DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE="COMPUTE_WH"
export DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE="TRANSFORMER"
```

For PostgreSQL:
```bash
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://user:pass@host:5432/db"
```

## Development

### Running Tests
```bash
pytest tests/
```

### Code Quality
```bash
black msh_engine/
flake8 msh_engine/
mypy msh_engine/
```

## License

**msh-engine** is licensed under the **Business Source License (BSL 1.1)**.
You may use this software for non-production or development purposes. Production use requires a commercial license.
