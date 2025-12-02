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

### 🔄 Lifecycle Management
*   **Remote State Handling**: Manages deployment state (Blue/Green versions) in the destination warehouse, enabling stateless execution runners.
*   **Atomic Swaps**: Performs zero-downtime `CREATE OR REPLACE VIEW` swaps to ensure data consistency.

### 🔌 Core Connectivity
*   **REST API**: Generic, configurable loader for any RESTful endpoint.
*   **SQL Database**: High-performance connector for Postgres, MySQL, and other SQLAlchemy-supported databases.
*   **GraphQL**: Native support for querying GraphQL APIs.

## License

**msh-engine** is licensed under the **Business Source License (BSL 1.1)**.
You may use this software for non-production or development purposes. Production use requires a commercial license.
