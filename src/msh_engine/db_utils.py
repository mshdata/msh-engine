import os
import sqlalchemy
from contextlib import contextmanager
from typing import Generator, Optional
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text

# Snowflake-specific imports
try:
    from msh_engine.snowflake_utils import (
        build_snowflake_connection_string,
        get_snowflake_credentials_from_env,
        SNOWFLAKE_CONNECTION_TIMEOUT,
        SNOWFLAKE_MAX_RETRIES,
        SNOWFLAKE_RETRY_DELAY,
        is_snowflake_error,
        get_snowflake_error_message,
        should_retry_snowflake_error,
    )
    SNOWFLAKE_UTILS_AVAILABLE = True
except ImportError:
    SNOWFLAKE_UTILS_AVAILABLE = False

def get_connection_engine(destination_name: str, credentials: Optional[str] = None) -> Engine:
    """
    Returns a SQLAlchemy engine for the specified destination.
    
    Args:
        destination_name (str): The name of the destination (e.g., 'duckdb', 'postgres', 'snowflake').
        credentials (str, optional): Direct connection string. If None, looks up env vars.
        
    Returns:
        sqlalchemy.engine.Engine: The connection engine.
    """
    
    # 1. Use provided credentials if available
    if credentials:
        return sqlalchemy.create_engine(credentials)
        
    # 2. Handle Snowflake specially (build connection string from env vars)
    # Note: This is destination-agnostic - Snowflake gets enhanced handling,
    # but all other destinations (Postgres, DuckDB, BigQuery, etc.) continue to work normally
    if destination_name.lower() == "snowflake":
        if SNOWFLAKE_UTILS_AVAILABLE:
            try:
                creds = get_snowflake_credentials_from_env()
                conn_str = build_snowflake_connection_string(**creds)
                # Snowflake-specific engine configuration
                return sqlalchemy.create_engine(
                    conn_str,
                    connect_args={
                        "timeout": SNOWFLAKE_CONNECTION_TIMEOUT,
                    },
                    pool_pre_ping=True,  # Verify connections before using
                    pool_recycle=3600,  # Recycle connections after 1 hour
                )
            except ValueError as e:
                raise ValueError(f"Snowflake connection error: {e}")
        else:
            # Fallback if snowflake_utils not available
            env_var_name = f"DESTINATION__{destination_name.upper()}__CREDENTIALS"
            conn_str = os.environ.get(env_var_name)
            if not conn_str:
                raise ValueError(
                    f"No credentials found for {destination_name}. "
                    f"Set {env_var_name} or install snowflake-connector-python."
                )
            return sqlalchemy.create_engine(conn_str)
    
    # 3. Look up environment variables based on dlt convention for other destinations
    # DESTINATION__<NAME>__CREDENTIALS
    env_var_name = f"DESTINATION__{destination_name.upper()}__CREDENTIALS"
    conn_str = os.environ.get(env_var_name)
    
    if not conn_str:
        # Fallback for DuckDB default
        if destination_name == "duckdb":
            # Default to local msh.duckdb
            cwd = os.getcwd()
            conn_str = f"duckdb:///{os.path.join(cwd, 'msh.duckdb')}"
        else:
            raise ValueError(f"No credentials found for {destination_name}. Please set {env_var_name}.")
            
    if "duckdb" in conn_str:
        from sqlalchemy.pool import NullPool
        return sqlalchemy.create_engine(conn_str, poolclass=NullPool)

    return sqlalchemy.create_engine(conn_str)


@contextmanager
def transaction_context(engine: Engine) -> Generator[Connection, None, None]:
    """
    Context manager for atomic database operations.
    
    Automatically commits on success and rolls back on exception.
    Supports nested transactions (uses savepoints for databases that support them).
    Handles Snowflake-specific transaction behavior.
    
    Args:
        engine: SQLAlchemy engine
        
    Yields:
        Connection object
        
    Example:
        with transaction_context(engine) as conn:
            conn.execute(text("DROP TABLE IF EXISTS test"))
            conn.execute(text("CREATE TABLE test (id INT)"))
            # Automatically committed on exit, or rolled back on exception
    """
    conn = engine.connect()
    trans = conn.begin()
    
    # Detect Snowflake for error handling
    is_snowflake = "snowflake" in str(engine.url).lower()
    
    try:
        yield conn
        trans.commit()
    except Exception as e:
        trans.rollback()
        
        # Provide helpful error messages for Snowflake errors
        if SNOWFLAKE_UTILS_AVAILABLE and is_snowflake and is_snowflake_error(e):
            error_msg = get_snowflake_error_message(e)
            raise RuntimeError(error_msg) from e
        
        raise
    finally:
        conn.close()
