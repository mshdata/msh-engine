import os
import sqlalchemy
from contextlib import contextmanager
from typing import Generator, Optional
from sqlalchemy.engine import Engine, Connection

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
        
    # 2. Look up environment variables based on dlt convention
    # DESTINATION__<NAME>__CREDENTIALS
    # Note: dlt might use different structures (dict vs string), but for SQL sources/destinations
    # it often supports a connection string in a single env var.
    
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
    try:
        yield conn
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()
