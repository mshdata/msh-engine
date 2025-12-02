"""
Secure SQL utilities for msh-engine.

This module provides safe SQL operations with input validation and parameterized queries
to prevent SQL injection attacks.
"""
import re
from typing import Optional, Tuple, Any
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


# SQL keywords that should not be used as identifiers
SQL_KEYWORDS = {
    'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE',
    'ALTER', 'TABLE', 'VIEW', 'INDEX', 'DATABASE', 'SCHEMA', 'TRUNCATE',
    'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'UNION', 'OR', 'AND', 'NOT',
    'NULL', 'TRUE', 'FALSE', 'IF', 'ELSE', 'WHEN', 'THEN', 'CASE', 'END',
    'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'LOCK', 'UNLOCK'
}

# Regex patterns for validation
IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*$')
SCHEMA_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*$')
HASH_PATTERN = re.compile(r'^[a-fA-F0-9]+$')


class SQLSecurityError(Exception):
    """Raised when SQL security validation fails."""
    pass


def safe_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validates and returns a safe SQL identifier (table, column, view name).
    
    Args:
        identifier: The identifier to validate
        identifier_type: Type of identifier for error messages (e.g., "table", "column")
        
    Returns:
        The validated identifier
        
    Raises:
        SQLSecurityError: If the identifier is invalid or contains SQL keywords
    """
    if not identifier:
        raise SQLSecurityError(f"Empty {identifier_type} name is not allowed")
    
    if not isinstance(identifier, str):
        raise SQLSecurityError(f"{identifier_type} must be a string, got {type(identifier)}")
    
    # Check length (prevent DoS via extremely long identifiers)
    if len(identifier) > 128:
        raise SQLSecurityError(f"{identifier_type} name too long (max 128 characters)")
    
    # Validate format
    if not IDENTIFIER_PATTERN.match(identifier):
        raise SQLSecurityError(
            f"Invalid {identifier_type} name '{identifier}'. "
            "Must start with a letter and contain only letters, numbers, and underscores."
        )
    
    # Check for SQL keywords
    if identifier.upper() in SQL_KEYWORDS:
        raise SQLSecurityError(
            f"Invalid {identifier_type} name '{identifier}': SQL keywords are not allowed"
        )
    
    return identifier


def safe_schema_name(schema: str) -> str:
    """
    Validates and returns a safe schema name.
    
    Args:
        schema: The schema name to validate
        
    Returns:
        The validated schema name
        
    Raises:
        SQLSecurityError: If the schema name is invalid
    """
    if not schema:
        raise SQLSecurityError("Empty schema name is not allowed")
    
    if not isinstance(schema, str):
        raise SQLSecurityError(f"Schema name must be a string, got {type(schema)}")
    
    if len(schema) > 128:
        raise SQLSecurityError("Schema name too long (max 128 characters)")
    
    if not SCHEMA_PATTERN.match(schema):
        raise SQLSecurityError(
            f"Invalid schema name '{schema}'. "
            "Must start with a letter and contain only letters, numbers, and underscores."
        )
    
    if schema.upper() in SQL_KEYWORDS:
        raise SQLSecurityError(f"Invalid schema name '{schema}': SQL keywords are not allowed")
    
    return schema


def safe_hash(hash_value: str, max_length: int = 32) -> str:
    """
    Validates a hash value (e.g., content hash).
    
    Args:
        hash_value: The hash value to validate
        max_length: Maximum allowed length
        
    Returns:
        The validated hash value
        
    Raises:
        SQLSecurityError: If the hash is invalid
    """
    if not hash_value:
        raise SQLSecurityError("Empty hash value is not allowed")
    
    if not isinstance(hash_value, str):
        raise SQLSecurityError(f"Hash must be a string, got {type(hash_value)}")
    
    if len(hash_value) > max_length:
        raise SQLSecurityError(f"Hash value too long (max {max_length} characters)")
    
    if not HASH_PATTERN.match(hash_value):
        raise SQLSecurityError(
            f"Invalid hash value '{hash_value}'. Must contain only hexadecimal characters."
        )
    
    return hash_value


def execute_safe_query(
    conn: Connection,
    query_template: str,
    parameters: Optional[dict] = None,
    **kwargs
) -> Any:
    """
    Executes a parameterized SQL query safely.
    
    Args:
        conn: SQLAlchemy connection
        query_template: SQL query template with :param_name placeholders
        parameters: Dictionary of parameters to bind
        **kwargs: Additional parameters (merged with parameters dict)
        
    Returns:
        Query result
        
    Example:
        execute_safe_query(
            conn,
            "SELECT * FROM information_schema.views WHERE table_name = :table_name AND table_schema = :schema",
            {"table_name": "my_table", "schema": "main"}
        )
    """
    if parameters is None:
        parameters = {}
    
    # Merge kwargs into parameters
    parameters = {**parameters, **kwargs}
    
    return conn.execute(text(query_template), parameters)


def execute_ddl_safe(
    conn: Connection,
    ddl_template: str,
    schema: Optional[str] = None,
    identifier: Optional[str] = None,
    **kwargs
) -> Any:
    """
    Executes a DDL statement safely with validated identifiers.
    
    Args:
        conn: SQLAlchemy connection
        ddl_template: DDL template with :schema and :identifier placeholders
        schema: Schema name (will be validated)
        identifier: Table/view name (will be validated)
        **kwargs: Additional parameters
        
    Returns:
        Execution result
        
    Example:
        execute_ddl_safe(
            conn,
            "DROP VIEW IF EXISTS :schema.:identifier",
            schema="main",
            identifier="my_view"
        )
    """
    # Validate identifiers first - this will raise SQLSecurityError if invalid
    validated_schema = None
    validated_identifier = None
    
    if schema:
        validated_schema = safe_schema_name(schema)
    if identifier:
        validated_identifier = safe_identifier(identifier)
    
    # Note: SQLAlchemy doesn't support parameterized identifiers directly
    # So we validate them and use string formatting for identifiers only
    # This is safe because we've validated them above using strict regex
    final_ddl = ddl_template
    if validated_schema and ':schema' in final_ddl:
        final_ddl = final_ddl.replace(':schema', validated_schema)
    if validated_identifier and ':identifier' in final_ddl:
        final_ddl = final_ddl.replace(':identifier', validated_identifier)
    
    # Execute with any remaining parameters
    return conn.execute(text(final_ddl), kwargs)


def qualified_name(schema: str, identifier: str) -> str:
    """
    Creates a qualified SQL name (schema.identifier) with validation.
    
    Args:
        schema: Schema name
        identifier: Table/view/column name
        
    Returns:
        Qualified name as "schema.identifier"
    """
    safe_schema_name(schema)
    safe_identifier(identifier)
    return f"{schema}.{identifier}"

