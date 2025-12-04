"""
Snowflake-specific utilities for connection handling, validation, and error management.
"""
import os
import re
import time
from typing import Optional, Dict, Any
from urllib.parse import quote_plus

try:
    import snowflake.connector.errors as snowflake_errors
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake_errors = None


# Snowflake connection constants
SNOWFLAKE_CONNECTION_TIMEOUT = 30
SNOWFLAKE_MAX_RETRIES = 3
SNOWFLAKE_RETRY_DELAY = 2  # seconds
SNOWFLAKE_MAX_IDENTIFIER_LENGTH = 255  # Snowflake limit
SNOWFLAKE_RECOMMENDED_IDENTIFIER_LENGTH = 63  # Best practice


def build_snowflake_connection_string(
    account: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    schema: Optional[str] = None
) -> str:
    """
    Builds a Snowflake SQLAlchemy connection string from individual components.
    
    Args:
        account: Snowflake account identifier (e.g., 'xyz123' or 'xyz123.us-east-1')
        user: Username
        password: Password
        database: Database name
        warehouse: Warehouse name
        role: Role name (optional)
        schema: Schema name (optional)
        
    Returns:
        SQLAlchemy-compatible connection string
        
    Raises:
        ValueError: If required parameters are missing
    """
    if not account or not user or not password:
        raise ValueError(
            "Snowflake connection requires account, user, and password. "
            "Set DESTINATION__SNOWFLAKE__CREDENTIALS__ACCOUNT, "
            "DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME, and "
            "DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD environment variables."
        )
    
    # Build connection string components
    # Format: snowflake://user:password@account/database?warehouse=warehouse&role=role&schema=schema
    conn_parts = [f"snowflake://{quote_plus(user)}:{quote_plus(password)}@{account}"]
    
    if database:
        conn_parts.append(f"/{database}")
    
    query_params = []
    if warehouse:
        query_params.append(f"warehouse={quote_plus(warehouse)}")
    if role:
        query_params.append(f"role={quote_plus(role)}")
    if schema:
        query_params.append(f"schema={quote_plus(schema)}")
    
    if query_params:
        conn_parts.append("?" + "&".join(query_params))
    
    return "".join(conn_parts)


def get_snowflake_credentials_from_env() -> Dict[str, Any]:
    """
    Extracts Snowflake credentials from environment variables following dlt convention.
    
    Expected env vars:
    - DESTINATION__SNOWFLAKE__CREDENTIALS__ACCOUNT (or HOST)
    - DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME (or USER)
    - DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD
    - DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE
    - DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE
    - DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE
    
    Returns:
        Dictionary with credential keys
        
    Raises:
        ValueError: If required credentials are missing
    """
    # Try dlt convention first
    account = (
        os.environ.get("DESTINATION__SNOWFLAKE__CREDENTIALS__ACCOUNT") or
        os.environ.get("DESTINATION__SNOWFLAKE__CREDENTIALS__HOST")
    )
    user = (
        os.environ.get("DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME") or
        os.environ.get("DESTINATION__SNOWFLAKE__CREDENTIALS__USER")
    )
    password = os.environ.get("DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD")
    database = os.environ.get("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE")
    warehouse = os.environ.get("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE")
    role = os.environ.get("DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE")
    
    if not account or not user or not password:
        raise ValueError(
            "Missing required Snowflake credentials. Set:\n"
            "  - DESTINATION__SNOWFLAKE__CREDENTIALS__ACCOUNT (or HOST)\n"
            "  - DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME (or USER)\n"
            "  - DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"
        )
    
    # Extract account identifier from host if needed
    # e.g., "xyz123.snowflakecomputing.com" -> "xyz123"
    if ".snowflakecomputing.com" in account:
        account = account.split(".")[0]
    
    return {
        "account": account,
        "user": user,
        "password": password,
        "database": database,
        "warehouse": warehouse,
        "role": role,
    }


def sanitize_snowflake_identifier(identifier: str, max_length: int = SNOWFLAKE_RECOMMENDED_IDENTIFIER_LENGTH) -> str:
    """
    Sanitizes an identifier for Snowflake compatibility.
    
    Snowflake identifiers:
    - Are case-sensitive but typically uppercase
    - Can contain letters, numbers, and underscores
    - Cannot start with a number
    - Should be uppercase by convention
    - Have a max length of 255 chars (recommended: 63)
    
    Args:
        identifier: Original identifier string
        max_length: Maximum length (default: 63)
        
    Returns:
        Sanitized, uppercase identifier
    """
    if not identifier:
        return identifier
    
    # Convert to uppercase (Snowflake convention)
    identifier = identifier.upper()
    
    # Replace invalid characters with underscores
    # Allow: letters, numbers, underscores
    identifier = re.sub(r'[^A-Z0-9_]', '_', identifier)
    
    # Remove leading/trailing underscores
    identifier = identifier.strip('_')
    
    # Ensure it doesn't start with a number
    if identifier and identifier[0].isdigit():
        identifier = f"_{identifier}"
    
    # Truncate to max length
    if len(identifier) > max_length:
        identifier = identifier[:max_length]
    
    # Ensure it's not empty
    if not identifier:
        identifier = "DEFAULT"
    
    return identifier


def validate_snowflake_identifier(identifier: str) -> bool:
    """
    Validates that an identifier is valid for Snowflake.
    
    Args:
        identifier: Identifier to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not identifier:
        return False
    
    if len(identifier) > SNOWFLAKE_MAX_IDENTIFIER_LENGTH:
        return False
    
    # Check for invalid characters
    if re.search(r'[^A-Z0-9_]', identifier.upper()):
        return False
    
    # Cannot start with a number
    if identifier[0].isdigit():
        return False
    
    return True


def is_snowflake_error(exception: Exception) -> bool:
    """
    Checks if an exception is a Snowflake-specific error.
    
    Args:
        exception: Exception to check
        
    Returns:
        True if it's a Snowflake error
    """
    if not SNOWFLAKE_AVAILABLE:
        return False
    
    return isinstance(exception, (
        snowflake_errors.ProgrammingError,
        snowflake_errors.OperationalError,
        snowflake_errors.DatabaseError,
        snowflake_errors.InterfaceError,
    ))


def get_snowflake_error_message(exception: Exception) -> str:
    """
    Extracts a user-friendly error message from a Snowflake exception.
    
    Args:
        exception: Snowflake exception
        
    Returns:
        User-friendly error message with suggested fixes
    """
    if not SNOWFLAKE_AVAILABLE or not is_snowflake_error(exception):
        return str(exception)
    
    error_code = getattr(exception, 'errno', None)
    error_msg = str(exception)
    
    # Map common Snowflake error codes to helpful messages
    error_messages = {
        250001: "Invalid username or password. Check your credentials.",
        250003: "Warehouse not found or not accessible. Verify warehouse name and permissions.",
        250005: "Database not found. Verify database name and permissions.",
        250006: "Schema not found. Verify schema name and permissions.",
        250007: "Role not found or not granted. Verify role name and permissions.",
        250008: "Warehouse is suspended. Resume it in the Snowflake UI.",
        250009: "Connection timeout. Check network connectivity and warehouse status.",
        250010: "Quota exceeded. Check your Snowflake account limits.",
    }
    
    if error_code in error_messages:
        return f"{error_messages[error_code]} (Error {error_code}: {error_msg})"
    
    # Check for common error patterns
    error_lower = error_msg.lower()
    if "warehouse" in error_lower and "suspended" in error_lower:
        return f"Warehouse is suspended. Resume it in the Snowflake UI. ({error_msg})"
    if "timeout" in error_lower:
        return f"Connection timeout. Check network connectivity and warehouse status. ({error_msg})"
    if "quota" in error_lower or "limit" in error_lower:
        return f"Quota exceeded. Check your Snowflake account limits. ({error_msg})"
    if "authentication" in error_lower or "login" in error_lower:
        return f"Authentication failed. Check your credentials. ({error_msg})"
    
    return f"Snowflake error: {error_msg}"


def should_retry_snowflake_error(exception: Exception) -> bool:
    """
    Determines if a Snowflake error is transient and should be retried.
    
    Args:
        exception: Exception to check
        
    Returns:
        True if the error should be retried
    """
    if not is_snowflake_error(exception):
        return False
    
    error_code = getattr(exception, 'errno', None)
    error_msg = str(exception).lower()
    
    # Retry on transient errors
    retryable_codes = [250009]  # Connection timeout
    retryable_patterns = ["timeout", "connection", "network", "temporary"]
    
    if error_code in retryable_codes:
        return True
    
    for pattern in retryable_patterns:
        if pattern in error_msg:
            return True
    
    return False

