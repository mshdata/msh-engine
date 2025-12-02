import dlt
import sqlalchemy
from sqlalchemy import text
from typing import Any, Dict, List, Optional
from .base import SourceStrategy
from ..logger import logger
from ..sql_utils import safe_identifier

class SqlDatabaseSource(SourceStrategy):
    def get_source(
        self, 
        source_config: Dict[str, Any], 
        columns: Optional[List[str]], 
        write_disposition: str, 
        incremental_config: Optional[Dict[str, Any]] = None,
        contract_config: Optional[Dict[str, Any]] = None
    ) -> Any:
        try:
            from dlt.sources.sql_database import sql_database
        except ImportError:
            raise ImportError("The 'sql_database' source is not available. Please install it or ensure dlt is configured correctly.")
            
        credentials = source_config.get("credentials")
        table_name = source_config.get("table")
        
        if columns:
            # Feature 1A: Pushdown Optimization (Smart Ingest)
            # Validate column names for safety (safe_identifier validates but doesn't quote)
            # We need to quote them ourselves for SQL
            validated_columns = [safe_identifier(col) for col in columns]
            safe_columns = [f'"{col}"' for col in validated_columns]
            cols_sql = ", ".join(safe_columns)
            
            # Handle table name (may be schema.table format)
            if '.' in table_name:
                # Schema.table format - validate and quote each part
                schema_part, table_part = table_name.split('.', 1)
                validated_schema = safe_identifier(schema_part)
                validated_table = safe_identifier(table_part)
                safe_table = f'"{validated_schema}"."{validated_table}"'
            else:
                validated_table = safe_identifier(table_name)
                safe_table = f'"{validated_table}"'
            
            query = f"SELECT {cols_sql} FROM {safe_table}"
            
            # Task 1: Incremental Logic for SQL Pushdown
            incremental_arg = None
            
            if incremental_config:
                cursor_field = incremental_config.get("cursor_field")
                if cursor_field:
                    logger.debug(f"Incremental SQL Pushdown on {cursor_field}")
                    incremental_arg = dlt.sources.incremental(cursor_field)
                    
            logger.debug(f"Smart Ingest (Pushdown) - {query}")
            
            def query_yielder(incremental=incremental_arg):
                engine = sqlalchemy.create_engine(credentials)
                
                with engine.connect() as conn:
                    # Use parameters for safety
                    params = {}
                    final_query_str = query
                    
                    if incremental and incremental.last_value:
                         # We assume table_name and cursor_path are safe (from config/schema)
                         # But last_value comes from state/DB, so we MUST parameterize it.
                         
                         # Check if WHERE exists
                         if "WHERE" in final_query_str.upper():
                             final_query_str += f" AND {incremental.cursor_path} > :cursor_val"
                         else:
                             final_query_str += f" WHERE {incremental.cursor_path} > :cursor_val"
                         
                         params["cursor_val"] = incremental.last_value
                         logger.debug(f"Incremental Query: {final_query_str} | Params: {params}")

                    # Execute with parameters
                    result = conn.execute(text(final_query_str), params)
                    keys = result.keys()
                    for row in result:
                        yield dict(zip(keys, row))
                        
            source_data = dlt.resource(
                query_yielder,
                name=table_name,
                write_disposition=write_disposition,
                args={"incremental": incremental_arg} if incremental_arg else None
            )
            
            # Apply contract config (schema evolution control)
            if contract_config:
                evolution_mode = contract_config.get("evolution", "evolve")
                if evolution_mode == "freeze":
                    source_data.apply_hints(schema_evolution="freeze")
                    logger.debug("Schema evolution frozen (contract: evolution=freeze)")
            
        else:
            source = sql_database(credentials=credentials)
            
            if table_name and table_name in source.resources:
                source_data = source.resources[table_name]
                source_data.write_disposition = write_disposition
                
                # Apply contract config (schema evolution control)
                if contract_config:
                    evolution_mode = contract_config.get("evolution", "evolve")
                    if evolution_mode == "freeze":
                        source_data.apply_hints(schema_evolution="freeze")
                        logger.debug("Schema evolution frozen (contract: evolution=freeze)")
                
                # If incremental, we might need to configure the resource
                if incremental_config:
                     cursor_field = incremental_config.get("cursor_field")
                     if cursor_field:
                         # dlt sql_database source might handle this if configured?
                         pass
            else:
                raise ValueError(f"Table '{table_name}' not found in database source.")
                
        return source_data
