import re
import os
import dlt
import sqlglot
from sqlglot import exp
from sqlalchemy import text
from typing import Optional, List, Tuple, Dict, Any
from sqlalchemy.engine import Connection
from .sql_utils import (
    safe_identifier, safe_schema_name, safe_hash,
    execute_safe_query, execute_ddl_safe, SQLSecurityError
)
from .logger import logger
from .metadata import MetadataExtractor

def get_active_hash(conn: Connection, asset_name: str, target_schema: str = "main") -> Optional[str]:
    """
    Determines the active hash for an asset by inspecting the view definition.
    Returns the hash string (e.g., "1a8c") or None if not found.
    """
    try:
        # Validate inputs
        try:
            safe_identifier(asset_name, "asset")
            safe_schema_name(target_schema)
        except SQLSecurityError:
            return None
            
        # Query information_schema to get view definition using parameterized query
        query = (
            "SELECT view_definition FROM information_schema.views "
            "WHERE table_name = :table_name AND table_schema = :table_schema"
        )
        
        result = execute_safe_query(
            conn,
            query,
            {"table_name": asset_name, "table_schema": target_schema}
        ).fetchone()
        
        if not result:
            return None
            
        view_def = result[0]
        if view_def:
            view_def = view_def.strip()
        
        # Use sqlglot to parse the view definition
        try:
            parsed = sqlglot.parse_one(view_def)
            
            # Find the source table in the FROM clause
            # We look for tables that match the pattern model_{asset_name}_{hash}
            for table in parsed.find_all(exp.Table):
                name = table.name
                # Check if name matches model_{asset_name}_{hash}
                pattern = fr"model_{asset_name}_([a-zA-Z0-9]+)"
                match = re.search(pattern, name)
                if match:
                    return match.group(1)
                    
        except Exception as e:
            logger.warning(f"Failed to parse view definition for {asset_name}: {e}")
            
        return None
        
    except Exception as e:
        logger.warning(f"Could not determine active hash for {asset_name}: {e}")
        return None

def cleanup_junk(
    conn: Connection, 
    asset_name: str, 
    active_hash: str, 
    target_schema: str = "main", 
    raw_schema: str = "msh_raw"
) -> List[Tuple[str, str, str]]:
    """
    Drops all tables/views matching raw_{asset_name}_* and model_{asset_name}_*
    EXCEPT those matching the active_hash.
    """
    if not active_hash:
        return [] # Safety check: don't delete everything if we don't know what's active
        
    # Validation to prevent SQL Injection
    try:
        safe_identifier(asset_name, "asset")
        safe_hash(active_hash)
        safe_schema_name(target_schema)
        safe_schema_name(raw_schema)
    except SQLSecurityError as e:
        logger.warning(f"Invalid input for cleanup: {e}")
        return []
        
    try:
        # Find junk tables/views
        # We look for tables starting with raw_{asset_name}_ or model_{asset_name}_
        # We check in target_schema (for models) and raw_schema (for raw tables)
        
        # Use parameterized query for schema filtering
        # Note: SQLAlchemy doesn't support parameterized IN lists directly,
        # but we've validated the schemas, so we can safely construct the query
        query = (
            "SELECT table_schema, table_name, table_type FROM information_schema.tables "
            f"WHERE table_schema IN ('{target_schema}', '{raw_schema}')"
        )
        objects = conn.execute(text(query)).fetchall()
        
        junk = []
        for schema, name, type_ in objects:
            # Check if it belongs to this asset
            if name.startswith(f"raw_{asset_name}_") or name.startswith(f"model_{asset_name}_"):
                # Check if it matches active hash
                if active_hash not in name:
                    junk.append((schema, name, type_))
                    
        # Drop junk using safe DDL execution
        dropped_items = []
        for schema, name, type_ in junk:
            try:
                if type_ == 'VIEW':
                    execute_ddl_safe(
                        conn,
                        "DROP VIEW IF EXISTS :schema.:identifier",
                        schema=schema,
                        identifier=name
                    )
                else:
                    execute_ddl_safe(
                        conn,
                        "DROP TABLE IF EXISTS :schema.:identifier",
                        schema=schema,
                        identifier=name
                    )
                dropped_items.append((schema, name, type_))
            except SQLSecurityError as e:
                logger.warning(f"Skipping unsafe drop operation for {schema}.{name}: {e}")
                continue
                
        return dropped_items
                
    except Exception as e:
        logger.warning(f"Janitor failed for {asset_name}: {e}")
        return []

def check_table_exists(conn: Connection, table_name: str, schema: str = "main") -> bool:
    """
    Checks if a table or view exists in the database.
    """
    try:
        # Validate inputs
        try:
            safe_identifier(table_name, "table")
            safe_schema_name(schema)
        except SQLSecurityError:
            return False
        
        # Use safe query execution
        # Note: DESCRIBE is not standard SQL, so we use information_schema instead
        query = (
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = :table_name AND table_schema = :table_schema"
        )
        result = execute_safe_query(
            conn,
            query,
            {"table_name": table_name, "table_schema": schema}
        ).scalar()
        
        return result > 0
    except Exception:
        return False

def get_project_status(conn: Connection, target_schema: str = "main") -> Dict[str, str]:
    """
    Returns a dict of {asset_name: active_hash} for all assets in the project.
    """
    status = {}
    try:
        # Validate schema name
        try:
            safe_schema_name(target_schema)
        except SQLSecurityError:
            return status
            
        # Query information_schema.views using parameterized query
        query = (
            "SELECT table_name, view_definition FROM information_schema.views "
            "WHERE table_schema = :table_schema"
        )
        results = execute_safe_query(
            conn,
            query,
            {"table_schema": target_schema}
        ).fetchall()
        
        for name, view_def in results:
            # Check if it looks like an msh view (select * from model_name_hash)
            pattern = f"model_{name}_([a-zA-Z0-9]+)"
            match = re.search(pattern, view_def)
            if match:
                status[name] = match.group(1)
                
    except Exception as e:
        logger.warning(f"Could not fetch project status: {e}")
        
    return status

class StateManager:
    def __init__(
        self, 
        destination: str = "duckdb", 
        credentials: Optional[str] = None, 
        dataset_name: str = "msh_meta"
    ) -> None:
        self.destination: str = destination
        self.credentials: Optional[str] = credentials
        self.dataset_name: str = dataset_name
        self.pipeline_name: str = "msh_state_manager"

    def _get_pipeline(self) -> Any:
        return dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination,
            dataset_name=self.dataset_name
        )

    def save_deployment_state(self, state_dict: Dict[str, Any]) -> None:
        """
        Persists deployment state to msh_state_history table using dlt.
        Also updates versions.json cache for AI context.
        """
        pipeline = self._get_pipeline()
        try:
            pipeline.run([state_dict], table_name="msh_state_history", write_disposition="append")
            
            # Update versions.json cache
            try:
                import os
                project_root = os.getcwd()
                if project_root.endswith("build"):
                    project_root = os.path.dirname(project_root)
                
                metadata_extractor = MetadataExtractor(project_root=project_root)
                asset_name = state_dict.get("asset_name", "")
                deployment_hash = state_dict.get("hash", "")
                blue_schema = state_dict.get("blue_schema")
                green_schema = state_dict.get("green_schema")
                
                deployment_metadata = metadata_extractor.get_deployment_metadata(
                    asset_name,
                    deployment_hash,
                    blue_schema,
                    green_schema
                )
                metadata_extractor.update_versions_cache(asset_name, deployment_metadata)
            except Exception as e:
                logger.warning(f"Failed to update versions cache: {e}")
        except Exception as e:
            logger.warning(f"Failed to save remote state: {e}")

    def get_latest_deployment(self, asset_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the latest deployment hash for an asset from msh_state_history.
        """
        # Validate asset name and schema first
        try:
            safe_identifier(asset_name, "asset")
            safe_schema_name(self.dataset_name)
        except SQLSecurityError:
            return None
            
        pipeline = self._get_pipeline()
        try:
            with pipeline.sql_client() as client:
                # Note: dlt's execute_sql doesn't support parameterized queries
                # We validate inputs first, then use validated string interpolation
                # This is safe because we've validated asset_name and dataset_name above
                query_safe = (
                    f"SELECT previous_hash, timestamp FROM msh_state_history "
                    f"WHERE asset = '{asset_name}' ORDER BY timestamp DESC LIMIT 1"
                )
                
                try:
                    res = client.execute_sql(query_safe)
                except Exception:
                     # Retry with qualified name
                     query_safe = (
                         f"SELECT previous_hash, timestamp FROM {self.dataset_name}.msh_state_history "
                         f"WHERE asset = '{asset_name}' ORDER BY timestamp DESC LIMIT 1"
                     )
                     res = client.execute_sql(query_safe)

                if res and len(res) > 0:
                    row = res[0]
                    return {"previous_hash": row[0], "timestamp": row[1]}
        except Exception as e:
            # print(f"DEBUG: Remote state fetch failed: {e}")
            pass
        return None

    def get_last_successful_run(self, asset_name: str) -> Optional[float]:
        latest = self.get_latest_deployment(asset_name)
        if latest:
            return latest["timestamp"]
        return None

    def get_asset_history(self, asset_name: str) -> List[Dict[str, Any]]:
        # Validate asset name
        try:
            safe_identifier(asset_name, "asset")
            safe_schema_name(self.dataset_name)
        except SQLSecurityError:
            return []
            
        history = []
        pipeline = self._get_pipeline()
        try:
            with pipeline.sql_client() as client:
                # Use validated string interpolation (safe because we validated above)
                query = (
                    f"SELECT previous_hash, timestamp FROM {self.dataset_name}.msh_state_history "
                    f"WHERE asset = '{asset_name}' ORDER BY timestamp DESC"
                )
                try:
                    res = client.execute_sql(query)
                except Exception:
                     # Retry with unqualified name
                     try:
                         query = (
                             f"SELECT previous_hash, timestamp FROM msh_state_history "
                             f"WHERE asset = '{asset_name}' ORDER BY timestamp DESC"
                         )
                         res = client.execute_sql(query)
                     except Exception:
                         # Table likely doesn't exist yet
                         return []
                     
                for row in res:
                    history.append({
                        "hash": row[0],
                        "timestamp": row[1],
                        "status": "deployed"
                    })
        except Exception as e:
            # print(f"WARNING: Failed to fetch history: {e}")
            pass
        return history
