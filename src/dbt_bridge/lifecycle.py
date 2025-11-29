import re

def get_active_hash(conn, asset_name):
    """
    Determines the active hash for an asset by inspecting the view definition.
    Returns the hash string (e.g., "1a8c") or None if not found.
    """
    try:
        # Query information_schema to get view definition
        # Note: DuckDB view definitions are in sql column of duckdb_views or information_schema.views
        # information_schema.views.view_definition
        query = f"SELECT view_definition FROM information_schema.views WHERE table_name = '{asset_name}' AND table_schema = 'main'"
        result = conn.execute(query).fetchone()
        
        if not result:
            return None
            
        view_def = result[0]
        # View def looks like: CREATE VIEW revenue AS SELECT * FROM main.model_revenue_1a8c;
        # We need to extract "1a8c" from "model_revenue_1a8c"
        
        # Regex to find model_{asset_name}_{hash}
        # Hash is usually 4 chars (from our compiler)
        pattern = f"model_{asset_name}_([a-zA-Z0-9]+)"
        match = re.search(pattern, view_def)
        
        if match:
            return match.group(1)
        return None
        
    except Exception as e:
        print(f"Warning: Could not determine active hash for {asset_name}: {e}")
        return None

def cleanup_junk(conn, asset_name, active_hash):
    """
    Drops all tables/views matching raw_{asset_name}_* and model_{asset_name}_*
    EXCEPT those matching the active_hash.
    """
    if not active_hash:
        return # Safety check: don't delete everything if we don't know what's active
        
    try:
        # Find junk tables/views
        # We look for tables starting with raw_{asset_name}_ or model_{asset_name}_
        # We can use duckdb_tables and duckdb_views or information_schema
        
        # Get all tables and views in main and msh_raw schemas
        query = "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema IN ('main', 'msh_raw')"
        objects = conn.execute(query).fetchall()
        
        junk = []
        for schema, name, type_ in objects:
            # Check if it belongs to this asset
            if name.startswith(f"raw_{asset_name}_") or name.startswith(f"model_{asset_name}_"):
                # Check if it matches active hash
                if active_hash not in name:
                    junk.append((schema, name, type_))
                    
        # Drop junk
        dropped_items = []
        for schema, name, type_ in junk:
            # print(f"🧹 Janitor: Dropping junk {type_} {schema}.{name}")
            if type_ == 'VIEW':
                conn.execute(f"DROP VIEW IF EXISTS {schema}.{name}")
            else:
                conn.execute(f"DROP TABLE IF EXISTS {schema}.{name}")
            dropped_items.append((schema, name, type_))
            
        return dropped_items
                
    except Exception as e:
        print(f"Warning: Janitor failed for {asset_name}: {e}")
        return []
