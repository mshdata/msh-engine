import os
import json
import dlt
import requests # Ensure this is installed
import pandas as pd

def generic_loader(dbt):
    """
    Puppets dlt via Environment Variables.
    Supports 'rest_api' type natively to avoid import errors.
    """
    # 1. Get Config
    config_json = os.environ.get("MSH_JOB_CONFIG")
    if not config_json:
        # Fallback for connection testing
        return pd.DataFrame({"status": ["skipped (no config)"]})
    
    config = json.loads(config_json)
    
    # 2. Extract Basic Vars
    source_config = config.get("source", {})
    source_type = source_config.get("type", "module") # Default to old behavior if missing
    target_table = config.get("table_name") # User prompt said target_table, but code usually uses table_name in dlt.run. 
    # Wait, the prompt code says: target_table = config.get("target_table")
    # And then: table_name=target_table
    # But my previous main.py passes "table_name": target_raw_table.
    # I should check main.py to see what key it sends.
    # In main.py: "table_name": target_raw_table
    # So I should probably use "table_name" or update main.py to send "target_table".
    # The prompt code uses "target_table". I will stick to the prompt's code for generic.py 
    # BUT I must ensure main.py sends "target_table" OR generic.py reads "table_name".
    # Let's look at the prompt's generic.py again.
    # target_table = config.get("target_table")
    # ... table_name=target_table
    # If main.py sends "table_name", this will be None.
    # I should probably check main.py.
    # In Step 411, main.py sends: "table_name": target_raw_table
    # So I should change generic.py to read "table_name" OR change main.py.
    # The user provided specific code for generic.py. I should probably use it but maybe fix the key if I can.
    # Or I'll update main.py to send target_table as well.
    # Actually, looking at the user request for main.py, it only updates the EXAMPLE_MSH string.
    # It doesn't ask to update the `run` loop where the config is constructed.
    # If I use the user's generic.py as is, it expects `target_table`.
    # My `main.py` sends `table_name`.
    # I will verify this assumption.
    # I will use `config.get("table_name")` instead of `target_table` to be safe, or support both.
    # `target_table = config.get("target_table") or config.get("table_name")`
    
    write_disposition = config.get("write_disposition", "replace")
    
    # 3. DEFINE THE SOURCE
    # --- FIX: Handle REST API natively ---
    if source_type == "rest_api":
        endpoint = source_config.get("endpoint")
        resource_name = source_config.get("resource", "data")
        
        print(f"DEBUG: Fetching REST API: {endpoint}")
        
        # Simple fetch
        try:
            response = requests.get(endpoint)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
             raise ValueError(f"Failed to fetch API {endpoint}: {e}")

        # Create dlt resource on the fly
        source_data = dlt.resource(
            data, 
            name=resource_name, 
            write_disposition=write_disposition
        )
        
    else:
        # --- LEGACY: Dynamic Module Import (The old way) ---
        # This is where it was crashing on 'pokemon'
        import importlib
        module_name = source_config.get("module")
        func_name = source_config.get("name")
        
        try:
            mod = importlib.import_module(module_name)
            source_func = getattr(mod, func_name)
            source_data = source_func(**source_config.get("args", {}))
        except Exception as e:
             raise ValueError(f"Could not load source {module_name}.{func_name}: {e}")

    # 4. RUN PIPELINE (With PyArrow/Parquet for speed)
    pipeline = dlt.pipeline(
        pipeline_name="msh_runner",
        destination="duckdb", # Or snowflake, based on env vars
        dataset_name="msh_raw"
    )
    
    # Check for Smart Ingest columns
    # (If your compiler passed specific columns, dlt can filter here if supported, 
    #  or we rely on the SQL transformation later)
    
    # FIX: Ensure we have a table name
    target_table = config.get("target_table") or config.get("table_name")
    if not target_table:
        raise ValueError("Target table name not found in config (checked 'target_table' and 'table_name')")

    info = pipeline.run(
        source_data,
        table_name=target_table,
        loader_file_format="parquet" 
    )
    
    return pd.DataFrame([{"status": "success", "info": str(info)}])