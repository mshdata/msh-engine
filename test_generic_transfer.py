import os
import json
import pandas as pd
from unittest.mock import MagicMock
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
import dbt_bridge
import dlt

# Dummy source function
def dummy_source(arg1):
    yield {"id": 1, "val": arg1}

def test_generic_transfer():
    # Mock dbt
    dbt = MagicMock()
    dbt.source.side_effect = lambda x, y: print(f"dbt.source called with {x}, {y}")

    # Config
    config = {
        "source": {
            "module": "__main__", # This file
            "name": "dummy_source",
            "args": {"arg1": "hello"},
            "resource": None
        },
        "destination": "duckdb", # Use local duckdb
        "dataset_name": "test_ds",
        "table_name": "test_table",
        "write_disposition": "replace"
    }
    
    os.environ["MSH_JOB_CONFIG"] = json.dumps(config)
    
    # Ensure clean state
    db_file = "dbt_bridge_test.duckdb"
    if os.path.exists(db_file):
        os.remove(db_file)
        
    # We need to tell dlt to use this file for duckdb destination
    # But generic_transfer passes 'duckdb' string as destination.
    # dlt will look for credentials. 
    # For this test, we can patch dbt_bridge.transfer or just set up dlt config.
    # Easier: Let's patch dbt_bridge.transfer to just verify it gets called with correct args,
    # OR we can actually run it if we set up dlt config.
    # Let's try to run it but we need to configure the destination.
    # We can pass a destination object in the config? No, config is JSON.
    # So generic_transfer reads string 'duckdb'.
    # dlt will default to in-memory or error if no creds.
    # Let's set DLT_DESTINATION__DUCKDB__CREDENTIALS env var to the file path.
    
    os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = f"duckdb:///{db_file}"

    print("--- Running generic_transfer test ---")
    try:
        receipt = dbt_bridge.generic_transfer(dbt)
        print("Receipt:")
        print(receipt)
        
        assert receipt['status'][0] == 'success'
        print("SUCCESS: generic_transfer passed")
        
    except Exception as e:
        print(f"FAILURE: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if os.path.exists(db_file):
            os.remove(db_file)

if __name__ == "__main__":
    test_generic_transfer()
