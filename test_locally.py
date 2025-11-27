import dbt_bridge
import pandas as pd
import dlt
from unittest.mock import MagicMock
import duckdb
import os

# Mock dbt object
class MockDBT:
    def __init__(self):
        self.config = MagicMock()
        self.source = MagicMock()
        # Mock source to just print
        self.source.side_effect = lambda x, y: print(f"dbt.source called with {x}, {y}")

def test_local_transfer():
    print("--- Setting up local test ---")
    dbt = MockDBT()
    
    # 1. Source Data (List of Dicts)
    data = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}, {'id': 3, 'name': 'Charlie'}]
    print(f"Source data: {data}")

    # 2. Destination (DuckDB local file)
    db_file = "test_local.duckdb"
    if os.path.exists(db_file):
        os.remove(db_file)
        
    destination = dlt.destinations.duckdb(db_file)

    # 3. Test api_to_df
    print("\n--- Testing api_to_df ---")
    df = dbt_bridge.api_to_df(data)
    print("Converted to DataFrame:")
    print(df)
    
    # 4. Run Transfer
    print("\n--- Testing transfer ---")
    receipt = dbt_bridge.transfer(
        dbt=dbt,
        source_data=df, # Passing DF as source
        target_destination=destination,
        dataset_name="test_dataset",
        table_name="test_table",
        write_disposition="replace",
        dbt_source_ref=("mock_source", "mock_table")
    )

    print("\nTransfer Receipt:")
    print(receipt)

    # 5. Verify data in DuckDB
    print("\n--- Verifying Data in DuckDB ---")
    conn = duckdb.connect(db_file)
    try:
        # dlt creates tables in the dataset_name schema
        result = conn.execute("SELECT * FROM test_dataset.test_table").fetchall()
        print("Data found in DuckDB:")
        for row in result:
            print(row)
        
        assert len(result) == 3
        print("\nSUCCESS: Data verification passed!")
    except Exception as e:
        print(f"\nFAILURE: Could not verify data: {e}")
    finally:
        conn.close()
        # Clean up
        if os.path.exists(db_file):
            os.remove(db_file)

if __name__ == "__main__":
    test_local_transfer()
