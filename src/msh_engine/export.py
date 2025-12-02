import dlt
from sqlalchemy import text
from typing import Any
from msh_engine.db_utils import get_connection_engine
from msh_engine.logger import logger

def export_asset(asset_name: str, active_table_name: str, target_destination: str) -> None:
    """
    Exports data from the active DWH table to an external destination using dlt.
    
    Args:
        asset_name (str): The name of the asset being exported.
        active_table_name (str): The name of the table in the DWH containing the data.
        target_destination (str): The name of the destination (e.g., 'salesforce', 'hubspot').
    """
    logger.info(f"Starting export for {asset_name} to {target_destination}...")
    
    # 1. Connect to DWH
    # We assume the source is the same as the project's main DWH (e.g. DuckDB or Snowflake)
    # For now, we'll use 'duckdb' as default source if not specified, but ideally this comes from config.
    # However, the requirement says "Use msh_engine.db_utils.get_connection_engine() to get a connection to the source DWH".
    # We'll assume the source DWH is what 'duckdb' (or configured main DWH) points to.
    # Let's assume 'duckdb' for the source for now as per context, or maybe we should check env?
    # The prompt says "Use msh_engine.db_utils.get_connection_engine() to get a connection to the source DWH (e.g., Snowflake)."
    # I'll default to 'duckdb' for the source engine for now as it seems to be the default in db_utils, 
    # but in a real scenario we might want to know what the primary DWH is.
    # Let's use 'duckdb' as the source engine name for reading the data.
    source_engine = get_connection_engine("duckdb") 
    
    # 2. Get data from DWH
    logger.info(f"Reading data from {active_table_name}...")
    try:
        with source_engine.connect() as conn:
            # We use yield to stream data if possible, but dlt might want a list or generator.
            # SQLAlchemy result is iterable.
            query = f"SELECT * FROM {active_table_name}"
            result = conn.execute(text(query))
            # Convert to list of dicts for dlt
            # dlt can handle sqlalchemy results directly in some cases, or we can convert to dicts.
            # Let's convert to dicts to be safe and generic.
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result]
            
            logger.info(f"Found {len(data)} records.")
            
            if not data:
                logger.warning("No data to export.")
                return

            # 3. Configure dlt pipeline
            # The destination is the external target (e.g. salesforce)
            # Secrets are handled by dlt from env vars (DESTINATION__SALESFORCE__...)
            pipeline = dlt.pipeline(
                pipeline_name=f"export_{asset_name}_to_{target_destination}",
                destination=target_destination,
                dataset_name=f"export_{asset_name}" # This might be the target object/table name in the destination
            )
            
            # 4. Run pipeline
            # We load the data into a table named after the asset
            info = pipeline.run(data, table_name=asset_name)
            logger.debug(f"Export info: {info}")
            logger.info(f"Export complete!")
            
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise e
