import dlt
import pandas as pd
import time
from datetime import datetime
from typing import Any, Optional, Tuple, Union, List, Iterator
from .logger import logger

def api_to_df(source: Any) -> pd.DataFrame:
    """
    Consumes a dlt source, resource, generator, or list of dicts and converts it to a Pandas DataFrame.
    This is useful for the "API -> RAM -> DB" pattern where you want to transform data in Pandas
    before loading it to the destination.

    Args:
        source: A dlt source, resource, generator, or list of dicts.

    Returns:
        pd.DataFrame: The data as a Pandas DataFrame.
    """
    # If it's already a DataFrame, return it
    if isinstance(source, pd.DataFrame):
        return source

    # If it's a list of dicts, easy conversion
    if isinstance(source, list):
        return pd.DataFrame(source)

    # If it's a dlt source or resource, we can iterate over it
    # dlt sources/resources are iterables that yield data items (dicts)
    try:
        data = list(source)
        return pd.DataFrame(data)
    except Exception as e:
        # Fallback or error if it's not iterable in a way we expect
        raise ValueError(f"Could not convert source to DataFrame. Ensure it is a dlt source, resource, generator, or list. Error: {e}")

def transfer(
    dbt: Any,
    source_data: Any,
    target_destination: Any,
    dataset_name: str,
    table_name: str,
    write_disposition: str = 'replace',
    primary_key: Optional[Union[str, List[str]]] = None,
    dbt_source_ref: Optional[Tuple[str, str]] = None
) -> pd.DataFrame:
    """
    Transfers data from a source to a destination using dlt, running within a dbt Python model.
    
    Args:
        dbt: The dbt object passed to the model function.
        source_data: The data source (e.g., dlt source, generator, list of dicts, or pd.DataFrame).
        target_destination: The dlt destination (e.g., 'snowflake', 'postgres', or a destination object).
        dataset_name: The name of the dataset/schema in the destination.
        table_name: The name of the table in the destination.
        write_disposition: The write disposition. Options:
            - 'replace': Full refresh (default)
            - 'append': Add new rows only
            - 'merge': Upsert based on primary_key
        primary_key: Column(s) to use as primary key for merge operations. 
                     Can be a string (single column) or list of strings (composite key).
                     Required when write_disposition='merge'.
        dbt_source_ref: Optional tuple ("source_name", "table_name") to register lineage in dbt.
        
    Returns:
        pd.DataFrame: A "Receipt" DataFrame containing the status of the load.
    """
    
    # 1. Lineage Hack: Register Ghost Reference
    if dbt_source_ref:
        try:
            source_name, source_table = dbt_source_ref
            # This registers the node in the dbt DAG
            dbt.source(source_name, source_table)
        except Exception as e:
            logger.warning(f"Failed to register dbt source reference: {e}")

    start_time = time.time()
    job_id = f"msh_engine_{int(start_time)}"
    status = "success"
    rows_loaded = 0
    error_message = None

    try:
        # 2. Initialize dlt pipeline
        # We use the dataset_name as the pipeline name to ensure uniqueness if needed, 
        # or a generic name. dlt handles state management.
        # State is stored in the destination to support ephemeral environments.
        pipeline = dlt.pipeline(
            pipeline_name=f"msh_engine_{dataset_name}_{table_name}",
            destination=target_destination,
            dataset_name=dataset_name,
        )

        # 3. Run the pipeline
        # Apply primary key hint if specified (required for merge)
        if primary_key and write_disposition == 'merge':
            # Apply the primary_key hint to the resource
            source_data = dlt.resource(
                source_data,
                name=table_name,
                write_disposition=write_disposition,
                primary_key=primary_key
            )
            load_info = pipeline.run(source_data, table_name=table_name)
        else:
            # dlt.run returns a LoadInfo object
            load_info = pipeline.run(
                source_data,
                table_name=table_name,
                write_disposition=write_disposition
            )
        
        # Extract metrics from load_info
        # dlt LoadInfo provides row counts through load_packages
        try:
            # Try to get row count from load_info.load_packages
            for package in load_info.load_packages:
                # Check for table metrics in the package
                if hasattr(package, 'schema_update') and package.schema_update:
                    # Schema update contains table information
                    for table_name, table_info in package.schema_update.items():
                        if hasattr(table_info, 'row_count'):
                            rows_loaded += table_info.row_count
                        elif isinstance(table_info, dict) and 'row_count' in table_info:
                            rows_loaded += table_info['row_count']
                
                # Also check jobs for row counts
                if hasattr(package, 'jobs') and package.jobs:
                    completed_jobs = package.jobs.get('completed_jobs', [])
                    for job in completed_jobs:
                        # Some dlt versions report row counts in job metrics
                        if hasattr(job, 'file_info') and job.file_info:
                            if hasattr(job.file_info, 'file_size'):
                                # Estimate rows from file size (rough approximation)
                                # This is a fallback if exact counts aren't available
                                pass
                        # Check for row_count attribute
                        if hasattr(job, 'row_count'):
                            rows_loaded += job.row_count
                        elif isinstance(job, dict) and 'row_count' in job:
                            rows_loaded += job['row_count']
            
            # Alternative: Try load_info.load_metrics if available (newer dlt versions)
            if hasattr(load_info, 'load_metrics') and load_info.load_metrics:
                if hasattr(load_info.load_metrics, 'rows_count'):
                    rows_loaded = load_info.load_metrics.rows_count
                elif isinstance(load_info.load_metrics, dict) and 'rows_count' in load_info.load_metrics:
                    rows_loaded = load_info.load_metrics['rows_count']
            
            # If still zero, try to get from pipeline stats
            if rows_loaded == 0 and hasattr(pipeline, 'last_trace'):
                trace = pipeline.last_trace
                if trace and hasattr(trace, 'normalize_info'):
                    normalize_info = trace.normalize_info
                    if normalize_info and hasattr(normalize_info, 'rows'):
                        rows_loaded = normalize_info.rows
        except (AttributeError, TypeError, KeyError) as e:
            # If extraction fails, log warning but don't fail the operation
            logger.warning(f"Could not extract exact row count from load_info: {e}")
            # rows_loaded remains 0, which indicates "unknown" rather than "zero rows"

    except Exception as e:
        status = "failed"
        error_message = str(e)
        logger.error(f"Error during dlt pipeline run: {e}")

    end_time = time.time()
    duration = end_time - start_time
    
    # 4. Return Receipt DataFrame
    receipt_data = {
        'status': [status],
        'rows_loaded': [rows_loaded], # Placeholder, as exact count requires more parsing
        'job_id': [job_id],
        'timestamp': [datetime.utcnow()],
        'duration_seconds': [duration],
        'error_message': [error_message]
    }
    
    return pd.DataFrame(receipt_data)

def generic_transfer(dbt) -> pd.DataFrame:
    """
    A generic dbt Python model that reads configuration from the MSH_JOB_CONFIG
    environment variable and executes a dlt pipeline using the transfer() function.
    
    The MSH_JOB_CONFIG should be a JSON string with the following structure:
    {
        "source": {
            "module": "path.to.module", # e.g., "dlt.sources.helpers.rest_client" or "my_sources"
            "name": "source_func_name", # e.g., "stripe_source"
            "args": { ... },            # Arguments for the source function
            "resource": "resource_name" # Optional: specific resource to select
        },
        "destination": { ... },         # Optional: override destination config
        "dataset_name": "...",
        "table_name": "...",
        "write_disposition": "...",
        "primary_key": ...
    }
    """
    import json
    import os
    import importlib
    
    config_str = os.environ.get("MSH_JOB_CONFIG")
    if not config_str:
        raise ValueError("MSH_JOB_CONFIG environment variable is not set.")
        
    try:
        config = json.loads(config_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in MSH_JOB_CONFIG: {e}")
        
    # Extract config
    source_config = config.get("source", {})
    dataset_name = config.get("dataset_name")
    table_name = config.get("table_name")
    write_disposition = config.get("write_disposition", "replace")
    primary_key = config.get("primary_key")
    
    if not dataset_name or not table_name:
        raise ValueError("dataset_name and table_name are required in MSH_JOB_CONFIG")

    # Dynamic Source Loading
    module_name = source_config.get("module")
    func_name = source_config.get("name")
    source_args = source_config.get("args", {})
    resource_name = source_config.get("resource")
    
    if not module_name or not func_name:
        # Fallback for simple "source: stripe" style if we want to support it, 
        # but for now let's require explicit module/name or handle the CLI side to resolve it.
        # If the CLI resolves "stripe" to "dlt.sources.stripe", that's better.
        # For now, we'll assume the config is fully resolved.
        raise ValueError("source.module and source.name are required in MSH_JOB_CONFIG")
        
    try:
        mod = importlib.import_module(module_name)
        source_func = getattr(mod, func_name)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Could not import source function {func_name} from {module_name}: {e}")
        
    # Instantiate source
    source = source_func(**source_args)
    
    # Select resource if specified
    if resource_name:
        if hasattr(source, "with_resources"):
             source = source.with_resources(resource_name)
        else:
            # If it's not a dlt source (e.g. generator), we can't select resources easily 
            # unless we wrap it. But dlt sources should have this.
            pass

    # Determine destination
    # In dbt context, we usually want to write to the same warehouse dbt is using.
    # But dlt needs a destination. 
    # If not provided, we might default to 'duckdb' for local or 'snowflake' etc.
    # Ideally, we pass the dbt connection info to dlt? 
    # Or we just let dlt handle it via secrets/config.
    # For this implementation, we'll assume dlt is configured via secrets.toml or env vars
    # or passed in config.
    target_destination = config.get("destination", "duckdb") # Default to duckdb for safety? Or maybe 'dummy'?
    # Actually, dbt-bridge transfer() takes target_destination.
    
    return transfer(
        dbt=dbt,
        source_data=source,
        target_destination=target_destination,
        dataset_name=dataset_name,
        table_name=table_name,
        write_disposition=write_disposition,
        primary_key=primary_key,
        dbt_source_ref=(dataset_name, table_name) # Register lineage
    )

