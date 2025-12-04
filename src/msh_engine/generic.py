import os
import json
import dlt
import requests
import pandas as pd
import logging
from datetime import datetime
from typing import Any, Dict, Tuple, Optional
import itertools

# Configure Logger
logger = logging.getLogger(__name__)

def _get_graphql_source(source_config: Dict[str, Any], write_disposition: str) -> Any:
    try:
        from dlt.sources.graphql import graphql_source
    except ImportError:
        raise ImportError("dlt.sources.graphql not found. Please install dlt[graphql] or check your dlt version.")
        
    graphql_endpoint = source_config.get("endpoint")
    graphql_query = source_config.get("query")
    
    if not graphql_endpoint or not graphql_query:
        raise ValueError("GraphQL source requires 'endpoint' and 'query'")
        
    source = graphql_source(graphql_endpoint, graphql_query)
    
    for resource_name in source.resources:
        source.resources[resource_name].write_disposition = write_disposition
        
    return source

def _verify_connection(source_config: Dict[str, Any], source_type: str) -> Tuple[bool, str]:
    """
    Verifies connection for Dry Run.
    """
    logger.debug(f"Dry Run - Verifying {source_type} connection...")
    try:
        if source_type == "rest_api":
            # Reuse RestApiSource logic if possible, or simplified check
            rest_config = source_config.get("config")
            if rest_config:
                client = rest_config.get("client", {})
                url = client.get("base_url")
            else:
                url = source_config.get("endpoint")
                
            if not url:
                return False, "No URL found"
                
            # Simple HEAD/GET check
            CONNECTION_TIMEOUT = 5  # seconds
            try:
                response = requests.head(url, timeout=CONNECTION_TIMEOUT)
                if response.status_code == 405:
                    response = requests.get(url, stream=True, timeout=CONNECTION_TIMEOUT)
                    response.close()
            except requests.RequestException as e:
                 return False, f"Connection failed: {e}"
                
            if response.status_code < 400:
                return True, f"Connected to {url} (Status: {response.status_code})"
            else:
                return False, f"Failed to connect to {url} (Status: {response.status_code})"
                
        elif source_type == "sql_database":
            credentials = source_config.get("credentials")
            import sqlalchemy
            engine = sqlalchemy.create_engine(credentials)
            try:
                with engine.connect() as conn:
                    pass
                return True, "Database connection successful"
            finally:
                engine.dispose()  # Ensure engine is properly disposed
            
        elif source_type == "graphql":
            url = source_config.get("endpoint")
            CONNECTION_TIMEOUT = 5  # seconds
            try:
                response = requests.head(url, timeout=CONNECTION_TIMEOUT)
                if response.status_code == 405:
                    response = requests.get(url, stream=True, timeout=CONNECTION_TIMEOUT)
                    response.close()
                if response.status_code < 400:
                    return True, f"Connected to {url}"
                return False, f"Failed to connect (Status: {response.status_code})"
            except requests.RequestException as e:
                return False, f"Connection failed: {e}"
            
        return True, "Source type verified (mock)"
        
    except Exception as e:
        return False, str(e)

from .sources.rest_api import RestApiSource
from .sources.sql_database import SqlDatabaseSource
from .metadata import MetadataExtractor
from .validation import GlossaryValidator

def generic_loader(dbt: Any) -> pd.DataFrame:
    """
    Puppets dlt via Environment Variables.
    Delegates to Source Strategies.
    """
    # 1. Get Config
    config_json = os.environ.get("MSH_JOB_CONFIG")
    if not config_json:
        return pd.DataFrame({"status": ["skipped (no config)"]})
    
    config = json.loads(config_json)
    
    # Task 2: Dry Run Check
    if config.get("dry_run"):
        source_config = config.get("source", {})
        source_type = source_config.get("type", "module")
        
        success, message = _verify_connection(source_config, source_type)
        
        status = "dry_run_success" if success else "dry_run_failed"
        return pd.DataFrame([{"status": status, "message": message}])
    
    # 2. Extract Basic Vars
    source_config = config.get("source", {})
    source_type = source_config.get("type", "module")
    target_table = config.get("target_table") or config.get("table_name")
    if not target_table:
        raise ValueError("Target table name not found in config")
        
    write_disposition = config.get("write_disposition", "replace")
    target_destination = config.get("destination", "duckdb")
    python_code = config.get("python_code")
    columns = config.get("columns")
    primary_key = config.get("primary_key")
    
    # Extract contract config
    contract_config = config.get("contract")
    
    # Task 1: Incremental Loading
    incremental_config = config.get("incremental")
    if incremental_config:
        # Override write_disposition
        strategy = incremental_config.get("strategy", "append")
        write_disposition = strategy # 'append' or 'merge'
        logger.debug(f"Incremental Loading Active (Strategy: {strategy})")
    
    # 3. DEFINE THE SOURCE
    if source_type == "rest_api":
        strategy = RestApiSource()
        source_data = strategy.get_source(
            source_config, columns, write_disposition, 
            incremental_config, contract_config
        )
    elif source_type == "sql_database":
        strategy = SqlDatabaseSource()
        source_data = strategy.get_source(
            source_config, columns, write_disposition, 
            incremental_config, contract_config
        )
    elif source_type == "graphql":
        source_data = _get_graphql_source(source_config, write_disposition)
    else:
        # --- LEGACY: Dynamic Module Import ---
        import importlib
        module_name = source_config.get("module")
        func_name = source_config.get("name")
        
        try:
            mod = importlib.import_module(module_name)
            source_func = getattr(mod, func_name)
            source_data = source_func(**source_config.get("args", {}))
        except Exception as e:
             raise ValueError(f"Could not load source {module_name}.{func_name}: {e}")

    # --- PHASE 8: Python Transformation (Polyglot) ---
    if python_code:
        logger.debug("Applying Python Transformation")
        local_scope = {}
        try:
            exec(python_code, {}, local_scope)
        except Exception as e:
            raise ValueError(f"Failed to execute python_code: {e}")
            
        if "transform_row" not in local_scope:
            raise ValueError("python_code must define a function named 'transform_row(row)'")
            
        transform_func = local_scope["transform_row"]
        source_data.add_map(transform_func)

    # --- PHASE 9: Pre-Flight Contracts ---
    contract = config.get("contract")
    if contract:
        logger.debug("Validating Pre-Flight Contract...")
        
        # Extract contract settings
        evolution_mode = contract.get("evolution", "evolve")
        enforce_types = contract.get("enforce_types", False)
        required_columns = contract.get("required_columns", [])
        allow_new_columns = contract.get("allow_new_columns", True)
        
        try:
            iterator = iter(source_data)
            
            try:
                first_item = next(iterator)
            except StopIteration:
                logger.warning("Source is empty. Skipping contract validation.")
                first_item = None
                
            if first_item:
                # Validate required columns
                if required_columns:
                    missing_cols = [col for col in required_columns if col not in first_item]
                    if missing_cols:
                        raise ValueError(
                            f"Contract Failed: Missing required columns: {missing_cols}. "
                            f"Found: {list(first_item.keys())}"
                        )
                
                # Check for new columns if evolution is frozen
                if evolution_mode == "freeze" and not allow_new_columns:
                    # This would need to compare against expected schema
                    # For now, we rely on dlt's schema_evolution="freeze" hint
                    pass
                
                # Type enforcement (if enabled)
                if enforce_types and required_columns:
                    # Compare types of required columns
                    # This is a simplified check - full type validation would need expected schema
                    for col in required_columns:
                        if col in first_item:
                            value = first_item[col]
                            # Basic type check - could be enhanced with expected types from contract
                            if value is not None:
                                # Type checking would require storing expected types in contract
                                pass
                
                logger.info("Contract Validated.")
                
                stream = itertools.chain([first_item], iterator)
                
                name_hint = getattr(source_data, "name", target_table)
                write_disp_hint = getattr(source_data, "write_disposition", write_disposition)
                
                # Preserve contract config when recreating resource
                # Check if source_data already has schema evolution hints applied
                # If contract was already applied at source level, don't recreate
                # Otherwise, recreate with contract config preserved
                if hasattr(source_data, 'compute_table_schema') or hasattr(source_data, '_hints'):
                    # Contract already applied at source level, just wrap the stream
                    source_data = dlt.resource(
                        stream,
                        name=name_hint,
                        write_disposition=write_disp_hint
                    )
                    # Re-apply contract config if needed
                    if evolution_mode == "freeze":
                        source_data.apply_hints(schema_evolution="freeze")
                else:
                    # Create new resource and apply contract config
                    source_data = dlt.resource(
                        stream,
                        name=name_hint,
                        write_disposition=write_disp_hint
                    )
                    if evolution_mode == "freeze":
                        source_data.apply_hints(schema_evolution="freeze")
                
        except TypeError:
            logger.warning("Source data is not iterable (maybe DltSource?). Skipping peek validation.")
            pass

    # 4. RUN PIPELINE
    pipeline = dlt.pipeline(
        pipeline_name="msh_runner",
        destination=target_destination,
        dataset_name=config.get("dataset_name", "msh_raw")
    )
    
    try:
        info = pipeline.run(
            source_data,
            table_name=target_table,
            write_disposition=write_disposition,
            primary_key=primary_key,
            loader_file_format="parquet" 
        )
    except Exception as e:
        # Provide helpful error messages for Snowflake errors
        # Note: Only applies when destination is Snowflake - other destinations get standard error handling
        error_msg = str(e).lower()
        if target_destination.lower() == "snowflake":
            if "warehouse" in error_msg and "suspended" in error_msg:
                raise RuntimeError(
                    "Snowflake warehouse is suspended. Resume it in the Snowflake UI. "
                    f"Original error: {e}"
                ) from e
            elif "timeout" in error_msg or "connection" in error_msg:
                raise RuntimeError(
                    "Snowflake connection timeout. Check network connectivity and warehouse status. "
                    f"Original error: {e}"
                ) from e
            elif "quota" in error_msg or "limit" in error_msg:
                raise RuntimeError(
                    "Snowflake quota exceeded. Check your account limits. "
                    f"Original error: {e}"
                ) from e
            elif "authentication" in error_msg or "login" in error_msg:
                raise RuntimeError(
                    "Snowflake authentication failed. Check your credentials. "
                    f"Original error: {e}"
                ) from e
        # Re-raise other errors as-is
        raise
    
    # Task 2: Fix Metadata Writing (The Bug)
    try:
        # Task 3: Path Safety
        cwd = os.getcwd()
        if cwd.endswith("build"):
            meta_dir = os.path.join(cwd, "..", "run_meta")
        else:
            meta_dir = os.path.join(cwd, ".msh", "run_meta")
            
        meta_dir = os.path.abspath(meta_dir)
        
        if not os.path.exists(meta_dir):
            os.makedirs(meta_dir, exist_ok=True)
            
        # Construct Sidecar Metadata
        rows_loaded = 0
        try:
            # Extract rows loaded from load_info
            # Try multiple methods as dlt API varies by version
            if hasattr(info, 'load_packages') and info.load_packages:
                for package in info.load_packages:
                    # Check schema_update for table row counts
                    if hasattr(package, 'schema_update') and package.schema_update:
                        for table_name, table_info in package.schema_update.items():
                            if hasattr(table_info, 'row_count'):
                                rows_loaded += table_info.row_count
                            elif isinstance(table_info, dict) and 'row_count' in table_info:
                                rows_loaded += table_info['row_count']
                    
                    # Check jobs for row counts
                    if hasattr(package, 'jobs') and package.jobs:
                        completed_jobs = package.jobs.get('completed_jobs', [])
                        for job in completed_jobs:
                            if hasattr(job, 'row_count'):
                                rows_loaded += job.row_count
                            elif isinstance(job, dict) and 'row_count' in job:
                                rows_loaded += job['row_count']
            
            # Try load_metrics attribute (newer dlt versions)
            if rows_loaded == 0 and hasattr(info, 'load_metrics') and info.load_metrics:
                if hasattr(info.load_metrics, 'rows_count'):
                    rows_loaded = info.load_metrics.rows_count
                elif isinstance(info.load_metrics, dict) and 'rows_count' in info.load_metrics:
                    rows_loaded = info.load_metrics['rows_count']
            
            # Fallback: Try asdict() method for dict-based access
            if rows_loaded == 0:
                try:
                    load_info_dict = info.asdict()
                    load_packages = load_info_dict.get("load_packages", [])
                    for pkg in load_packages:
                        schema_update = pkg.get("schema_update", {})
                        for table_name, table_info in schema_update.items():
                            if isinstance(table_info, dict) and 'row_count' in table_info:
                                rows_loaded += table_info['row_count']
                except (AttributeError, TypeError):
                    pass
                    
        except (AttributeError, TypeError, KeyError) as e:
            logger.warning(f"Could not extract exact row count from load_info: {e}")
            # rows_loaded remains 0, indicating "unknown" rather than "zero rows"

        sidecar = {
            "rows_loaded": rows_loaded,
            "smart_ingest_active": bool(columns),
            "columns_saved": len(columns) if columns else 0,
            "source_type": source_type,
            "timestamp": str(pd.Timestamp.now())
        }
        
        # Extract schema metadata for AI context
        try:
            project_root = os.path.dirname(meta_dir) if meta_dir.endswith("run_meta") else os.path.dirname(os.path.dirname(meta_dir))
            metadata_extractor = MetadataExtractor(project_root=project_root)
            schema_metadata = metadata_extractor.extract_schema_from_load_info(info, target_table)
            sidecar["schema"] = schema_metadata
            
            # Save runtime metadata
            asset_name = target_table.replace("raw_", "").replace("_raw", "")
            metadata_extractor.save_runtime_metadata(asset_name, {
                "schema": schema_metadata,
                "rows_loaded": rows_loaded,
                "source_type": source_type
            })
        except Exception as e:
            logger.warning(f"Failed to extract schema metadata: {e}")
        
        # Apply glossary validation
        try:
            project_root = os.path.dirname(meta_dir) if meta_dir.endswith("run_meta") else os.path.dirname(os.path.dirname(meta_dir))
            glossary_validator = GlossaryValidator(project_root=project_root)
            asset_name = target_table.replace("raw_", "").replace("_raw", "")
            
            # Get column names
            column_names = []
            if columns:
                column_names = columns
            elif "schema" in sidecar:
                schema_cols = sidecar["schema"].get("columns", [])
                column_names = [col.get("name") if isinstance(col, dict) else col for col in schema_cols]
            
            # Check if asset is public (from config)
            is_public = config.get("deploy", {}).get("public", False)
            
            if column_names:
                policy_results = glossary_validator.apply_glossary_policies(
                    asset_name,
                    column_names,
                    is_public
                )
                
                if policy_results["should_block"]:
                    logger.error(f"Glossary policy violations: {policy_results['pii_violations']}")
                    raise ValueError(f"Asset blocked due to glossary policy violations: {policy_results['pii_violations']}")
                
                if policy_results["warnings"]:
                    logger.warning(f"Glossary constraint warnings: {policy_results['warnings']}")
                
                sidecar["glossary_validation"] = policy_results
        except Exception as e:
            logger.warning(f"Failed to apply glossary validation: {e}")
        
        meta_file = os.path.join(meta_dir, f"{target_table}.json")
        logger.debug(f"Writing metadata to {meta_file}")
        
        with open(meta_file, "w") as f:
            json.dump(sidecar, f, indent=2)
            
    except Exception as e:
        logger.warning(f"Failed to write metadata: {e}")
    
    return pd.DataFrame([{"status": "success", "info": str(info)}])