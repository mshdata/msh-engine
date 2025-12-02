import dlt
import requests
from typing import Any, Dict, List, Optional
from .base import SourceStrategy

class RestApiSource(SourceStrategy):
    def get_source(
        self, 
        source_config: Dict[str, Any], 
        columns: Optional[List[str]], 
        write_disposition: str, 
        incremental_config: Optional[Dict[str, Any]] = None,
        contract_config: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Native REST Source using dlt.sources.rest_api for automatic pagination.
        """
        try:
            from dlt.sources.rest_api import rest_api_source
        except ImportError:
            raise ImportError("dlt[rest_api] is not installed. Please install it.")
        
        from ..logger import logger

        # Extract config
        rest_config = source_config.get("config")
        
        if rest_config:
            # Declarative mode
            source = rest_api_source(rest_config)
            
            target_resource = source_config.get("resource")
            if target_resource:
                if target_resource in source.resources:
                    source_data = source.resources[target_resource]
                    source_data.write_disposition = write_disposition
                    
                    # Apply contract config (schema evolution control)
                    if contract_config:
                        evolution_mode = contract_config.get("evolution", "evolve")
                        if evolution_mode == "freeze":
                            source_data.apply_hints(schema_evolution="freeze")
                            logger.debug("Schema evolution frozen (contract: evolution=freeze)")
                    
                    return source_data
                     
            # Apply write_disposition to all resources?
            for r_name in source.resources:
                source.resources[r_name].write_disposition = write_disposition
                
                # Apply contract config to all resources
                if contract_config:
                    evolution_mode = contract_config.get("evolution", "evolve")
                    if evolution_mode == "freeze":
                        source.resources[r_name].apply_hints(schema_evolution="freeze")
                        logger.debug(f"Schema evolution frozen for resource {r_name} (contract: evolution=freeze)")
                
            return source

        else:
            # Simple mode: endpoint + resource name
            endpoint = source_config.get("endpoint")
            resource_name = source_config.get("resource", "data")
            
            if not endpoint:
                raise ValueError("REST API source requires 'config' or 'endpoint'")
                
            from urllib.parse import urlparse
            parsed = urlparse(endpoint)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            path = parsed.path
            if parsed.query:
                path += f"?{parsed.query}"
                
            simple_config = {
                "client": {
                    "base_url": base_url
                },
                "resources": [
                    {
                        "name": resource_name,
                        "endpoint": {
                            "path": path,
                        }
                    }
                ]
            }
            
            logger.debug(f"Generated REST Config: {simple_config}")
            
            source = rest_api_source(simple_config)
            source_data = source.resources[resource_name]
            source_data.write_disposition = write_disposition
            
            # Apply contract config (schema evolution control)
            if contract_config:
                evolution_mode = contract_config.get("evolution", "evolve")
                if evolution_mode == "freeze":
                    # Disable schema evolution in dlt
                    source_data.apply_hints(schema_evolution="freeze")
                    logger.debug("Schema evolution frozen (contract: evolution=freeze)")
            
            # 4. Smart Ingest (Memory Filtering)
            if columns:
                logger.debug(f"Smart Ingest (Memory Filtering) - Keeping: {columns}")
                def filter_row(row):
                    return {k: v for k, v in row.items() if k in columns}
                source_data.add_map(filter_row)
            
            return source_data
