"""
Metadata extraction for msh-engine.

Extracts schema information, deployment state, and runtime metadata
for AI context generation.
"""
import os
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from .logger import logger


class MetadataExtractor:
    """Extracts metadata from engine runtime."""
    
    def __init__(self, project_root: Optional[str] = None):
        """
        Initialize metadata extractor.
        
        Args:
            project_root: Project root directory for metadata cache
        """
        if project_root is None:
            project_root = os.getcwd()
        
        self.project_root = project_root
        self.meta_dir = os.path.join(project_root, ".msh", "run_meta")
        os.makedirs(self.meta_dir, exist_ok=True)
    
    def extract_schema_from_load_info(
        self,
        load_info: Any,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Extract schema information from dlt LoadInfo.
        
        Args:
            load_info: dlt LoadInfo object
            table_name: Table name
            
        Returns:
            Schema dictionary with columns and types
        """
        schema = {
            "columns": [],
            "table_name": table_name,
            "extracted_at": datetime.now().isoformat()
        }
        
        try:
            # Try to extract schema from load_info
            if hasattr(load_info, "load_packages"):
                for package in load_info.load_packages:
                    if hasattr(package, "schema_update"):
                        schema_update = package.schema_update
                        if isinstance(schema_update, dict):
                            table_schema = schema_update.get(table_name, {})
                            if isinstance(table_schema, dict):
                                columns = table_schema.get("columns", {})
                                for col_name, col_info in columns.items():
                                    if isinstance(col_info, dict):
                                        schema["columns"].append({
                                            "name": col_name,
                                            "type": col_info.get("data_type", "unknown"),
                                            "nullable": col_info.get("nullable", True),
                                        })
            
            # Fallback: Try to get schema from destination
            # This would require querying information_schema
            # For now, we return what we can extract
            
        except Exception as e:
            logger.warning(f"Failed to extract schema from load_info: {e}")
        
        return schema
    
    def save_runtime_metadata(
        self,
        asset_name: str,
        metadata: Dict[str, Any]
    ) -> None:
        """
        Save runtime metadata for an asset.
        
        Args:
            asset_name: Asset name
            metadata: Metadata dictionary to save
        """
        try:
            meta_file = os.path.join(self.meta_dir, f"{asset_name}_runtime.json")
            
            # Load existing metadata if exists
            existing_metadata = {}
            if os.path.exists(meta_file):
                try:
                    with open(meta_file, "r") as f:
                        existing_metadata = json.load(f)
                except (json.JSONDecodeError, IOError):
                    pass
            
            # Merge with new metadata
            existing_metadata.update(metadata)
            existing_metadata["last_updated"] = datetime.now().isoformat()
            
            # Save
            with open(meta_file, "w") as f:
                json.dump(existing_metadata, f, indent=2)
            
            logger.debug(f"Saved runtime metadata for {asset_name}")
        
        except Exception as e:
            logger.warning(f"Failed to save runtime metadata: {e}")
    
    def get_deployment_metadata(
        self,
        asset_name: str,
        deployment_hash: str,
        blue_schema: Optional[str] = None,
        green_schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate deployment metadata for versions.json.
        
        Args:
            asset_name: Asset name
            deployment_hash: Deployment hash
            blue_schema: Blue schema name
            green_schema: Green schema name
            
        Returns:
            Deployment metadata dictionary
        """
        return {
            "asset_id": asset_name,
            "hash": deployment_hash,
            "deployed_at": datetime.now().isoformat(),
            "blue_schema": blue_schema,
            "green_schema": green_schema,
            "status": "deployed"
        }
    
    def update_versions_cache(
        self,
        asset_name: str,
        deployment_metadata: Dict[str, Any]
    ) -> None:
        """
        Update versions.json cache with deployment information.
        
        Args:
            asset_name: Asset name
            deployment_metadata: Deployment metadata dictionary
        """
        try:
            versions_file = os.path.join(self.project_root, ".msh", "versions.json")
            
            # Load existing versions
            versions_data = {}
            if os.path.exists(versions_file):
                try:
                    with open(versions_file, "r") as f:
                        versions_data = json.load(f)
                except (json.JSONDecodeError, IOError):
                    versions_data = {"versions": {}}
            else:
                versions_data = {"versions": {}}
            
            # Update versions for this asset
            if "versions" not in versions_data:
                versions_data["versions"] = {}
            
            if asset_name not in versions_data["versions"]:
                versions_data["versions"][asset_name] = []
            
            # Add new deployment
            versions_data["versions"][asset_name].append(deployment_metadata)
            
            # Keep only last 10 deployments per asset
            versions_data["versions"][asset_name] = versions_data["versions"][asset_name][-10:]
            
            # Update metadata
            versions_data["generated_at"] = datetime.now().isoformat()
            
            # Save
            os.makedirs(os.path.dirname(versions_file), exist_ok=True)
            with open(versions_file, "w") as f:
                json.dump(versions_data, f, indent=2)
            
            logger.debug(f"Updated versions cache for {asset_name}")
        
        except Exception as e:
            logger.warning(f"Failed to update versions cache: {e}")

