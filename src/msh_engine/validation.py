"""
Glossary-based validation for msh-engine.

Applies glossary policies during ingestion and validates against glossary constraints.
"""
import os
import json
import yaml
from typing import Dict, Any, List, Optional
from .logger import logger


class GlossaryValidator:
    """Validates assets against glossary policies."""
    
    def __init__(self, project_root: Optional[str] = None):
        """
        Initialize glossary validator.
        
        Args:
            project_root: Project root directory for glossary files
        """
        if project_root is None:
            project_root = os.getcwd()
        
        self.project_root = project_root
        self.glossary_data = self._load_glossary()
    
    def _load_glossary(self) -> Dict[str, Any]:
        """Load glossary from cache or YAML."""
        # Try JSON cache first
        glossary_json = os.path.join(self.project_root, ".msh", "glossary.json")
        if os.path.exists(glossary_json):
            try:
                with open(glossary_json, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load glossary cache: {e}")
        
        # Try YAML
        glossary_yaml = os.path.join(self.project_root, "glossary.yaml")
        if os.path.exists(glossary_yaml):
            try:
                with open(glossary_yaml, "r") as f:
                    return yaml.safe_load(f) or {}
            except (yaml.YAMLError, IOError) as e:
                logger.warning(f"Failed to load glossary YAML: {e}")
        
        # Try msh.yaml glossary section
        msh_yaml = os.path.join(self.project_root, "msh.yaml")
        if os.path.exists(msh_yaml):
            try:
                with open(msh_yaml, "r") as f:
                    msh_config = yaml.safe_load(f) or {}
                    return msh_config.get("glossary", {})
            except (yaml.YAMLError, IOError) as e:
                logger.warning(f"Failed to load glossary from msh.yaml: {e}")
        
        return {}
    
    def validate_pii_policy(
        self,
        asset_id: str,
        columns: List[str],
        is_public: bool = False
    ) -> List[str]:
        """
        Validate asset against PII policies.
        
        Args:
            asset_id: Asset ID
            columns: List of column names
            is_public: Whether asset is marked as public
            
        Returns:
            List of policy violations
        """
        violations = []
        
        if not is_public:
            return violations
        
        policies = self.glossary_data.get("policies", [])
        
        for policy in policies:
            rules = policy.get("rules", [])
            applies_to = policy.get("applies_to", [])
            
            # Check if policy applies to this asset
            applies = False
            for app in applies_to:
                if app.get("asset") == asset_id:
                    applies = True
                    break
            
            if not applies:
                continue
            
            # Check rules
            for rule in rules:
                if "llm_context:" in rule:
                    # This is an LLM context rule
                    if "no_pii" in rule.lower() or "pii" in rule.lower():
                        # Check for PII columns
                        pii_keywords = ["email", "phone", "ssn", "credit_card", "password", "secret", "token"]
                        pii_columns = [col for col in columns if any(keyword in col.lower() for keyword in pii_keywords)]
                        
                        if pii_columns:
                            violations.append(
                                f"Policy '{policy.get('name')}' prohibits PII columns in public assets. "
                                f"Found: {', '.join(pii_columns)}"
                            )
        
        return violations
    
    def validate_glossary_constraints(
        self,
        asset_id: str,
        columns: List[str]
    ) -> List[str]:
        """
        Validate asset columns against glossary constraints.
        
        Args:
            asset_id: Asset ID
            columns: List of column names
            
        Returns:
            List of constraint violations
        """
        violations = []
        
        terms = self.glossary_data.get("terms", [])
        
        # Find terms linked to this asset
        linked_terms = []
        for term in terms:
            linked_assets = term.get("linked_assets", [])
            if asset_id in linked_assets:
                linked_terms.append(term)
        
        # Check if required columns are present
        for term in linked_terms:
            linked_columns = term.get("linked_columns", [])
            required_columns = [
                lc.get("column") for lc in linked_columns
                if lc.get("asset") == asset_id and lc.get("role") == "required"
            ]
            
            missing_columns = [col for col in required_columns if col not in columns]
            if missing_columns:
                violations.append(
                    f"Term '{term.get('name')}' requires columns: {', '.join(missing_columns)}"
                )
        
        return violations
    
    def apply_glossary_policies(
        self,
        asset_id: str,
        columns: List[str],
        is_public: bool = False
    ) -> Dict[str, Any]:
        """
        Apply glossary policies to asset ingestion.
        
        Args:
            asset_id: Asset ID
            columns: List of column names
            is_public: Whether asset is marked as public
            
        Returns:
            Dictionary with policy application results
        """
        pii_policy_violations = self.validate_pii_policy(asset_id, columns, is_public)
        constraint_violations = self.validate_glossary_constraints(asset_id, columns)
        
        return {
            "pii_violations": pii_policy_violations,
            "constraint_violations": constraint_violations,
            "should_block": len(pii_policy_violations) > 0,
            "warnings": constraint_violations
        }
    
    def mask_pii_columns(
        self,
        columns: List[str],
        asset_id: str
    ) -> List[str]:
        """
        Mask or filter PII columns based on glossary policies.
        
        Args:
            columns: List of column names
            asset_id: Asset ID
            
        Returns:
            Filtered list of columns (PII columns removed)
        """
        policies = self.glossary_data.get("policies", [])
        pii_columns = set()
        
        for policy in policies:
            applies_to = policy.get("applies_to", [])
            for app in applies_to:
                if app.get("asset") == asset_id:
                    column_name = app.get("column")
                    if column_name:
                        pii_columns.add(column_name)
        
        # Also check for common PII patterns
        pii_keywords = ["email", "phone", "ssn", "credit_card", "password", "secret", "token"]
        for col in columns:
            if any(keyword in col.lower() for keyword in pii_keywords):
                pii_columns.add(col)
        
        return [col for col in columns if col not in pii_columns]

