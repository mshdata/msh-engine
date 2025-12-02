from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

class SourceStrategy(ABC):
    @abstractmethod
    def get_source(
        self, 
        source_config: Dict[str, Any], 
        columns: Optional[List[str]], 
        write_disposition: str, 
        incremental_config: Optional[Dict[str, Any]] = None,
        contract_config: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Returns a dlt source or resource.
        """
        pass
