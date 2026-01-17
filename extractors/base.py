from abc import ABC, abstractmethod
from dlfi import DLFI
from typing import Dict, Any

class BaseExtractor(ABC):
    """
    The blueprint that all Extractors must follow.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the extractor (e.g., 'Twitter Extractor')."""
        pass

    @abstractmethod
    def can_handle(self, url: str) -> bool:
        """
        Returns True if this extractor can handle the given URL.
        Example: return 'twitter.com' in url
        """
        pass

    @abstractmethod
    def extract(self, url: str, db: DLFI) -> Dict[str, Any]:
        """
        Performs the logic:
        1. Download content (to temp).
        2. Create Vaults/Records in DB.
        3. Append Files.
        4. Return a summary dict (e.g., {'status': 'success', 'records_created': 5}).
        """
        pass