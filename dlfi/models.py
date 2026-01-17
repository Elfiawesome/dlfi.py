from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional

@dataclass
class DiscoveredFile:
    """Represents a physical file downloaded to a temp location."""
    local_path: str          # Path to the temp file on disk
    original_name: str       # e.g., "image.jpg"
    source_url: str          # Where it came from (provenance)
    delete_after_import: bool = True # Should Ingestor delete local_path after DB insert?

@dataclass
class DiscoveredNode:
    """Represents a Folder (Vault) or Item (Record) found by the extractor."""
    suggested_path: str      # Virtual path: "twitter/user_xyz/tweet_123"
    node_type: str           # "VAULT" or "RECORD"
    
    metadata: Dict = field(default_factory=dict)
    
    files: List[DiscoveredFile] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    
    # List of (Relation_Name, Target_Path)
    # e.g. [("AUTHORED_BY", "twitter/user_xyz")]
    relationships: List[Tuple[str, str]] = field(default_factory=list)