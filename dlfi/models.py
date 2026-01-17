from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, IO

@dataclass
class DiscoveredNode:
    """Represents a Folder (Vault) or Item (Record) found by the extractor."""
    suggested_path: str      # Virtual path: "twitter/user_xyz/tweet_123"
    node_type: str           # "VAULT" or "RECORD"
    
    metadata: Dict = field(default_factory=dict)
    
    files: List['DiscoveredFile'] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    
    # List of (Relation_Name, Target_Path)
    # e.g. [("AUTHORED_BY", "twitter/user_xyz")]
    relationships: List[Tuple[str, str]] = field(default_factory=list)

@dataclass
class DiscoveredFile:
    """
    Represents a file to be ingested into the archive.
    
    You must provide either:
    1. `local_path`: A path to a file already on disk (e.g. from a temp download).
    2. `stream`: A file-like object (e.g. response.raw) to be read during ingestion.
    """
    original_name: str       # e.g., "image.jpg"
    source_url: str          # Where it came from (provenance)
    
    stream: Optional[IO[bytes]] = None