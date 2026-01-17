import os
import sqlite3
import json
import hashlib
import uuid
import shutil
import time
from pathlib import Path
from typing import Optional, Dict, List, Any

class DLFI:
    def __init__(self, archive_root: str):
        """
        Initialize the Archive System.
        :param archive_root: Path to the root directory of your archive.
        """
        self.root = Path(archive_root).resolve()
        self.system_dir = self.root / ".dlfi"
        self.storage_dir = self.system_dir / "storage"
        self.db_path = self.system_dir / "db.sqlite"

        # Initialize directories
        self._initialize_structure()
        
        # Connect to DB
        self.conn = self._get_connection()
        self._initialize_schema()

    def _initialize_structure(self):
        """Creates the .dlfi hidden folders if they don't exist."""
        if not self.storage_dir.exists():
            os.makedirs(self.storage_dir, exist_ok=True)
            print(f"[DLFI] Initialized storage at {self.storage_dir}")

    def _get_connection(self) -> sqlite3.Connection:
        """Returns a tuned SQLite connection."""
        conn = sqlite3.connect(self.db_path)
        # OPTIMIZATION: Enable Write-Ahead Logging for concurrency and speed
        conn.execute("PRAGMA journal_mode=WAL;")
        # OPTIMIZATION: Faster writes, safe enough for local single-user
        conn.execute("PRAGMA synchronous=NORMAL;")
        # Enable Foreign Keys
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _initialize_schema(self):
        """Creates the Database Tables with Indices for performance."""
        with self.conn:
            # 1. NODES (Vaults and Records)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    uuid TEXT PRIMARY KEY,
                    parent_uuid TEXT,
                    type TEXT CHECK(type IN ('VAULT', 'RECORD')) NOT NULL,
                    name TEXT NOT NULL,
                    cached_path TEXT UNIQUE, 
                    metadata JSON,
                    created_at REAL,
                    last_modified REAL,
                    FOREIGN KEY(parent_uuid) REFERENCES nodes(uuid) ON DELETE CASCADE
                );
            """)
            # Index for fast path lookups and child traversals
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_uuid);")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_path ON nodes(cached_path);")

            # 2. BLOBS (Physical Files - Deduplicated)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS blobs (
                    hash TEXT PRIMARY KEY,
                    ext TEXT,
                    size_bytes INTEGER,
                    storage_path TEXT
                );
            """)

            # 3. NODE_FILES (Linking Records to Blobs)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS node_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_uuid TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    original_name TEXT,
                    display_order INTEGER,
                    added_at REAL,
                    FOREIGN KEY(node_uuid) REFERENCES nodes(uuid) ON DELETE CASCADE,
                    FOREIGN KEY(file_hash) REFERENCES blobs(hash)
                );
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_node_files_node ON node_files(node_uuid);")

            # 4. EDGES (Relationships / Graph)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS edges (
                    source_uuid TEXT,
                    target_uuid TEXT,
                    relation TEXT,
                    created_at REAL,
                    PRIMARY KEY (source_uuid, target_uuid, relation),
                    FOREIGN KEY(source_uuid) REFERENCES nodes(uuid) ON DELETE CASCADE,
                    FOREIGN KEY(target_uuid) REFERENCES nodes(uuid) ON DELETE CASCADE
                );
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_uuid);")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_uuid);")

            # 5. TAGS (Primitive Tagging)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    node_uuid TEXT,
                    tag TEXT,
                    PRIMARY KEY (node_uuid, tag),
                    FOREIGN KEY(node_uuid) REFERENCES nodes(uuid) ON DELETE CASCADE
                );
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag);")

    def close(self):
        self.conn.close()

    # --- Helper: Hashing for Efficiency ---
    @staticmethod
    def get_file_hash(filepath: str) -> str:
        """Stream the file to calculate SHA256 without loading into RAM."""
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(65536) # 64kb chunks
                if not data:
                    break
                sha256.update(data)
        return sha256.hexdigest()

# --- WRITE OPERATIONS: Vaults & Records ---

    def create_vault(self, path: str, metadata: dict = None) -> str:
        """
        Ensures a Vault (folder) exists at the specific path.
        Creates parents recursively if needed.
        Returns the UUID of the vault.
        """
        return self._resolve_path(path, create_if_missing=True, node_type='VAULT', metadata=metadata)

    def create_record(self, path: str, metadata: dict = None) -> str:
        """
        Creates a Record at the specific path.
        Returns the UUID of the record.
        """
        return self._resolve_path(path, create_if_missing=True, node_type='RECORD', metadata=metadata)

    def append_file(self, record_path: str, file_path: str):
        """
        1. Hashes the external file.
        2. Copies it to .dlfi/storage (if not already there).
        3. Links it to the record.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # 1. Get Node UUID
        node_uuid = self._resolve_path(record_path, create_if_missing=False)
        if not node_uuid:
            raise ValueError(f"Record not found: {record_path}")

        # 2. Calculate Hash (Efficient Streaming)
        file_hash = self.get_file_hash(str(file_path))
        file_size = file_path.stat().st_size
        ext = file_path.suffix.lower().lstrip('.')

        with self.conn:
            # 3. Check if Blob exists in DB, if not, store it
            cursor = self.conn.execute("SELECT hash FROM blobs WHERE hash = ?", (file_hash,))
            if not cursor.fetchone():
                # Sharding Strategy: first 2 chars / next 2 chars
                shard_a = file_hash[:2]
                shard_b = file_hash[2:4]
                storage_subdir = self.storage_dir / shard_a / shard_b
                os.makedirs(storage_subdir, exist_ok=True)
                
                target_path = storage_subdir / file_hash
                
                # Copy physical file
                shutil.copy2(file_path, target_path)

                # Insert Blob Record
                rel_path = f"{shard_a}/{shard_b}/{file_hash}"
                self.conn.execute("""
                    INSERT INTO blobs (hash, ext, size_bytes, storage_path)
                    VALUES (?, ?, ?, ?)
                """, (file_hash, ext, file_size, rel_path))

            # 4. Link Blob to Node (Record)
            # Check existing order count to append at end
            cur = self.conn.execute("SELECT COUNT(*) FROM node_files WHERE node_uuid = ?", (node_uuid,))
            count = cur.fetchone()[0]
            
            self.conn.execute("""
                INSERT INTO node_files (node_uuid, file_hash, original_name, display_order, added_at)
                VALUES (?, ?, ?, ?, ?)
            """, (node_uuid, file_hash, file_path.name, count + 1, time.time()))
            
            # Update last_modified on the record
            self.conn.execute("UPDATE nodes SET last_modified = ? WHERE uuid = ?", (time.time(), node_uuid))

    # --- INTERNAL: Path Resolution Logic ---

    def _resolve_path(self, path: str, create_if_missing=False, node_type='VAULT', metadata=None) -> Optional[str]:
        """
        Converts a path string (e.g., "manga/jojo") into a UUID.
        If create_if_missing is True, it builds the hierarchy.
        """
        clean_path = path.strip("/").replace("\\", "/") # Normalize
        parts = clean_path.split("/")
        
        current_parent_uuid = None # Root
        current_path_str = ""

        for i, part in enumerate(parts):
            is_last = (i == len(parts) - 1)
            if i > 0:
                current_path_str += "/"
            current_path_str += part

            # Check if this node exists
            cursor = self.conn.execute(
                "SELECT uuid FROM nodes WHERE parent_uuid IS ? AND name = ?", 
                (current_parent_uuid, part)
            )
            row = cursor.fetchone()

            if row:
                current_parent_uuid = row[0] # Move down the tree
            else:
                if not create_if_missing:
                    return None # Path doesn't exist
                
                # Create it
                new_uuid = str(uuid.uuid4())
                # Determine type: Intermediate parts are always VAULTs. Last part uses requested type.
                actual_type = node_type if is_last else 'VAULT'
                # Only apply metadata to the exact target, not the parents
                actual_meta = json.dumps(metadata) if (is_last and metadata) else None
                
                with self.conn:
                    self.conn.execute("""
                        INSERT INTO nodes (uuid, parent_uuid, type, name, cached_path, metadata, created_at, last_modified)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (new_uuid, current_parent_uuid, actual_type, part, current_path_str, actual_meta, time.time(), time.time()))
                
                current_parent_uuid = new_uuid

        return current_parent_uuid
    
	# --- GRAPH OPERATIONS: Linking & Tagging ---

    def link(self, source_path: str, target_path: str, relation: str):
        """
        Creates a directed relationship between two nodes.
        Example: link('manga/jojo', 'people/araki', 'AUTHORED_BY')
        """
        src_uuid = self._resolve_path(source_path)
        tgt_uuid = self._resolve_path(target_path)

        if not src_uuid: raise ValueError(f"Source path not found: {source_path}")
        if not tgt_uuid: raise ValueError(f"Target path not found: {target_path}")

        with self.conn:
            self.conn.execute("""
                INSERT OR REPLACE INTO edges (source_uuid, target_uuid, relation, created_at)
                VALUES (?, ?, ?, ?)
            """, (src_uuid, tgt_uuid, relation.upper(), time.time()))

    def add_tag(self, path: str, tag: str):
        """Adds a primitive string tag to a node."""
        node_uuid = self._resolve_path(path)
        if not node_uuid: raise ValueError(f"Node not found: {path}")

        with self.conn:
            self.conn.execute("""
                INSERT OR IGNORE INTO tags (node_uuid, tag)
                VALUES (?, ?)
            """, (node_uuid, tag.lower()))

    # --- EXPORT SYSTEM: Static Generation ---

    def export(self, output_dir: str):
        """
        Generates a static file system version of the archive.
        Returns: The path to the index.json
        """
        out_path = Path(output_dir).resolve()
        if out_path.exists():
            print(f"[Export] Cleaning previous export at {out_path}...")
            shutil.rmtree(out_path)
        os.makedirs(out_path)

        print("[Export] Building UUID lookup map...")
        # 1. Build a memory map of UUID -> Path for fast relationship resolution
        uuid_to_path = {}
        cursor = self.conn.execute("SELECT uuid, cached_path FROM nodes")
        for row in cursor:
            uuid_to_path[row[0]] = row[1]

        print("[Export] Generating hierarchy and files...")
        # 2. Iterate all nodes and build structure
        # We fetch everything needed for the meta.json in one go per node would be ideal, 
        # but for simplicity/readability we will query per node.
        nodes_cursor = self.conn.execute("SELECT uuid, type, cached_path, metadata FROM nodes")
        
        for n_uuid, n_type, n_path, n_meta in nodes_cursor:
            # Determine physical path
            # If it's a RECORD, we make it a FOLDER so it can hold the file + meta.json
            node_out_dir = out_path / n_path
            os.makedirs(node_out_dir, exist_ok=True)

            # A. Prepare Metadata
            meta_dict = json.loads(n_meta) if n_meta else {}
            meta_dict['uuid'] = n_uuid
            meta_dict['type'] = n_type
            
            # B. Fetch Tags
            tags_cur = self.conn.execute("SELECT tag FROM tags WHERE node_uuid = ?", (n_uuid,))
            meta_dict['tags'] = [r[0] for r in tags_cur]

            # C. Fetch Relationships (Outgoing)
            # We resolve UUIDs back to Paths here so the JSON is human-readable
            rels = []
            edges_cur = self.conn.execute("SELECT target_uuid, relation FROM edges WHERE source_uuid = ?", (n_uuid,))
            for tgt_uuid, rel_name in edges_cur:
                tgt_path = uuid_to_path.get(tgt_uuid, "UNKNOWN_NODE")
                rels.append({"relation": rel_name, "target_path": tgt_path})
            meta_dict['relationships'] = rels

            # D. Handle Files (Only for Records usually, but Vaults can technically have attachments in this logic)
            files_list = []
            files_cur = self.conn.execute("""
                SELECT nf.original_name, b.storage_path 
                FROM node_files nf 
                JOIN blobs b ON nf.file_hash = b.hash 
                WHERE nf.node_uuid = ? 
                ORDER BY nf.display_order
            """, (n_uuid,))
            
            for orig_name, blob_rel_path in files_cur:
                # Copy physical file
                src_blob = self.storage_dir / blob_rel_path
                dst_file = node_out_dir / orig_name
                
                # Copy if exists (it should)
                if src_blob.exists():
                    shutil.copy2(src_blob, dst_file)
                    files_list.append(orig_name)
                else:
                    print(f"[Warning] Blob missing: {blob_rel_path}")

            meta_dict['files'] = files_list

            # E. Write _meta.json
            with open(node_out_dir / "_meta.json", "w", encoding='utf-8') as f:
                json.dump(meta_dict, f, indent=2)

        # 3. Create Global Index (for Search/Frontend)
        print("[Export] creating index.json...")
        with open(out_path / "index.json", "w", encoding='utf-8') as f:
            json.dump(uuid_to_path, f, indent=2)
        
        print(f"[Export] Complete. Data available in {out_path}")