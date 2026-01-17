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