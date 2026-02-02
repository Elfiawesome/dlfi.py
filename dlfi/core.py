import os
import sqlite3
import json
import hashlib
import uuid
import shutil
import time
import tempfile
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any, IO

from .crypto import VaultCrypto
from .partition import FilePartitioner
from .config import VaultConfig, VaultConfigManager

logger = logging.getLogger(__name__)


class DLFI:
	def __init__(self, archive_root: str, password: Optional[str] = None):
		"""
		Initialize the Archive System.
		:param archive_root: Path to the root directory of your archive.
		:param password: Password for encrypted vaults (required if vault is encrypted).
		"""
		self.root = Path(archive_root).resolve()
		self.system_dir = self.root / ".dlfi"
		self.storage_dir = self.root / "blobs"  # Shared blob storage (outside .dlfi)
		self.temp_dir = self.system_dir / "temp"
		self.db_path = self.system_dir / "db.sqlite"
		self.config_path = self.system_dir / "config.json"
		
		# Initialize directories
		self._initialize_structure()
		
		# Load or create config
		self.config = VaultConfig.load(self.config_path)
		
		# Initialize crypto
		if self.config.encrypted:
			if not password:
				raise ValueError("Password required for encrypted vault")
			if not self.config.salt:
				raise ValueError("Encrypted vault missing salt in config")
			self.crypto = VaultCrypto.from_salt_b64(password, self.config.salt)
			
			try:
				if self.crypto.decrypt_string(self.config.check_value) == VaultConfigManager.VERIFICATION_STRING:
					pass
			except Exception as e:
				raise ValueError(f"Encrypted vault wrong password: {e}")
		else:
			self.crypto = VaultCrypto(password=password) if password else VaultCrypto()
			if password and not self.config.encrypted:
				# New vault with password - enable encryption
				self.config.encrypted = True
				self.config.salt = self.crypto.get_salt_b64()
				# Create verification token for new encrypted vault
				self.config.check_value = self.crypto.encrypt_string(VaultConfigManager.VERIFICATION_STRING)
				self.config.save(self.config_path)
		
		# Initialize partitioner
		self.partitioner = FilePartitioner(chunk_size=self.config.partition_size)
		
		# Connect to DB
		self.conn = self._get_connection()
		self._initialize_schema()
		
		# Config manager for runtime changes
		self._config_manager = None

	def _initialize_structure(self):
		"""Creates the archive structure if it doesn't exist."""
		os.makedirs(self.system_dir, exist_ok=True)
		os.makedirs(self.storage_dir, exist_ok=True)
		os.makedirs(self.temp_dir, exist_ok=True)
		
		# Clean stale temp files from previous runs
		for tmp_file in self.temp_dir.glob("*"):
			try:
				os.remove(tmp_file)
			except OSError:
				pass

		logger.info(f"Initialized archive at {self.root}")

	def _get_connection(self) -> sqlite3.Connection:
		"""Returns a tuned SQLite connection."""
		conn = sqlite3.connect(self.db_path, check_same_thread=False)
		conn.execute("PRAGMA journal_mode=WAL;")
		conn.execute("PRAGMA synchronous=NORMAL;")
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
			self.conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_uuid);")
			self.conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_path ON nodes(cached_path);")

			# 2. BLOBS (Physical Files - Deduplicated)
			self.conn.execute("""
				CREATE TABLE IF NOT EXISTS blobs (
					hash TEXT PRIMARY KEY,
					ext TEXT,
					size_bytes INTEGER,
					storage_path TEXT,
					part_count INTEGER DEFAULT 0
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
		"""Close the database connection."""
		self.conn.close()

	@property
	def config_manager(self) -> VaultConfigManager:
		"""Get the configuration manager for runtime config changes."""
		if self._config_manager is None:
			self._config_manager = VaultConfigManager(self)
		return self._config_manager

	# --- Helper: Hashing ---
	@staticmethod
	def get_file_hash(filepath: str) -> str:
		"""Stream the file to calculate SHA256 without loading into RAM."""
		sha256 = hashlib.sha256()
		with open(filepath, 'rb') as f:
			while True:
				data = f.read(65536)
				if not data:
					break
				sha256.update(data)
		return sha256.hexdigest()

	@staticmethod
	def get_bytes_hash(data: bytes) -> str:
		"""Calculate SHA256 of bytes."""
		return hashlib.sha256(data).hexdigest()

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

	def append_file(self, record_path: str, file_path: str, filename_override: str = None):
		"""
		Appends a local file to a record.
		"""
		file_path = Path(file_path)
		if not file_path.exists():
			raise FileNotFoundError(f"File not found: {file_path}")

		node_uuid = self._resolve_path(record_path, create_if_missing=False)
		if not node_uuid:
			raise ValueError(f"Record not found: {record_path}")

		final_name = filename_override if filename_override else file_path.name
		
		# Read and hash plaintext
		with open(file_path, 'rb') as f:
			plaintext = f.read()
		
		file_hash = self.get_bytes_hash(plaintext)
		file_size = len(plaintext)
		
		self._store_blob_and_link(node_uuid, file_hash, file_size, final_name, plaintext)

	def append_stream(self, record_path: str, file_stream: IO[bytes], filename: str):
		"""
		Appends a data stream to a record.
		"""
		node_uuid = self._resolve_path(record_path, create_if_missing=False)
		if not node_uuid:
			raise ValueError(f"Record not found: {record_path}")

		# Stream to memory while hashing (for plaintext hash)
		sha256 = hashlib.sha256()
		chunks = []
		
		try:
			while True:
				chunk = file_stream.read(65536)
				if not chunk:
					break
				sha256.update(chunk)
				chunks.append(chunk)
		except Exception as e:
			logger.error(f"Stream interrupted for {filename}: {e}")
			raise

		plaintext = b''.join(chunks)
		file_hash = sha256.hexdigest()
		file_size = len(plaintext)
		
		self._store_blob_and_link(node_uuid, file_hash, file_size, filename, plaintext)

	def _store_blob_and_link(self, node_uuid: str, file_hash: str, file_size: int, 
							filename: str, plaintext: bytes):
		"""
		Internal: Handles blob storage with encryption and partitioning.
		Hash is of PLAINTEXT for deduplication.
		"""
		ext = Path(filename).suffix.lower().lstrip('.')

		with self.conn:
			# Check if blob exists (deduplication by plaintext hash)
			cursor = self.conn.execute("SELECT hash FROM blobs WHERE hash = ?", (file_hash,))
			if not cursor.fetchone():
				# Encrypt if enabled
				if self.crypto.enabled:
					data_to_store = self.crypto.encrypt(plaintext)
				else:
					data_to_store = plaintext
				
				# Determine storage location
				shard_a = file_hash[:2]
				shard_b = file_hash[2:4]
				storage_subdir = self.storage_dir / shard_a / shard_b
				os.makedirs(storage_subdir, exist_ok=True)
				
				# Handle partitioning
				part_count = 0
				if self.partitioner.needs_partitioning(len(data_to_store)):
					parts = self.partitioner.split_bytes(data_to_store)
					part_count = len(parts)
					for i, part_data in enumerate(parts, 1):
						part_path = storage_subdir / f"{file_hash}.{i:03d}"
						with open(part_path, 'wb') as f:
							f.write(part_data)
					logger.debug(f"Stored blob {file_hash[:8]}... in {part_count} parts")
				else:
					target_path = storage_subdir / file_hash
					with open(target_path, 'wb') as f:
						f.write(data_to_store)
					logger.debug(f"Stored new blob: {file_hash[:8]}... ({filename})")

				# Insert blob record
				rel_path = f"{shard_a}/{shard_b}/{file_hash}"
				self.conn.execute("""
					INSERT INTO blobs (hash, ext, size_bytes, storage_path, part_count)
					VALUES (?, ?, ?, ?, ?)
				""", (file_hash, ext, file_size, rel_path, part_count))
			else:
				logger.debug(f"Deduplicated blob: {file_hash[:8]}...")

			# Link blob to node
			cur = self.conn.execute("SELECT COUNT(*) FROM node_files WHERE node_uuid = ?", (node_uuid,))
			count = cur.fetchone()[0]
			
			self.conn.execute("""
				INSERT INTO node_files (node_uuid, file_hash, original_name, display_order, added_at)
				VALUES (?, ?, ?, ?, ?)
			""", (node_uuid, file_hash, filename, count + 1, time.time()))
			
			self.conn.execute("UPDATE nodes SET last_modified = ? WHERE uuid = ?", (time.time(), node_uuid))

	def read_blob(self, file_hash: str) -> Optional[bytes]:
		"""
		Read and decrypt a blob by its hash.
		Returns plaintext bytes or None if not found.
		"""
		cursor = self.conn.execute(
			"SELECT storage_path, part_count FROM blobs WHERE hash = ?", (file_hash,)
		)
		row = cursor.fetchone()
		if not row:
			return None
		
		storage_path, part_count = row
		
		# Read data (handle partitions)
		if part_count > 0:
			parts = FilePartitioner.get_part_files(self.storage_dir, file_hash)
			data = bytearray()
			for part in sorted(parts, key=lambda p: p.name):
				with open(part, 'rb') as f:
					data.extend(f.read())
			data = bytes(data)
		else:
			blob_path = self.storage_dir / storage_path
			if not blob_path.exists():
				return None
			with open(blob_path, 'rb') as f:
				data = f.read()
		
		# Decrypt if needed
		if self.crypto.enabled:
			data = self.crypto.decrypt(data)
		
		return data

	# --- Path Resolution ---

	def _resolve_path(self, path: str, create_if_missing=False, node_type='VAULT', metadata=None) -> Optional[str]:
		"""
		Converts a path string into a UUID.
		If create_if_missing is True, it builds the hierarchy.
		"""
		clean_path = path.strip("/").replace("\\", "/")
		parts = clean_path.split("/")
		
		current_parent_uuid = None
		current_path_str = ""

		for i, part in enumerate(parts):
			is_last = (i == len(parts) - 1)
			if i > 0:
				current_path_str += "/"
			current_path_str += part

			cursor = self.conn.execute(
				"SELECT uuid FROM nodes WHERE parent_uuid IS ? AND name = ?", 
				(current_parent_uuid, part)
			)
			row = cursor.fetchone()

			if row:
				current_parent_uuid = row[0]
			else:
				if not create_if_missing:
					return None
				
				new_uuid = str(uuid.uuid4())
				actual_type = node_type if is_last else 'VAULT'
				actual_meta = json.dumps(metadata) if (is_last and metadata) else None
				
				with self.conn:
					self.conn.execute("""
						INSERT INTO nodes (uuid, parent_uuid, type, name, cached_path, metadata, created_at, last_modified)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					""", (new_uuid, current_parent_uuid, actual_type, part, current_path_str, actual_meta, time.time(), time.time()))
				
				current_parent_uuid = new_uuid

		return current_parent_uuid

	# --- GRAPH OPERATIONS ---

	def link(self, source_path: str, target_path: str, relation: str):
		"""Creates a directed relationship between two nodes."""
		src_uuid = self._resolve_path(source_path)
		tgt_uuid = self._resolve_path(target_path)

		if not src_uuid: 
			raise ValueError(f"Source path not found: {source_path}")
		if not tgt_uuid: 
			raise ValueError(f"Target path not found: {target_path}")

		with self.conn:
			self.conn.execute("""
				INSERT OR REPLACE INTO edges (source_uuid, target_uuid, relation, created_at)
				VALUES (?, ?, ?, ?)
			""", (src_uuid, tgt_uuid, relation.upper(), time.time()))

	def add_tag(self, path: str, tag: str):
		"""Adds a primitive string tag to a node."""
		node_uuid = self._resolve_path(path)
		if not node_uuid: 
			raise ValueError(f"Node not found: {path}")

		with self.conn:
			self.conn.execute("""
				INSERT OR IGNORE INTO tags (node_uuid, tag)
				VALUES (?, ?)
			""", (node_uuid, tag.lower()))

	# --- STATIC SITE GENERATION ---

	def generate_static_site(self):
		"""
		Generates static site files (manifest.json and index.html).
		Blobs are already in the shared storage folder.
		"""
		from .static import StaticSiteGenerator
		generator = StaticSiteGenerator(self)
		generator.generate()

	# --- LEGACY EXPORT (for backwards compatibility) ---
	
	def export(self, output_dir: str = None):
		"""
		Generate static site. If output_dir is provided (legacy), logs a warning.
		Static site is now generated in the archive root.
		"""
		if output_dir:
			logger.warning(
				"output_dir parameter is deprecated. Static site is generated in archive root. "
				"Blobs are shared between database and static site."
			)
		self.generate_static_site()

	# --- QUERY BUILDER ---

	def query(self) -> 'QueryBuilder':
		"""Returns a fluent Query Builder."""
		return QueryBuilder(self)


class QueryBuilder:
	def __init__(self, dlfi_instance):
		self.db = dlfi_instance
		self.conn = dlfi_instance.conn
		self.conditions = []
		self.params = []
		self.tables = ["nodes n"]
		self.distinct = False

	def inside(self, path_prefix: str):
		clean = path_prefix.strip("/")
		self.conditions.append("n.cached_path LIKE ?")
		self.params.append(f"{clean}/%")
		return self

	def type(self, node_type: str):
		self.conditions.append("n.type = ?")
		self.params.append(node_type)
		return self

	def meta_eq(self, key: str, value: Any):
		self.conditions.append(f"json_extract(n.metadata, '$.{key}') = ?")
		self.params.append(value)
		return self

	def has_tag(self, tag: str):
		if "tags t" not in self.tables:
			self.tables.append("JOIN tags t ON n.uuid = t.node_uuid")
		self.conditions.append("t.tag = ?")
		self.params.append(tag.lower())
		self.distinct = True
		return self

	def related_to(self, target_path: str, relation: str = None):
		"""Finds nodes that directly point to the target."""
		target_uuid = self.db._resolve_path(target_path)
		if not target_uuid:
			self.conditions.append("1=0")
			return self

		alias = f"e_{len(self.tables)}"
		self.tables.append(f"JOIN edges {alias} ON n.uuid = {alias}.source_uuid")
		
		self.conditions.append(f"{alias}.target_uuid = ?")
		self.params.append(target_uuid)

		if relation:
			self.conditions.append(f"{alias}.relation = ?")
			self.params.append(relation)
		
		self.distinct = True
		return self

	def contains_related(self, target_path: str, relation: str = None):
		"""Finds Vaults containing any child related to the target."""
		target_uuid = self.db._resolve_path(target_path)
		if not target_uuid:
			self.conditions.append("1=0")
			return self

		subquery = """
		EXISTS (
			SELECT 1 FROM nodes child
			JOIN edges e ON child.uuid = e.source_uuid
			WHERE child.cached_path LIKE n.cached_path || '/%'
			AND e.target_uuid = ?
		"""
		
		sub_params = [target_uuid]

		if relation:
			subquery += " AND e.relation = ?"
			sub_params.append(relation)

		subquery += ")"

		self.conditions.append(subquery)
		self.params.extend(sub_params)
		return self

	def execute(self) -> List[Dict]:
		base = "SELECT DISTINCT n.uuid, n.cached_path, n.type, n.metadata FROM " if self.distinct else "SELECT n.uuid, n.cached_path, n.type, n.metadata FROM "
		query_str = base + " ".join(self.tables)
		
		if self.conditions:
			query_str += " WHERE " + " AND ".join(self.conditions)
		
		query_str += " ORDER BY n.cached_path ASC"
		
		cursor = self.conn.execute(query_str, self.params)
		results = []
		for row in cursor:
			results.append({
				"uuid": row[0],
				"path": row[1],
				"type": row[2],
				"metadata": json.loads(row[3]) if row[3] else {}
			})
		return results