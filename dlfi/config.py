import json
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, asdict, field
import logging
import shutil

logger = logging.getLogger(__name__)


@dataclass
class VaultConfig:
	"""Configuration for a DLFI vault."""
	encrypted: bool = False
	salt: Optional[str] = None  # Base64 encoded encryption salt
	partition_size: int = 50 * 1024 * 1024  # 50MB default, 0 = disabled
	version: int = 2  # Schema version for future migrations
	
	def to_dict(self) -> dict:
		return asdict(self)
	
	@classmethod
	def from_dict(cls, data: dict) -> 'VaultConfig':
		# Only use known fields
		known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
		return cls(**known)
	
	def save(self, path: Path):
		"""Save config to JSON file."""
		path.parent.mkdir(parents=True, exist_ok=True)
		with open(path, 'w', encoding='utf-8') as f:
			json.dump(self.to_dict(), f, indent=2)
		logger.debug(f"Saved vault config to {path}")
	
	@classmethod
	def load(cls, path: Path) -> 'VaultConfig':
		"""Load config from JSON file, or return defaults if not found."""
		if not path.exists():
			logger.debug(f"No config found at {path}, using defaults")
			return cls()
		
		try:
			with open(path, 'r', encoding='utf-8') as f:
				data = json.load(f)
			return cls.from_dict(data)
		except (json.JSONDecodeError, IOError) as e:
			logger.warning(f"Failed to load config: {e}, using defaults")
			return cls()
	
	def validate(self) -> bool:
		"""Validate config consistency."""
		if self.encrypted and not self.salt:
			logger.error("Encrypted vault requires salt")
			return False
		if self.partition_size < 0:
			logger.error("Partition size cannot be negative")
			return False
		return True


class VaultConfigManager:
	"""Manages vault configuration changes including re-encryption."""
	
	def __init__(self, dlfi_instance):
		self.dlfi = dlfi_instance
	
	def enable_encryption(self, password: str) -> bool:
		"""
		Enable encryption on an existing vault.
		Re-encrypts all blobs and updates config.
		"""
		from .crypto import VaultCrypto
		
		if self.dlfi.config.encrypted:
			logger.error("Vault is already encrypted")
			return False
		
		logger.info("Enabling encryption on vault...")
		
		# Create new crypto instance
		new_crypto = VaultCrypto(password=password)
		
		# Re-encrypt all blobs
		self._reprocess_blobs(old_crypto=None, new_crypto=new_crypto)
		
		# Update config
		self.dlfi.config.encrypted = True
		self.dlfi.config.salt = new_crypto.get_salt_b64()
		self.dlfi.config.save(self.dlfi.config_path)
		
		# Update instance
		self.dlfi.crypto = new_crypto
		
		logger.info("Encryption enabled successfully")
		return True
	
	def disable_encryption(self, current_password: str) -> bool:
		"""
		Disable encryption on an existing vault.
		Decrypts all blobs and updates config.
		"""
		from .crypto import VaultCrypto
		
		if not self.dlfi.config.encrypted:
			logger.error("Vault is not encrypted")
			return False
		
		# Verify password
		try:
			old_crypto = VaultCrypto.from_salt_b64(current_password, self.dlfi.config.salt)
		except Exception as e:
			logger.error(f"Invalid password: {e}")
			return False
		
		logger.info("Disabling encryption on vault...")
		
		# Decrypt all blobs
		self._reprocess_blobs(old_crypto=old_crypto, new_crypto=None)
		
		# Update config
		self.dlfi.config.encrypted = False
		self.dlfi.config.salt = None
		self.dlfi.config.save(self.dlfi.config_path)
		
		# Update instance
		self.dlfi.crypto = VaultCrypto()  # No password = no encryption
		
		logger.info("Encryption disabled successfully")
		return True
	
	def change_password(self, old_password: str, new_password: str) -> bool:
		"""Change encryption password, re-encrypting all blobs."""
		from .crypto import VaultCrypto
		
		if not self.dlfi.config.encrypted:
			logger.error("Vault is not encrypted")
			return False
		
		# Verify old password
		try:
			old_crypto = VaultCrypto.from_salt_b64(old_password, self.dlfi.config.salt)
		except Exception as e:
			logger.error(f"Invalid current password: {e}")
			return False
		
		logger.info("Changing vault password...")
		
		# Create new crypto with new password and new salt
		new_crypto = VaultCrypto(password=new_password)
		
		# Re-encrypt all blobs
		self._reprocess_blobs(old_crypto=old_crypto, new_crypto=new_crypto)
		
		# Update config
		self.dlfi.config.salt = new_crypto.get_salt_b64()
		self.dlfi.config.save(self.dlfi.config_path)
		
		# Update instance
		self.dlfi.crypto = new_crypto
		
		logger.info("Password changed successfully")
		return True
	
	def change_partition_size(self, new_size: int) -> bool:
		"""Change partition size and re-partition all blobs."""
		from .partition import FilePartitioner
		
		logger.info(f"Changing partition size to {new_size} bytes...")
		
		old_partitioner = self.dlfi.partitioner
		new_partitioner = FilePartitioner(chunk_size=new_size)
		
		# Re-partition all blobs
		self._repartition_blobs(old_partitioner, new_partitioner)
		
		# Update config
		self.dlfi.config.partition_size = new_size
		self.dlfi.config.save(self.dlfi.config_path)
		
		# Update instance
		self.dlfi.partitioner = new_partitioner
		
		logger.info("Partition size changed successfully")
		return True
	
	def _reprocess_blobs(self, old_crypto, new_crypto):
		"""Re-encrypt or decrypt all blobs."""
		from .partition import FilePartitioner
		
		cursor = self.dlfi.conn.execute("SELECT hash, storage_path, size_bytes FROM blobs")
		blobs = cursor.fetchall()
		
		for file_hash, rel_path, size_bytes in blobs:
			try:
				self._reprocess_single_blob(file_hash, old_crypto, new_crypto)
			except Exception as e:
				logger.error(f"Failed to reprocess blob {file_hash[:8]}: {e}")
				raise
	
	def _reprocess_single_blob(self, file_hash: str, old_crypto, new_crypto):
		"""Re-encrypt/decrypt a single blob (handles partitions)."""
		from .partition import FilePartitioner
		
		part_files = FilePartitioner.get_part_files(self.dlfi.storage_dir, file_hash)
		if not part_files:
			logger.warning(f"Blob files not found for {file_hash[:8]}")
			return
		
		# Read and decrypt
		data = bytearray()
		for part in sorted(part_files, key=lambda p: p.name):
			with open(part, 'rb') as f:
				part_data = f.read()
			if old_crypto and old_crypto.enabled:
				part_data = old_crypto.decrypt(part_data)
			data.extend(part_data)
		
		# Encrypt with new key
		if new_crypto and new_crypto.enabled:
			data = new_crypto.encrypt(bytes(data))
		
		# Write back (re-partition if needed)
		shard_a = file_hash[:2]
		shard_b = file_hash[2:4]
		blob_dir = self.dlfi.storage_dir / shard_a / shard_b
		
		# Remove old parts
		for part in part_files:
			part.unlink()
		
		# Write new data
		if self.dlfi.partitioner.needs_partitioning(len(data)):
			parts = self.dlfi.partitioner.split_bytes(bytes(data))
			for i, part_data in enumerate(parts, 1):
				part_path = blob_dir / f"{file_hash}.{i:03d}"
				with open(part_path, 'wb') as f:
					f.write(part_data)
		else:
			with open(blob_dir / file_hash, 'wb') as f:
				f.write(data)
	
	def _repartition_blobs(self, old_partitioner, new_partitioner):
		"""Re-partition all blobs with new chunk size."""
		cursor = self.dlfi.conn.execute("SELECT hash, size_bytes FROM blobs")
		blobs = cursor.fetchall()
		
		for file_hash, size_bytes in blobs:
			try:
				self._repartition_single_blob(file_hash, old_partitioner, new_partitioner)
			except Exception as e:
				logger.error(f"Failed to repartition blob {file_hash[:8]}: {e}")
				raise
	
	def _repartition_single_blob(self, file_hash: str, old_partitioner, new_partitioner):
		"""Re-partition a single blob."""
		from .partition import FilePartitioner
		
		part_files = FilePartitioner.get_part_files(self.dlfi.storage_dir, file_hash)
		if not part_files:
			logger.warning(f"Blob files not found for {file_hash[:8]}")
			return
		
		# Read all parts
		data = bytearray()
		for part in sorted(part_files, key=lambda p: p.name):
			with open(part, 'rb') as f:
				data.extend(f.read())
		
		# Get blob directory
		shard_a = file_hash[:2]
		shard_b = file_hash[2:4]
		blob_dir = self.dlfi.storage_dir / shard_a / shard_b
		
		# Remove old parts
		for part in part_files:
			part.unlink()
		
		# Write with new partitioning
		if new_partitioner.needs_partitioning(len(data)):
			parts = new_partitioner.split_bytes(bytes(data))
			for i, part_data in enumerate(parts, 1):
				part_path = blob_dir / f"{file_hash}.{i:03d}"
				with open(part_path, 'wb') as f:
					f.write(part_data)
		else:
			with open(blob_dir / file_hash, 'wb') as f:
				f.write(data)