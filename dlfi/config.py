import json
from dlfi.partition import FilePartitioner
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, asdict
import dlfi
import logging
import os

logger = logging.getLogger(__name__)


@dataclass
class VaultConfig:
	"""Configuration for a DLFI vault."""
	encrypted: bool = False
	salt: Optional[str] = None  # Base64 encoded encryption salt
	check_value: Optional[str] = None  # Encrypted verification string
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
	"""Manages vault configuration changes including re-encryption and re-partitioning."""
	
	VERIFICATION_STRING = "DLFI_VERIFICATION"
	
	def __init__(self, dlfi_instance: 'dlfi.core.DLFI'):
		self.dlfi = dlfi_instance
	
	def _get_all_blob_hashes(self) -> list:
		"""Get all blob hashes from database."""
		cursor = self.dlfi.conn.execute("SELECT hash FROM blobs")
		return [row[0] for row in cursor.fetchall()]
	
	def _read_blob_raw(self, file_hash: str) -> Optional[bytes]:
		"""Read raw blob data (encrypted or not) from storage."""
		from .partition import FilePartitioner
		
		part_files = FilePartitioner.get_part_files(self.dlfi.storage_dir, file_hash)
		if not part_files:
			return None
		
		# Read and concatenate all parts
		data = bytearray()
		for part in sorted(part_files, key=lambda p: p.name):
			with open(part, 'rb') as f:
				data.extend(f.read())
		
		return bytes(data)
	
	def _write_blob_raw(self, file_hash: str, data: bytes, partitioner: FilePartitioner) -> int:
		"""
		Write raw blob data to storage with given partitioner.
		Returns the number of parts written.
		"""
		shard_a = file_hash[:2]
		shard_b = file_hash[2:4]
		blob_dir = self.dlfi.storage_dir / shard_a / shard_b
		blob_dir.mkdir(parents=True, exist_ok=True)
		
		# Remove any existing files for this hash
		for existing in blob_dir.glob(f"{file_hash}*"):
			existing.unlink()
		
		# Write with partitioning
		if partitioner.needs_partitioning(len(data)):
			parts = partitioner.split_bytes(data)
			for i, part_data in enumerate(parts, 1):
				part_path = blob_dir / f"{file_hash}.{i:03d}"
				with open(part_path, 'wb') as f:
					f.write(part_data)
			return len(parts)
		else:
			with open(blob_dir / file_hash, 'wb') as f:
				f.write(data)
			return 0
	
	def _update_blob_part_count(self, file_hash: str, part_count: int):
		"""Update part_count in database."""
		self.dlfi.conn.execute(
			"UPDATE blobs SET part_count = ? WHERE hash = ?",
			(part_count, file_hash)
		)
	
	def enable_encryption(self, password: str) -> bool:
		"""
		Enable encryption on an existing vault.
		Encrypts all existing blobs and updates config.
		"""
		from .crypto import VaultCrypto
		
		if self.dlfi.config.encrypted:
			logger.error("Vault is already encrypted")
			return False
		
		if not password:
			logger.error("Password is required to enable encryption")
			return False
		
		logger.info("Enabling encryption on vault...")
		
		# Create new crypto instance
		new_crypto = VaultCrypto(password=password)
		
		# Process all blobs
		blob_hashes = self._get_all_blob_hashes()
		total = len(blob_hashes)
		
		logger.info(f"Encrypting {total} blobs...")
		
		with self.dlfi.conn:
			for i, file_hash in enumerate(blob_hashes, 1):
				try:
					# Read plaintext data
					plaintext = self._read_blob_raw(file_hash)
					if plaintext is None:
						logger.warning(f"Blob not found: {file_hash[:8]}...")
						continue
					
					# Encrypt
					encrypted = new_crypto.encrypt(plaintext)
					
					# Write back (may need re-partitioning due to size change)
					part_count = self._write_blob_raw(file_hash, encrypted, self.dlfi.partitioner)
					self._update_blob_part_count(file_hash, part_count)
					
					if i % 100 == 0 or i == total:
						logger.info(f"Encrypted {i}/{total} blobs")
						
				except Exception as e:
					logger.error(f"Failed to encrypt blob {file_hash[:8]}: {e}")
					raise
		
		# Update config
		self.dlfi.config.encrypted = True
		self.dlfi.config.salt = new_crypto.get_salt_b64()
		self.dlfi.config.check_value = new_crypto.encrypt_string(self.VERIFICATION_STRING)
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
		
		if not current_password:
			logger.error("Current password is required")
			return False
		
		# Verify password by creating crypto instance
		try:
			old_crypto = VaultCrypto.from_salt_b64(current_password, self.dlfi.config.salt)
			# Verify check value if it exists
			if self.dlfi.config.check_value:
				try:
					decrypted_check = old_crypto.decrypt_string(self.dlfi.config.check_value)
					if decrypted_check != self.VERIFICATION_STRING:
						raise ValueError("Incorrect password")
				except Exception:
					raise ValueError("Incorrect password")
		except Exception as e:
			logger.error(f"Failed to initialize decryption: {e}")
			return False
		
		logger.info("Disabling encryption on vault...")
		
		# Process all blobs
		blob_hashes = self._get_all_blob_hashes()
		total = len(blob_hashes)
		
		logger.info(f"Decrypting {total} blobs...")
		
		with self.dlfi.conn:
			for i, file_hash in enumerate(blob_hashes, 1):
				try:
					# Read encrypted data
					encrypted = self._read_blob_raw(file_hash)
					if encrypted is None:
						logger.warning(f"Blob not found: {file_hash[:8]}...")
						continue
					
					# Decrypt
					try:
						plaintext = old_crypto.decrypt(encrypted)
					except Exception as e:
						logger.error(f"Decryption failed for {file_hash[:8]}: {e}")
						raise ValueError(f"Decryption failed - incorrect password or corrupted data")
					
					# Write back (may need re-partitioning due to size change)
					part_count = self._write_blob_raw(file_hash, plaintext, self.dlfi.partitioner)
					self._update_blob_part_count(file_hash, part_count)
					
					if i % 100 == 0 or i == total:
						logger.info(f"Decrypted {i}/{total} blobs")
						
				except Exception as e:
					logger.error(f"Failed to decrypt blob {file_hash[:8]}: {e}")
					raise
		
		# Update config
		self.dlfi.config.encrypted = False
		self.dlfi.config.salt = None
		self.dlfi.config.check_value = None
		self.dlfi.config.save(self.dlfi.config_path)
		
		# Update instance
		self.dlfi.crypto = VaultCrypto()  # No password = no encryption
		
		logger.info("Encryption disabled successfully")
		return True
	
	def change_password(self, old_password: str, new_password: str) -> bool:
		"""
		Change encryption password.
		Re-encrypts all blobs with new key.
		"""
		from .crypto import VaultCrypto
		
		if not self.dlfi.config.encrypted:
			logger.error("Vault is not encrypted - use enable_encryption() instead")
			return False
		
		if not old_password or not new_password:
			logger.error("Both old and new passwords are required")
			return False
		
		# Verify old password
		try:
			old_crypto = VaultCrypto.from_salt_b64(old_password, self.dlfi.config.salt)
			# Verify check value if it exists
			if self.dlfi.config.check_value:
				try:
					decrypted_check = old_crypto.decrypt_string(self.dlfi.config.check_value)
					if decrypted_check != self.VERIFICATION_STRING:
						raise ValueError("Incorrect password")
				except Exception:
					raise ValueError("Incorrect password")
		except Exception as e:
			logger.error(f"Failed to verify old password: {e}")
			return False
		
		# Create new crypto with new password and NEW salt
		new_crypto = VaultCrypto(password=new_password)
		
		logger.info("Changing vault password...")
		
		# Process all blobs
		blob_hashes = self._get_all_blob_hashes()
		total = len(blob_hashes)
		
		logger.info(f"Re-encrypting {total} blobs with new password...")
		
		with self.dlfi.conn:
			for i, file_hash in enumerate(blob_hashes, 1):
				try:
					# Read encrypted data
					encrypted_old = self._read_blob_raw(file_hash)
					if encrypted_old is None:
						logger.warning(f"Blob not found: {file_hash[:8]}...")
						continue
					
					# Decrypt with old key
					try:
						plaintext = old_crypto.decrypt(encrypted_old)
					except Exception as e:
						logger.error(f"Decryption failed for {file_hash[:8]}: {e}")
						raise ValueError(f"Decryption failed - incorrect password or corrupted data")
					
					# Encrypt with new key
					encrypted_new = new_crypto.encrypt(plaintext)
					
					# Write back (may need re-partitioning due to size change from new nonce)
					part_count = self._write_blob_raw(file_hash, encrypted_new, self.dlfi.partitioner)
					self._update_blob_part_count(file_hash, part_count)
					
					if i % 100 == 0 or i == total:
						logger.info(f"Re-encrypted {i}/{total} blobs")
						
				except Exception as e:
					logger.error(f"Failed to re-encrypt blob {file_hash[:8]}: {e}")
					raise
		
		# Update config with new salt and new check value
		self.dlfi.config.salt = new_crypto.get_salt_b64()
		self.dlfi.config.check_value = new_crypto.encrypt_string(self.VERIFICATION_STRING)
		self.dlfi.config.save(self.dlfi.config_path)
		
		# Update instance
		self.dlfi.crypto = new_crypto
		
		logger.info("Password changed successfully")
		return True
	
	def change_partition_size(self, new_size: int) -> bool:
		"""
		Change partition size and re-partition all blobs accordingly.
		
		:param new_size: New chunk size in bytes. 0 to disable partitioning.
		"""
		from .partition import FilePartitioner
		
		if new_size < 0:
			logger.error("Partition size cannot be negative")
			return False
		
		if new_size > 0 and new_size < FilePartitioner.MIN_CHUNK_SIZE:
			logger.error(f"Partition size must be at least {FilePartitioner.MIN_CHUNK_SIZE} bytes")
			return False
		
		old_size = self.dlfi.config.partition_size
		if old_size == new_size:
			logger.info("Partition size unchanged")
			return True
		
		logger.info(f"Changing partition size from {old_size} to {new_size} bytes...")
		
		# Create new partitioner
		new_partitioner = FilePartitioner(chunk_size=new_size)
		
		# Process all blobs
		blob_hashes = self._get_all_blob_hashes()
		total = len(blob_hashes)
		
		logger.info(f"Re-partitioning {total} blobs...")
		
		with self.dlfi.conn:
			for i, file_hash in enumerate(blob_hashes, 1):
				try:
					# Read current data (encrypted or plaintext, doesn't matter)
					data = self._read_blob_raw(file_hash)
					if data is None:
						logger.warning(f"Blob not found: {file_hash[:8]}...")
						continue
					
					# Write back with new partitioning
					part_count = self._write_blob_raw(file_hash, data, new_partitioner)
					self._update_blob_part_count(file_hash, part_count)
					
					if i % 100 == 0 or i == total:
						logger.info(f"Re-partitioned {i}/{total} blobs")
						
				except Exception as e:
					logger.error(f"Failed to re-partition blob {file_hash[:8]}: {e}")
					raise
		
		# Update config
		self.dlfi.config.partition_size = new_size
		self.dlfi.config.save(self.dlfi.config_path)
		
		# Update instance
		self.dlfi.partitioner = new_partitioner
		
		logger.info("Partition size changed successfully")
		return True
	
	def reconfigure(self, 
					password: str = None,
					new_password: str = None,
					enable_encryption: bool = None,
					partition_size: int = None) -> bool:
		"""
		Convenience method to change multiple settings at once.
		Handles the correct order of operations.
		
		:param password: Current password (required if vault is encrypted)
		:param new_password: New password (if changing password or enabling encryption)
		:param enable_encryption: True to enable, False to disable, None to keep current
		:param partition_size: New partition size, or None to keep current
		"""
		success = True
		
		# Handle encryption changes first
		if enable_encryption is True and not self.dlfi.config.encrypted:
			pwd = new_password or password
			if not pwd:
				logger.error("Password required to enable encryption")
				return False
			success = self.enable_encryption(pwd) and success
			
		elif enable_encryption is False and self.dlfi.config.encrypted:
			if not password:
				logger.error("Current password required to disable encryption")
				return False
			success = self.disable_encryption(password) and success
			
		elif new_password and self.dlfi.config.encrypted:
			if not password:
				logger.error("Current password required to change password")
				return False
			success = self.change_password(password, new_password) and success
		
		# Then handle partition size
		if partition_size is not None and partition_size != self.dlfi.config.partition_size:
			success = self.change_partition_size(partition_size) and success
		
		return success