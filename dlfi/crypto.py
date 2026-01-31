import os
import base64
import hashlib
from typing import Optional, Tuple, BinaryIO
from pathlib import Path
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import logging

logger = logging.getLogger(__name__)


class VaultCrypto:
	"""
	Handles all encryption/decryption for the vault using AES-256-GCM.
	Compatible with Web Crypto API for client-side decryption.
	"""
	SALT_SIZE = 16
	NONCE_SIZE = 12  # 96 bits for AES-GCM
	KEY_SIZE = 32    # 256 bits for AES-256
	ITERATIONS = 100000
	
	def __init__(self, password: Optional[str] = None, salt: Optional[bytes] = None):
		self.password = password
		self.salt = salt if salt else os.urandom(self.SALT_SIZE)
		self._key: Optional[bytes] = None
		
		if password:
			self._derive_key()
	
	def _derive_key(self):
		"""Derive encryption key from password using PBKDF2-SHA256."""
		kdf = PBKDF2HMAC(
			algorithm=hashes.SHA256(),
			length=self.KEY_SIZE,
			salt=self.salt,
			iterations=self.ITERATIONS,
			backend=default_backend()
		)
		self._key = kdf.derive(self.password.encode('utf-8'))
	
	@property
	def enabled(self) -> bool:
		return self._key is not None
	
	def encrypt(self, plaintext: bytes) -> bytes:
		"""
		Encrypt data using AES-256-GCM.
		Returns: nonce (12 bytes) + ciphertext + tag (16 bytes)
		"""
		if not self.enabled:
			return plaintext
		
		nonce = os.urandom(self.NONCE_SIZE)
		aesgcm = AESGCM(self._key)
		ciphertext = aesgcm.encrypt(nonce, plaintext, None)
		return nonce + ciphertext
	
	def decrypt(self, data: bytes) -> bytes:
		"""
		Decrypt AES-256-GCM encrypted data.
		Input format: nonce (12 bytes) + ciphertext + tag (16 bytes)
		"""
		if not self.enabled:
			return data
		
		if len(data) < self.NONCE_SIZE + 16:
			raise ValueError("Invalid encrypted data: too short")
		
		nonce = data[:self.NONCE_SIZE]
		ciphertext = data[self.NONCE_SIZE:]
		aesgcm = AESGCM(self._key)
		return aesgcm.decrypt(nonce, ciphertext, None)
	
	def encrypt_string(self, plaintext: str) -> str:
		"""Encrypt a string and return URL-safe base64."""
		if not self.enabled:
			return plaintext
		encrypted = self.encrypt(plaintext.encode('utf-8'))
		return base64.urlsafe_b64encode(encrypted).decode('ascii')
	
	def decrypt_string(self, encrypted: str) -> str:
		"""Decrypt a URL-safe base64 string."""
		if not self.enabled:
			return encrypted
		data = base64.urlsafe_b64decode(encrypted)
		return self.decrypt(data).decode('utf-8')
	
	def encrypt_filename(self, filename: str) -> str:
		"""
		Encrypt a filename to a filesystem-safe string.
		Uses URL-safe base64 without padding.
		"""
		if not self.enabled:
			return filename
		
		encrypted = self.encrypt(filename.encode('utf-8'))
		# Use URL-safe base64 and strip padding for cleaner filenames
		encoded = base64.urlsafe_b64encode(encrypted).decode('ascii')
		return encoded.rstrip('=')
	
	def decrypt_filename(self, encrypted_name: str) -> str:
		"""Decrypt a filesystem-safe encrypted filename."""
		if not self.enabled:
			return encrypted_name
		
		# Restore base64 padding
		padding = 4 - (len(encrypted_name) % 4)
		if padding != 4:
			encrypted_name += '=' * padding
		
		encrypted = base64.urlsafe_b64decode(encrypted_name)
		return self.decrypt(encrypted).decode('utf-8')
	
	def encrypt_file(self, input_path: Path, output_path: Path):
		"""Encrypt an entire file."""
		with open(input_path, 'rb') as f:
			plaintext = f.read()
		
		ciphertext = self.encrypt(plaintext)
		
		with open(output_path, 'wb') as f:
			f.write(ciphertext)
	
	def decrypt_file(self, input_path: Path, output_path: Path):
		"""Decrypt an entire file."""
		with open(input_path, 'rb') as f:
			ciphertext = f.read()
		
		plaintext = self.decrypt(ciphertext)
		
		with open(output_path, 'wb') as f:
			f.write(plaintext)
	
	def encrypt_stream(self, data: bytes) -> bytes:
		"""Encrypt data from memory. Alias for encrypt()."""
		return self.encrypt(data)
	
	def get_salt_b64(self) -> str:
		"""Return salt as base64 string for storage."""
		return base64.b64encode(self.salt).decode('ascii')
	
	@classmethod
	def from_salt_b64(cls, password: str, salt_b64: str) -> 'VaultCrypto':
		"""Create instance from stored salt."""
		salt = base64.b64decode(salt_b64)
		return cls(password=password, salt=salt)
	
	def change_password(self, new_password: str) -> 'VaultCrypto':
		"""
		Create a new crypto instance with a different password but new salt.
		The old instance is still needed to decrypt existing data.
		"""
		return VaultCrypto(password=new_password)
	
	def get_config_for_static(self) -> dict:
		"""Return config needed for client-side decryption."""
		return {
			"salt": self.get_salt_b64(),
			"iterations": self.ITERATIONS,
			"keyLength": self.KEY_SIZE,
			"nonceLength": self.NONCE_SIZE,
			"algorithm": "AES-GCM"
		}