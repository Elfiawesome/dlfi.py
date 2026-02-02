from dataclasses import dataclass, field
from pathlib import Path
from typing import List
import os
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class ServerConfig:
	"""Configuration for the DLFI web server."""
	host: str = "127.0.0.1"
	port: int = 8080
	debug: bool = False
	secret_key: str = field(default_factory=lambda: os.urandom(24).hex())
	default_vaults_dir: Path = None
	max_upload_size: int = 100 * 1024 * 1024  # 100MB
	
	def __post_init__(self):
		# Set default vaults dir if not provided
		if self.default_vaults_dir is None:
			self.default_vaults_dir = Path.cwd() / ".vaults"
		elif isinstance(self.default_vaults_dir, str):
			self.default_vaults_dir = Path(self.default_vaults_dir)
		
		# Ensure it's resolved
		self.default_vaults_dir = self.default_vaults_dir.resolve()
		
		# Ensure default vaults directory exists
		self.default_vaults_dir.mkdir(parents=True, exist_ok=True)
	
	@property
	def recent_vaults_file(self) -> Path:
		"""Recent vaults file is always in the default vaults directory."""
		return self.default_vaults_dir / ".recent"
	
	def get_recent_vaults(self) -> List[dict]:
		"""Get list of recently opened vault paths with their info."""
		if not self.recent_vaults_file.exists():
			return []
		
		result = []
		try:
			with open(self.recent_vaults_file, 'r', encoding='utf-8') as f:
				for line in f:
					line = line.strip()
					if not line:
						continue
					
					try:
						path = Path(line)
					except Exception:
						continue
					
					# Check if vault exists
					if not path.exists() or not (path / ".dlfi").exists():
						continue
					
					# Check if encrypted
					encrypted = False
					config_path = path / ".dlfi" / "config.json"
					if config_path.exists():
						try:
							with open(config_path, 'r', encoding='utf-8') as cf:
								vault_config = json.load(cf)
								encrypted = vault_config.get("encrypted", False)
						except:
							pass
					
					result.append({
						"name": path.name,
						"path": str(path),
						"encrypted": encrypted
					})
		except Exception as e:
			logger.warning(f"Error reading recent vaults: {e}")
			return []
		
		return result
	
	def add_recent_vault(self, vault_path: str):
		"""Add a vault path to recent list."""
		# Normalize the path
		try:
			path = Path(vault_path).resolve()
			path_str = str(path)
		except Exception as e:
			logger.warning(f"Could not add recent vault: {e}")
			return
		
		# Read existing entries
		existing = []
		if self.recent_vaults_file.exists():
			try:
				with open(self.recent_vaults_file, 'r', encoding='utf-8') as f:
					existing = [line.strip() for line in f if line.strip()]
			except:
				pass
		
		# Remove if already exists (case-insensitive on Windows)
		import platform
		if platform.system() == 'Windows':
			existing = [p for p in existing if p.lower() != path_str.lower()]
		else:
			existing = [p for p in existing if p != path_str]
		
		# Add to front
		existing.insert(0, path_str)
		
		# Keep only last 20
		existing = existing[:20]
		
		# Write back
		try:
			self.recent_vaults_file.parent.mkdir(parents=True, exist_ok=True)
			with open(self.recent_vaults_file, 'w', encoding='utf-8') as f:
				f.write('\n'.join(existing))
		except Exception as e:
			logger.warning(f"Error saving recent vaults: {e}")