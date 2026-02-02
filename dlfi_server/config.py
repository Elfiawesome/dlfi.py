from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List
import os


@dataclass
class ServerConfig:
	"""Configuration for the DLFI web server."""
	host: str = "127.0.0.1"
	port: int = 8080
	debug: bool = False
	secret_key: str = field(default_factory=lambda: os.urandom(24).hex())
	default_vaults_dir: Path = field(default_factory=lambda: Path.cwd() / ".vaults")
	max_upload_size: int = 100 * 1024 * 1024  # 100MB
	recent_vaults_file: Path = field(default_factory=lambda: Path.cwd() / ".vaults" / ".recent")
	
	def __post_init__(self):
		if isinstance(self.default_vaults_dir, str):
			self.default_vaults_dir = Path(self.default_vaults_dir)
		if isinstance(self.recent_vaults_file, str):
			self.recent_vaults_file = Path(self.recent_vaults_file)
		
		# Ensure default vaults directory exists
		self.default_vaults_dir.mkdir(parents=True, exist_ok=True)
	
	def get_recent_vaults(self) -> List[str]:
		"""Get list of recently opened vault paths."""
		if not self.recent_vaults_file.exists():
			return []
		try:
			with open(self.recent_vaults_file, 'r', encoding='utf-8') as f:
				paths = [line.strip() for line in f if line.strip()]
				# Filter to only existing vaults
				return [p for p in paths if Path(p).exists() and (Path(p) / ".dlfi").exists()]
		except:
			return []
	
	def add_recent_vault(self, path: str):
		"""Add a vault path to recent list."""
		path = str(Path(path).resolve())
		recent = self.get_recent_vaults()
		
		# Remove if already exists, add to front
		if path in recent:
			recent.remove(path)
		recent.insert(0, path)
		
		# Keep only last 20
		recent = recent[:20]
		
		try:
			self.recent_vaults_file.parent.mkdir(parents=True, exist_ok=True)
			with open(self.recent_vaults_file, 'w', encoding='utf-8') as f:
				f.write('\n'.join(recent))
		except:
			pass