import os
from pathlib import Path
from typing import List, Generator, IO, Tuple
import logging

logger = logging.getLogger(__name__)


class FilePartitioner:
	"""
	Handles splitting large files into smaller chunks for GitHub compatibility.
	Default chunk size is 50MB to stay well under GitHub's 100MB limit.
	"""
	DEFAULT_CHUNK_SIZE = 50 * 1024 * 1024  # 50MB
	MIN_CHUNK_SIZE = 1 * 1024 * 1024       # 1MB minimum
	
	def __init__(self, chunk_size: int = None):
		"""
		Initialize partitioner.
		:param chunk_size: Size in bytes. 0 or None = disabled.
		"""
		self._chunk_size = chunk_size if chunk_size else 0
	
	@property
	def chunk_size(self) -> int:
		return self._chunk_size
	
	@chunk_size.setter
	def chunk_size(self, value: int):
		if value and value < self.MIN_CHUNK_SIZE:
			raise ValueError(f"Chunk size must be at least {self.MIN_CHUNK_SIZE} bytes")
		self._chunk_size = value if value else 0
	
	@property
	def enabled(self) -> bool:
		return self._chunk_size > 0
	
	def needs_partitioning(self, file_size: int) -> bool:
		"""Check if a file needs to be split."""
		if not self.enabled:
			return False
		return file_size > self._chunk_size
	
	def get_part_count(self, file_size: int) -> int:
		"""Calculate number of parts for a file."""
		if not self.needs_partitioning(file_size):
			return 1
		return (file_size + self._chunk_size - 1) // self._chunk_size
	
	def split_bytes(self, data: bytes) -> List[bytes]:
		"""Split bytes into chunks."""
		if not self.needs_partitioning(len(data)):
			return [data]
		
		parts = []
		for i in range(0, len(data), self._chunk_size):
			parts.append(data[i:i + self._chunk_size])
		return parts
	
	def split_file(self, source_path: Path, dest_dir: Path, base_name: str) -> List[str]:
		"""
		Split a file into chunks.
		Returns list of chunk filenames.
		"""
		file_size = source_path.stat().st_size
		
		if not self.needs_partitioning(file_size):
			# No partitioning needed, just copy/return base name
			return [base_name]
		
		parts = []
		part_num = 1
		
		with open(source_path, 'rb') as f:
			while True:
				chunk = f.read(self._chunk_size)
				if not chunk:
					break
				
				part_name = f"{base_name}.{part_num:03d}"
				part_path = dest_dir / part_name
				
				with open(part_path, 'wb') as pf:
					pf.write(chunk)
				
				parts.append(part_name)
				part_num += 1
				logger.debug(f"Created partition: {part_name}")
		
		logger.info(f"Split {source_path.name} into {len(parts)} parts")
		return parts
	
	def reassemble_parts(self, part_paths: List[Path]) -> bytes:
		"""Reassemble chunks into bytes."""
		data = bytearray()
		for part in sorted(part_paths, key=lambda p: p.name):
			with open(part, 'rb') as f:
				data.extend(f.read())
		return bytes(data)
	
	def reassemble_to_file(self, part_paths: List[Path], dest_path: Path):
		"""Reassemble chunks into a single file."""
		with open(dest_path, 'wb') as dest:
			for part in sorted(part_paths, key=lambda p: p.name):
				with open(part, 'rb') as src:
					while True:
						chunk = src.read(65536)
						if not chunk:
							break
						dest.write(chunk)
	
	def iter_stream_chunks(self, stream: IO[bytes]) -> Generator[bytes, None, None]:
		"""Iterate over a stream in chunks."""
		while True:
			chunk = stream.read(self._chunk_size if self.enabled else 65536)
			if not chunk:
				break
			yield chunk
	
	@staticmethod
	def get_part_files(storage_dir: Path, file_hash: str) -> List[Path]:
		"""Find all part files for a given hash."""
		shard_a = file_hash[:2]
		shard_b = file_hash[2:4]
		blob_dir = storage_dir / shard_a / shard_b
		
		if not blob_dir.exists():
			return []
		
		# Check for single file first
		single = blob_dir / file_hash
		if single.exists():
			return [single]
		
		# Check for partitioned files
		parts = sorted(blob_dir.glob(f"{file_hash}.*"))
		return [p for p in parts if p.suffix.lstrip('.').isdigit()]
	
	@staticmethod
	def parse_part_info(filename: str) -> Tuple[str, int]:
		"""
		Parse a partition filename.
		Returns (base_hash, part_number) or (base_hash, 0) if not partitioned.
		"""
		if '.' in filename:
			parts = filename.rsplit('.', 1)
			if parts[1].isdigit():
				return parts[0], int(parts[1])
		return filename, 0