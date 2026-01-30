from .core import DLFI
from pathlib import Path
import extractors
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

class Job:
	config: 'JobConfig' = None
	db: 'DLFI' = None

	def __init__(self, config: 'JobConfig'):
		self.config = config

	def run(self, url: str, extr_config: dict = None) -> None:
		extractor = extractors.get_extractor_for_url(url)
		if not extractor:
			logger.error(f"No extractor found for URL: {url}")
			return
		
		extractor.load_cookies(self.config.cookies)
		
		logger.info(f"Starting extraction for: {url}")
		try:
			if extr_config is None:
				extr_config = extractor.default_config()
			
			for node in extractor.extract(url, extr_config):
				try:
					if node.node_type == "VAULT":
						self.db.create_vault(node.suggested_path, metadata=node.metadata)
					else:
						self.db.create_record(node.suggested_path, metadata=node.metadata)
					
					for tag in node.tags:
						self.db.add_tag(node.suggested_path, tag)
					
					for file_obj in node.files:
						try:
							logger.info(f"Downloading file: {file_obj.original_name}...")
							self.db.append_stream(
								record_path=node.suggested_path,
								file_stream=file_obj.stream,
								filename=file_obj.original_name
							)
						except Exception as e:
							# Catch stream/network/io errors for a SPECIFIC file
							# Log stack trace so we can debug, but continue to next file
							logger.error(f"Failed to ingest file {file_obj.original_name}: {e}", exc_info=True)
					
					# 4. Link Relationships
					for rel_name, target_path in node.relationships:
						try:
							self.db.link(node.suggested_path, target_path, rel_name)
						except ValueError as e:
							# Target might not exist yet if order is weird
							logger.warning(f"Could not link {node.suggested_path} -> {target_path}: {e}")

				except Exception as e:
					# Catch structural errors for a SPECIFIC node
					logger.error(f"Failed to process node {node.suggested_path}: {e}", exc_info=True)

		except Exception as e:
			# Catch fatal extractor errors (e.g. site is down, parsing logic broken)
			logger.critical(f"Fatal error during extraction job: {e}", exc_info=True)

@dataclass
class JobConfig():
	cookies: Path = None

	def __init__(self, cookies: str):
		self.cookies = Path(cookies).resolve()