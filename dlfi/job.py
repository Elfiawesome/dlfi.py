from .core import DLFI
from pathlib import Path
import extractors
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


class Job:
	config: 'JobConfig' = None
	db: 'DLFI' = None

	def __init__(self, config: 'JobConfig', db: 'DLFI' = None):
		self.config = config
		self.db = db

	def run(self, url: str, extr_config: dict = None) -> 'JobResult':
		job_result = JobResult(False)
		extractor = extractors.get_extractor_for_url(url)
		if not extractor:
			job_result.add_error(f"No extractor found for URL: {url}")
			return job_result
		
		if self.config.cookies:
			extractor.load_cookies(self.config.cookies)
		
		logger.info(f"Starting extraction for: {url}")
		try:
			if extr_config is None:
				extr_config = extractor.default_config()
			
			for node in extractor.extract(url, extr_config):
				try:
					if node.node_type == "VAULT":
						self.db.create_vault(node.suggested_path, metadata=node.metadata)
						job_result.new_vaults += 1
					else:
						self.db.create_record(node.suggested_path, metadata=node.metadata)
						job_result.new_records += 1
					
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
							job_result.new_files += 1
						except Exception as e:
							job_result.add_error(f"Failed to ingest file {file_obj.original_name}: {e}")
					
					for rel_name, target_path in node.relationships:
						try:
							self.db.link(node.suggested_path, target_path, rel_name)
						except ValueError as e:
							job_result.add_warning(f"Could not link {node.suggested_path} -> {target_path}: {e}")

				except Exception as e:
					job_result.add_error(f"Failed to process node {node.suggested_path}: {e}")

		except Exception as e:
			logger.critical(f"Fatal error during extraction job: {e}", exc_info=True)
			job_result.success = False
		
		if job_result.error_messages == 0:
			job_result.success = True

		return job_result


@dataclass
class JobConfig:
	cookies: Path = None

	def __init__(self, cookies: str = None):
		if cookies:
			self.cookies = Path(cookies).resolve()
		else:
			self.cookies = None

class JobResult:
	def __init__(self, success: bool):
		self.success = success
		self.new_vaults: int = 0
		self.new_records: int = 0
		self.new_files: int = 0
		self.error_messages: list[str] = []
	
	def add_error(self, error: str) -> None:
		logger.error(error, exc_info=True)
		self.error_messages.append(error)

	def add_warning(self, warn: str) -> None:
		logger.warning(warn)
