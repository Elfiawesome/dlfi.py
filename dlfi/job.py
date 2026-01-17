from .core import DLFI
import extractors
from dataclasses import dataclass

class Job:
	config: 'JobConfig' = None
	db: 'DLFI' = None

	def __init__(self, config: 'JobConfig'):
		self.config = config

	def run(self, url: str) -> None:
		extractor = extractors.get_extractor_for_url(url)
		extractor.load_cookies(self.config.cookies)
		
		for node in extractor.extract(url):
			if node.node_type == "VAULT":
				self.db.create_vault(node.suggested_path, metadata=node.metadata)
			else:
				self.db.create_record(node.suggested_path, metadata=node.metadata)
			
			for tag in node.tags:
				self.db.add_tag(node.suggested_path, tag)
			
			for file_obj in node.files:
				try:
					self.db.append_stream(
						record_path=node.suggested_path,
						file_stream=file_obj.stream,
						filename=file_obj.original_name
					)
				except Exception as e:
					pass
			
			for rel_name, target_path in node.relationships:
				try:
					self.db.link(node.suggested_path, target_path, rel_name)
				except ValueError as e:
					pass

@dataclass
class JobConfig():
	cookies: str = None