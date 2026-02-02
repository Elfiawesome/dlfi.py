import os
import json
import logging
import mimetypes
import io
import time
import re
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse, unquote
import threading

logger = logging.getLogger(__name__)


class DLFIServer:
	"""Web server for DLFI archive management."""
	
	def __init__(self, host: str = "127.0.0.1", port: int = 8080):
		self.host = host
		self.port = port
		self.dlfi = None
		self.archive_path = None
		self._server = None
		self._extraction_logs = []
		self._meta_keys_cache = None
		self._relations_cache = None
	
	def start(self, blocking: bool = True):
		handler = self._create_handler()
		self._server = HTTPServer((self.host, self.port), handler)
		logger.info(f"DLFI Server running at http://{self.host}:{self.port}")
		
		if blocking:
			try:
				self._server.serve_forever()
			except KeyboardInterrupt:
				logger.info("Server stopped by user")
		else:
			thread = threading.Thread(target=self._server.serve_forever)
			thread.daemon = True
			thread.start()
	
	def stop(self):
		if self._server:
			self._server.shutdown()
			if self.dlfi:
				self.dlfi.close()
	
	def open_archive(self, path: str, password: Optional[str] = None):
		from .core import DLFI
		if self.dlfi:
			self.dlfi.close()
		self.archive_path = Path(path).resolve()
		self.dlfi = DLFI(str(self.archive_path), password=password)
		self._meta_keys_cache = None
		self._relations_cache = None
		logger.info(f"Opened archive: {self.archive_path}")
	
	def close_archive(self):
		if self.dlfi:
			self.dlfi.close()
			self.dlfi = None
			self.archive_path = None
			self._meta_keys_cache = None
			self._relations_cache = None
	
	def _invalidate_caches(self):
		self._meta_keys_cache = None
		self._relations_cache = None
	
	def _get_all_metadata_keys(self) -> List[str]:
		if not self.dlfi:
			return []
		if self._meta_keys_cache is not None:
			return self._meta_keys_cache
		
		cursor = self.dlfi.conn.execute(
			"SELECT DISTINCT metadata FROM nodes WHERE metadata IS NOT NULL"
		)
		keys = set()
		for row in cursor:
			try:
				meta = json.loads(row[0])
				self._collect_keys(meta, "", keys)
			except:
				pass
		self._meta_keys_cache = sorted(keys)
		return self._meta_keys_cache
	
	def _collect_keys(self, obj: dict, prefix: str, keys: set):
		for k, v in obj.items():
			full_key = f"{prefix}.{k}" if prefix else k
			keys.add(full_key)
			if isinstance(v, dict):
				self._collect_keys(v, full_key, keys)
	
	def _get_metadata_values(self, key: str) -> List[str]:
		if not self.dlfi:
			return []
		cursor = self.dlfi.conn.execute(
			"SELECT DISTINCT metadata FROM nodes WHERE metadata IS NOT NULL"
		)
		values = set()
		key_parts = key.split('.')
		for row in cursor:
			try:
				meta = json.loads(row[0])
				val = meta
				for part in key_parts:
					if isinstance(val, dict) and part in val:
						val = val[part]
					else:
						val = None
						break
				if val is not None and isinstance(val, (str, int, float, bool)):
					values.add(str(val))
			except:
				pass
		return sorted(values)[:50]
	
	def _get_all_relations(self) -> List[str]:
		if not self.dlfi:
			return []
		if self._relations_cache is not None:
			return self._relations_cache
		
		cursor = self.dlfi.conn.execute("SELECT DISTINCT relation FROM edges ORDER BY relation")
		self._relations_cache = [r[0] for r in cursor]
		return self._relations_cache
	
	def _get_all_tags(self) -> List[str]:
		if not self.dlfi:
			return []
		cursor = self.dlfi.conn.execute("SELECT DISTINCT tag FROM tags ORDER BY tag")
		return [r[0] for r in cursor]
	
	def _create_handler(self):
		server = self
		
		class RequestHandler(BaseHTTPRequestHandler):
			def log_message(self, format, *args):
				logger.debug(f"{self.address_string()} - {format % args}")
			
			def send_json(self, data, status=200):
				self.send_response(status)
				self.send_header("Content-Type", "application/json")
				self.send_header("Access-Control-Allow-Origin", "*")
				self.end_headers()
				self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
			
			def send_error_json(self, message, status=400):
				self.send_json({"error": message}, status)
			
			def read_json_body(self) -> dict:
				length = int(self.headers.get("Content-Length", 0))
				if length == 0:
					return {}
				return json.loads(self.rfile.read(length).decode("utf-8"))
			
			def parse_multipart(self):
				content_type = self.headers.get("Content-Type", "")
				if "multipart/form-data" not in content_type:
					return {}, []
				
				boundary = None
				for part in content_type.split(";"):
					part = part.strip()
					if part.startswith("boundary="):
						boundary = part[9:].strip('"')
						break
				
				if not boundary:
					return {}, []
				
				length = int(self.headers.get("Content-Length", 0))
				body = self.rfile.read(length)
				boundary_bytes = f"--{boundary}".encode()
				parts = body.split(boundary_bytes)
				
				files = []
				fields = {}
				
				for part in parts[1:]:
					if part.strip() == b"--" or not part.strip():
						continue
					try:
						header_end = part.index(b"\r\n\r\n")
						headers_raw = part[:header_end].decode("utf-8", errors="ignore")
						content = part[header_end + 4:]
						if content.endswith(b"\r\n"):
							content = content[:-2]
						
						name = filename = None
						for line in headers_raw.split("\r\n"):
							if "Content-Disposition" in line:
								for item in line.split(";"):
									item = item.strip()
									if item.startswith("name="):
										name = item[5:].strip('"')
									elif item.startswith("filename="):
										filename = item[9:].strip('"')
						
						if filename:
							files.append({"name": name, "filename": filename, "data": content})
						elif name:
							fields[name] = content.decode("utf-8", errors="ignore")
					except:
						continue
				
				return fields, files
			
			def require_archive(self):
				if not server.dlfi:
					self.send_error_json("No archive open.", 400)
					return False
				return True
			
			def do_OPTIONS(self):
				self.send_response(200)
				self.send_header("Access-Control-Allow-Origin", "*")
				self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				self.send_header("Access-Control-Allow-Headers", "Content-Type")
				self.end_headers()
			
			def do_GET(self):
				parsed = urlparse(self.path)
				path = parsed.path
				query = parse_qs(parsed.query)
				
				if path == "/" or path == "/index.html":
					self.serve_html()
				elif path == "/api/status":
					self.api_status()
				elif path == "/api/config":
					self.api_get_config()
				elif path == "/api/extractors":
					self.api_list_extractors()
				elif path == "/api/tree":
					self.api_get_tree()
				elif path.startswith("/api/children/"):
					parent = path[14:] if len(path) > 14 else None
					self.api_get_children(parent if parent else None)
				elif path.startswith("/api/node/"):
					node_path = unquote(path[10:])
					self.api_get_node(node_path)
				elif path.startswith("/api/blob/"):
					blob_hash = path[10:]
					self.api_get_blob(blob_hash)
				elif path == "/api/autocomplete":
					context = query.get("context", [""])[0]
					q = query.get("q", [""])[0]
					self.api_smart_autocomplete(context, q)
				elif path == "/api/extraction-logs":
					self.send_json({"logs": server._extraction_logs[-100:]})
				else:
					self.send_error_json("Not found", 404)
			
			def do_POST(self):
				parsed = urlparse(self.path)
				path = parsed.path
				
				if path == "/api/archive/open":
					self.api_open_archive()
				elif path == "/api/archive/close":
					self.api_close_archive()
				elif path == "/api/archive/create":
					self.api_create_archive()
				elif path == "/api/vault":
					self.api_create_vault()
				elif path == "/api/record":
					self.api_create_record()
				elif path == "/api/upload":
					self.api_upload_file()
				elif path == "/api/tag":
					self.api_add_tag()
				elif path == "/api/link":
					self.api_create_link()
				elif path == "/api/smart-search":
					self.api_smart_search()
				elif path == "/api/query":
					self.api_query()
				elif path == "/api/extract":
					self.api_run_extractor()
				elif path == "/api/config/encryption":
					self.api_config_encryption()
				elif path == "/api/config/partition":
					self.api_config_partition()
				elif path == "/api/generate-static":
					self.api_generate_static()
				elif path == "/api/node/update":
					self.api_update_node()
				else:
					self.send_error_json("Not found", 404)
			
			def do_DELETE(self):
				parsed = urlparse(self.path)
				path = parsed.path
				
				if path.startswith("/api/node/"):
					node_path = unquote(path[10:])
					self.api_delete_node(node_path)
				elif path == "/api/tag":
					self.api_remove_tag()
				elif path == "/api/link":
					self.api_remove_link()
				else:
					self.send_error_json("Not found", 404)
			
			# === Smart Autocomplete ===
			
			def api_smart_autocomplete(self, context: str, query: str):
				if not self.require_archive():
					return
				
				try:
					suggestions = []
					q_lower = query.lower()
					
					if context == "start" or context == "key":
						# Suggest metadata keys, tag:, relations, and !
						meta_keys = server._get_all_metadata_keys()
						relations = server._get_all_relations()
						
						# Special prefixes
						prefixes = [
							{"value": "tag:", "label": "tag:", "type": "prefix", "hint": "filter by tag"},
							{"value": "!", "label": "!", "type": "prefix", "hint": "relationship query"},
							{"value": "^", "label": "^", "type": "prefix", "hint": "deep search (children inherit)"},
							{"value": "%", "label": "%", "type": "prefix", "hint": "reverse deep search (parents inherit)"},
						]
						
						for p in prefixes:
							if not query or p["value"].startswith(q_lower):
								suggestions.append(p)
						
						# Relations as direct search
						for rel in relations:
							if not query or q_lower in rel.lower():
								suggestions.append({
									"value": rel,
									"label": rel,
									"type": "relation",
									"hint": "has relationship type"
								})
						
						# Metadata keys
						for key in meta_keys:
							if not query or q_lower in key.lower():
								suggestions.append({
									"value": key,
									"label": key,
									"type": "key",
									"hint": "metadata key"
								})
						
						suggestions = suggestions[:25]
					
					elif context.startswith("tag"):
						# Suggest tags
						tags = server._get_all_tags()
						for tag in tags:
							if not query or q_lower in tag.lower():
								suggestions.append({
									"value": tag,
									"label": tag,
									"type": "tag",
									"hint": "tag"
								})
						suggestions = suggestions[:20]
					
					elif context.startswith("meta:"):
						# Suggest values for metadata key
						key = context[5:]
						if key.endswith(":") or key.endswith("="):
							key = key[:-1]
						
						values = server._get_metadata_values(key)
						for val in values:
							if not query or q_lower in val.lower():
								suggestions.append({
									"value": val,
									"label": val,
									"type": "value",
									"hint": f"value for {key}"
								})
						suggestions = suggestions[:20]
					
					elif context == "relation" or context == "rel":
						# Suggest relation names
						relations = server._get_all_relations()
						for rel in relations:
							if not query or q_lower in rel.lower():
								suggestions.append({
									"value": rel,
									"label": rel,
									"type": "relation",
									"hint": "relationship type"
								})
						suggestions = suggestions[:20]
					
					elif context == "path" or context == "!":
						# Suggest node paths
						cursor = server.dlfi.conn.execute(
							"SELECT cached_path FROM nodes ORDER BY cached_path LIMIT 100"
						)
						for r in cursor:
							if not query or q_lower in r[0].lower():
								suggestions.append({
									"value": r[0],
									"label": r[0],
									"type": "path",
									"hint": "node path"
								})
						suggestions = suggestions[:20]
					
					elif context == "!:":
						# After !path:, suggest relations
						relations = server._get_all_relations()
						for rel in relations:
							if not query or q_lower in rel.lower():
								suggestions.append({
									"value": rel,
									"label": rel,
									"type": "relation",
									"hint": "relationship type"
								})
						suggestions = suggestions[:20]
					
					self.send_json({"suggestions": suggestions})
				except Exception as e:
					logger.error(f"Autocomplete error: {e}")
					self.send_error_json(str(e), 500)
			
			# === Smart Search ===
			
			def api_smart_search(self):
				"""
				Parse and execute smart search query.
				
				Syntax:
				- tag:value - tag contains value
				- tag=value - tag equals value
				- key:value - metadata contains
				- key=value - metadata equals
				- key - metadata exists
				- RELATION - has this relationship type
				- !path - related to path (any direction)
				- !path:RELATION - related to path with relation
				- !path:RELATION> - outgoing to path
				- !path:RELATION< - incoming from path
				- ^... - deep search (children inherit)
				- %... - reverse deep search (parents inherit)
				"""
				if not self.require_archive():
					return
				
				try:
					body = self.read_json_body()
					query_str = body.get("q", "").strip()
					filters = body.get("filters", {})
					limit = body.get("limit", 200)
					
					parsed = self._parse_smart_query(query_str)
					
					# Build SQL
					base_select = """
						SELECT DISTINCT n.uuid, n.cached_path, n.type, n.name, n.metadata,
						(SELECT nf.file_hash FROM node_files nf 
						JOIN blobs b ON nf.file_hash = b.hash 
						WHERE nf.node_uuid = n.uuid AND b.ext IN ('jpg','jpeg','png','gif','webp','bmp','mp4','webm','mov')
						ORDER BY nf.display_order LIMIT 1) as preview_hash,
						(SELECT b.ext FROM node_files nf 
						JOIN blobs b ON nf.file_hash = b.hash 
						WHERE nf.node_uuid = n.uuid AND b.ext IN ('jpg','jpeg','png','gif','webp','bmp','mp4','webm','mov')
						ORDER BY nf.display_order LIMIT 1) as preview_ext
						FROM nodes n
					"""
					
					joins = []
					conditions = []
					params = []
					
					# Tag conditions (regular)
					for tag_cond in parsed["tags_contain"]:
						if tag_cond["deep"]:
							# Deep: children inherit parent tags
							conditions.append("""
								EXISTS (
									SELECT 1 FROM nodes ancestor
									JOIN tags at ON ancestor.uuid = at.node_uuid
									WHERE (n.cached_path = ancestor.cached_path OR n.cached_path LIKE ancestor.cached_path || '/%')
									AND at.tag LIKE ?
								)
							""")
							params.append(f"%{tag_cond['value'].lower()}%")
						elif tag_cond["reverse_deep"]:
							# Reverse deep: parents inherit children tags
							conditions.append("""
								EXISTS (
									SELECT 1 FROM nodes descendant
									JOIN tags dt ON descendant.uuid = dt.node_uuid
									WHERE (descendant.cached_path = n.cached_path OR descendant.cached_path LIKE n.cached_path || '/%')
									AND dt.tag LIKE ?
								)
							""")
							params.append(f"%{tag_cond['value'].lower()}%")
						else:
							if "tags t" not in " ".join(joins):
								joins.append("LEFT JOIN tags t ON n.uuid = t.node_uuid")
							conditions.append("t.tag LIKE ?")
							params.append(f"%{tag_cond['value'].lower()}%")
					
					for tag_cond in parsed["tags_eq"]:
						if tag_cond["deep"]:
							conditions.append("""
								EXISTS (
									SELECT 1 FROM nodes ancestor
									JOIN tags at ON ancestor.uuid = at.node_uuid
									WHERE (n.cached_path = ancestor.cached_path OR n.cached_path LIKE ancestor.cached_path || '/%')
									AND at.tag = ?
								)
							""")
							params.append(tag_cond['value'].lower())
						elif tag_cond["reverse_deep"]:
							conditions.append("""
								EXISTS (
									SELECT 1 FROM nodes descendant
									JOIN tags dt ON descendant.uuid = dt.node_uuid
									WHERE (descendant.cached_path = n.cached_path OR descendant.cached_path LIKE n.cached_path || '/%')
									AND dt.tag = ?
								)
							""")
							params.append(tag_cond['value'].lower())
						else:
							if "tags t" not in " ".join(joins):
								joins.append("LEFT JOIN tags t ON n.uuid = t.node_uuid")
							conditions.append("t.tag = ?")
							params.append(tag_cond['value'].lower())
					
					# Metadata conditions
					for key, value in parsed["meta_contain"].items():
						json_path = key
						conditions.append(f"LOWER(json_extract(n.metadata, '$.{json_path}')) LIKE ?")
						params.append(f"%{value.lower()}%")
					
					for key, value in parsed["meta_eq"].items():
						json_path = key
						try:
							num_val = float(value)
							conditions.append(f"json_extract(n.metadata, '$.{json_path}') = ?")
							params.append(num_val if num_val != int(num_val) else int(num_val))
						except ValueError:
							conditions.append(f"json_extract(n.metadata, '$.{json_path}') = ?")
							params.append(value)
					
					for key in parsed["meta_exists"]:
						json_path = key
						conditions.append(f"json_extract(n.metadata, '$.{json_path}') IS NOT NULL")
					
					# Relationship type conditions
					for rel_cond in parsed["has_relation"]:
						if rel_cond["deep"]:
							conditions.append("""
								EXISTS (
									SELECT 1 FROM nodes ancestor
									JOIN edges ae ON (ancestor.uuid = ae.source_uuid OR ancestor.uuid = ae.target_uuid)
									WHERE (n.cached_path = ancestor.cached_path OR n.cached_path LIKE ancestor.cached_path || '/%')
									AND ae.relation = ?
								)
							""")
							params.append(rel_cond['value'].upper())
						elif rel_cond["reverse_deep"]:
							conditions.append("""
								EXISTS (
									SELECT 1 FROM nodes descendant
									JOIN edges de ON (descendant.uuid = de.source_uuid OR descendant.uuid = de.target_uuid)
									WHERE (descendant.cached_path = n.cached_path OR descendant.cached_path LIKE n.cached_path || '/%')
									AND de.relation = ?
								)
							""")
							params.append(rel_cond['value'].upper())
						else:
							joins.append("JOIN edges er ON (n.uuid = er.source_uuid OR n.uuid = er.target_uuid)")
							conditions.append("er.relation = ?")
							params.append(rel_cond['value'].upper())
					
					# Related to path conditions
					for rel in parsed["related_to"]:
						target_path = rel["path"]
						relation = rel["relation"]
						direction = rel["direction"]  # None, '>', '<'
						deep = rel["deep"]
						reverse_deep = rel["reverse_deep"]
						
						# Resolve target UUID
						target_uuid = server.dlfi._resolve_path(target_path)
						if not target_uuid:
							conditions.append("1=0")  # No results
							continue
						
						if deep:
							# Children inherit: find nodes whose ancestors are related to target
							subquery = """
								EXISTS (
									SELECT 1 FROM nodes ancestor
									JOIN edges ae ON 
							"""
							if direction == '>':
								subquery += "ancestor.uuid = ae.source_uuid AND ae.target_uuid = ?"
							elif direction == '<':
								subquery += "ancestor.uuid = ae.target_uuid AND ae.source_uuid = ?"
							else:
								subquery += "(ancestor.uuid = ae.source_uuid AND ae.target_uuid = ?) OR (ancestor.uuid = ae.target_uuid AND ae.source_uuid = ?)"
							
							subquery += """
									WHERE (n.cached_path = ancestor.cached_path OR n.cached_path LIKE ancestor.cached_path || '/%')
							"""
							
							if direction is None:
								params.extend([target_uuid, target_uuid])
							else:
								params.append(target_uuid)
							
							if relation:
								subquery += " AND ae.relation = ?"
								params.append(relation.upper())
							
							subquery += ")"
							conditions.append(subquery)
						
						elif reverse_deep:
							# Parents inherit: find nodes whose descendants are related to target
							subquery = """
								EXISTS (
									SELECT 1 FROM nodes descendant
									JOIN edges de ON 
							"""
							if direction == '>':
								subquery += "descendant.uuid = de.source_uuid AND de.target_uuid = ?"
							elif direction == '<':
								subquery += "descendant.uuid = de.target_uuid AND de.source_uuid = ?"
							else:
								subquery += "(descendant.uuid = de.source_uuid AND de.target_uuid = ?) OR (descendant.uuid = de.target_uuid AND de.source_uuid = ?)"
							
							subquery += """
									WHERE (descendant.cached_path = n.cached_path OR descendant.cached_path LIKE n.cached_path || '/%')
							"""
							
							if direction is None:
								params.extend([target_uuid, target_uuid])
							else:
								params.append(target_uuid)
							
							if relation:
								subquery += " AND de.relation = ?"
								params.append(relation.upper())
							
							subquery += ")"
							conditions.append(subquery)
						
						else:
							# Direct relationship
							alias = f"e_rel_{len(joins)}"
							if direction == '>':
								joins.append(f"JOIN edges {alias} ON n.uuid = {alias}.source_uuid AND {alias}.target_uuid = ?")
								params.append(target_uuid)
							elif direction == '<':
								joins.append(f"JOIN edges {alias} ON n.uuid = {alias}.target_uuid AND {alias}.source_uuid = ?")
								params.append(target_uuid)
							else:
								joins.append(f"JOIN edges {alias} ON (n.uuid = {alias}.source_uuid AND {alias}.target_uuid = ?) OR (n.uuid = {alias}.target_uuid AND {alias}.source_uuid = ?)")
								params.extend([target_uuid, target_uuid])
							
							if relation:
								conditions.append(f"{alias}.relation = ?")
								params.append(relation.upper())
					
					# Full text search
					if parsed["text"]:
						text_query = " ".join(parsed["text"]).lower()
						if "tags t" not in " ".join(joins):
							joins.append("LEFT JOIN tags t ON n.uuid = t.node_uuid")
						conditions.append("""(
							LOWER(n.cached_path) LIKE ? OR
							LOWER(n.name) LIKE ? OR
							LOWER(n.metadata) LIKE ? OR
							t.tag LIKE ?
						)""")
						params.extend([f"%{text_query}%"] * 4)
					
					# Additional filters
					if filters.get("type"):
						conditions.append("n.type = ?")
						params.append(filters["type"])
					
					if filters.get("inside"):
						conditions.append("n.cached_path LIKE ?")
						params.append(f"{filters['inside'].strip('/')}/%")
					
					# Build final SQL
					sql = base_select
					if joins:
						sql += " " + " ".join(joins)
					if conditions:
						sql += " WHERE " + " AND ".join(conditions)
					sql += " ORDER BY n.cached_path LIMIT ?"
					params.append(limit)
					
					# Execute
					cursor = server.dlfi.conn.execute(sql, params)
					
					results = []
					for row in cursor:
						tags_cursor = server.dlfi.conn.execute(
							"SELECT tag FROM tags WHERE node_uuid = ?", (row[0],)
						)
						tags = [t[0] for t in tags_cursor]
						
						results.append({
							"uuid": row[0],
							"path": row[1],
							"type": row[2],
							"name": row[3],
							"metadata": json.loads(row[4]) if row[4] else {},
							"tags": tags,
							"preview_hash": row[5],
							"preview_ext": row[6]
						})
					
					self.send_json({"results": results, "count": len(results)})
					
				except Exception as e:
					logger.error(f"Smart search error: {e}", exc_info=True)
					self.send_error_json(str(e), 500)
			
			def _parse_smart_query(self, query: str) -> dict:
				"""Parse the smart search query string."""
				result = {
					"text": [],
					"tags_contain": [],  # [{"value": x, "deep": bool, "reverse_deep": bool}]
					"tags_eq": [],
					"meta_contain": {},
					"meta_eq": {},
					"meta_exists": [],
					"has_relation": [],  # [{"value": x, "deep": bool, "reverse_deep": bool}]
					"related_to": []  # [{"path": x, "relation": y, "direction": '>'/'<'/None, "deep": bool, "reverse_deep": bool}]
				}
				
				if not query:
					return result
				
				# Tokenize respecting quotes
				tokens = []
				current = ""
				in_quotes = False
				
				for char in query:
					if char == '"':
						in_quotes = not in_quotes
					elif char == ' ' and not in_quotes:
						if current:
							tokens.append(current)
							current = ""
					else:
						current += char
				
				if current:
					tokens.append(current)
				
				meta_keys = server._get_all_metadata_keys()
				relations = server._get_all_relations()
				
				for token in tokens:
					token = token.strip()
					if not token:
						continue
					
					# Check for deep/reverse deep modifiers
					deep = False
					reverse_deep = False
					
					if token.startswith("^"):
						deep = True
						token = token[1:]
					elif token.startswith("%"):
						reverse_deep = True
						token = token[1:]
					
					if not token:
						continue
					
					# Relationship query: !path or !path:RELATION or !path:RELATION> or !path:RELATION<
					if token.startswith("!"):
						rel_query = token[1:]
						direction = None
						
						if rel_query.endswith(">"):
							direction = ">"
							rel_query = rel_query[:-1]
						elif rel_query.endswith("<"):
							direction = "<"
							rel_query = rel_query[:-1]
						
						if ":" in rel_query:
							path, relation = rel_query.split(":", 1)
						else:
							path = rel_query
							relation = None
						
						result["related_to"].append({
							"path": path,
							"relation": relation,
							"direction": direction,
							"deep": deep,
							"reverse_deep": reverse_deep
						})
					
					# Tag queries
					elif token.lower().startswith("tag:"):
						result["tags_contain"].append({
							"value": token[4:],
							"deep": deep,
							"reverse_deep": reverse_deep
						})
					elif token.lower().startswith("tag="):
						result["tags_eq"].append({
							"value": token[4:],
							"deep": deep,
							"reverse_deep": reverse_deep
						})
					
					# Metadata with operator
					elif ":" in token:
						key, value = token.split(":", 1)
						if key and value:
							result["meta_contain"][key] = value
						elif key:
							result["meta_exists"].append(key)
					
					elif "=" in token:
						key, value = token.split("=", 1)
						if key and value:
							result["meta_eq"][key] = value
						elif key:
							result["meta_exists"].append(key)
					
					else:
						# Could be: relation name, metadata key, or plain text
						token_upper = token.upper()
						
						if token_upper in relations:
							result["has_relation"].append({
								"value": token_upper,
								"deep": deep,
								"reverse_deep": reverse_deep
							})
						elif token in meta_keys or "." in token:
							result["meta_exists"].append(token)
						else:
							# Check if it's a known relation (case insensitive)
							matched_rel = None
							for rel in relations:
								if rel.lower() == token.lower():
									matched_rel = rel
									break
							
							if matched_rel:
								result["has_relation"].append({
									"value": matched_rel,
									"deep": deep,
									"reverse_deep": reverse_deep
								})
							else:
								result["text"].append(token)
					
				return result
			
			# === Archive Management ===
			
			def api_status(self):
				data = {
					"archive_open": server.dlfi is not None,
					"archive_path": str(server.archive_path) if server.archive_path else None
				}
				if server.dlfi:
					data["encrypted"] = server.dlfi.config.encrypted
					data["partition_size"] = server.dlfi.config.partition_size
					
					stats = {}
					stats["nodes"] = server.dlfi.conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
					stats["blobs"] = server.dlfi.conn.execute("SELECT COUNT(*) FROM blobs").fetchone()[0]
					stats["total_size"] = server.dlfi.conn.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM blobs").fetchone()[0]
					stats["tags"] = server.dlfi.conn.execute("SELECT COUNT(DISTINCT tag) FROM tags").fetchone()[0]
					stats["relationships"] = server.dlfi.conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
					data["stats"] = stats
				
				self.send_json(data)
			
			def api_open_archive(self):
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					password = body.get("password")
					if not path:
						self.send_error_json("Path is required")
						return
					server.open_archive(path, password)
					self.send_json({"success": True, "path": str(server.archive_path)})
				except Exception as e:
					logger.error(f"Failed to open archive: {e}")
					self.send_error_json(str(e), 500)
			
			def api_create_archive(self):
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					password = body.get("password")
					partition_size = body.get("partition_size", 50 * 1024 * 1024)
					if not path:
						self.send_error_json("Path is required")
						return
					server.open_archive(path, password)
					if partition_size != server.dlfi.config.partition_size:
						server.dlfi.config.partition_size = partition_size
						server.dlfi.config.save(server.dlfi.config_path)
					self.send_json({"success": True, "path": str(server.archive_path)})
				except Exception as e:
					logger.error(f"Failed to create archive: {e}")
					self.send_error_json(str(e), 500)
			
			def api_close_archive(self):
				server.close_archive()
				self.send_json({"success": True})
			
			def api_get_config(self):
				if not self.require_archive():
					return
				try:
					config = {
						"path": str(server.archive_path),
						"encrypted": server.dlfi.config.encrypted,
						"partition_size": server.dlfi.config.partition_size,
						"version": server.dlfi.config.version
					}
					self.send_json(config)
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			# === Extractors ===
			
			def api_list_extractors(self):
				try:
					import extractors
					result = []
					for ext in extractors.AVAILABLE_EXTRACTORS:
						result.append({
							"name": ext.name,
							"slug": getattr(ext, 'slug', ext.name.lower()),
							"default_config": ext.default_config()
						})
					self.send_json({"extractors": result})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_run_extractor(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					url = body.get("url", "").strip()
					cookies_content = body.get("cookies", "")
					extractor_config = body.get("config", {})
					
					if not url:
						self.send_error_json("URL is required")
						return
					
					import extractors
					from .job import Job, JobConfig
					import tempfile
					
					extractor = extractors.get_extractor_for_url(url)
					if not extractor:
						self.send_error_json(f"No extractor found for URL: {url}")
						return
					
					cookie_file = None
					if cookies_content:
						tf = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False)
						tf.write(cookies_content)
						tf.close()
						cookie_file = tf.name
					
					try:
						job = Job(JobConfig(cookie_file))
						job.db = server.dlfi
						server._extraction_logs.append(f"[START] {url}")
						job.run(url, extractor_config if extractor_config else None)
						server._extraction_logs.append(f"[DONE] {url}")
						server._invalidate_caches()
						self.send_json({"success": True})
					finally:
						if cookie_file and os.path.exists(cookie_file):
							os.remove(cookie_file)
				
				except Exception as e:
					server._extraction_logs.append(f"[ERROR] {str(e)}")
					logger.error(f"Extraction failed: {e}", exc_info=True)
					self.send_error_json(str(e), 500)
			
			# === Tree & Navigation ===
			
			def api_get_tree(self):
				if not self.require_archive():
					return
				try:
					cursor = server.dlfi.conn.execute(
						"SELECT uuid, parent_uuid, type, name, cached_path FROM nodes ORDER BY cached_path"
					)
					nodes = [{"uuid": r[0], "parent": r[1], "type": r[2], "name": r[3], "path": r[4]} for r in cursor]
					self.send_json({"nodes": nodes})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_get_children(self, parent_uuid: Optional[str]):
				if not self.require_archive():
					return
				try:
					if parent_uuid in ("null", ""):
						parent_uuid = None
					
					cursor = server.dlfi.conn.execute(
						"SELECT uuid, type, name, cached_path FROM nodes WHERE parent_uuid IS ? ORDER BY type DESC, name",
						(parent_uuid,)
					)
					
					children = []
					for row in cursor:
						count = server.dlfi.conn.execute(
							"SELECT COUNT(*) FROM nodes WHERE parent_uuid = ?", (row[0],)
						).fetchone()[0]
						children.append({"uuid": row[0], "type": row[1], "name": row[2], "path": row[3], "hasChildren": count > 0})
					
					self.send_json({"children": children})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_get_node(self, node_path: str):
				if not self.require_archive():
					return
				try:
					cursor = server.dlfi.conn.execute(
						"SELECT uuid, parent_uuid, type, name, cached_path, metadata, created_at, last_modified FROM nodes WHERE cached_path = ?",
						(node_path,)
					)
					
					row = cursor.fetchone()
					if not row:
						self.send_error_json("Node not found", 404)
						return
					
					node = {
						"uuid": row[0], "parent": row[1], "type": row[2], "name": row[3],
						"path": row[4], "metadata": json.loads(row[5]) if row[5] else {},
						"created_at": row[6], "last_modified": row[7]
					}
					
					node["tags"] = [r[0] for r in server.dlfi.conn.execute(
						"SELECT tag FROM tags WHERE node_uuid = ?", (row[0],)
					)]
					
					node["relationships"] = [
						{"relation": r[0], "target_path": r[1], "target_uuid": r[2]}
						for r in server.dlfi.conn.execute(
							"SELECT e.relation, n.cached_path, e.target_uuid FROM edges e LEFT JOIN nodes n ON e.target_uuid = n.uuid WHERE e.source_uuid = ?",
							(row[0],)
						)
					]
					
					node["incoming_relationships"] = [
						{"relation": r[0], "source_path": r[1], "source_uuid": r[2]}
						for r in server.dlfi.conn.execute(
							"SELECT e.relation, n.cached_path, e.source_uuid FROM edges e LEFT JOIN nodes n ON e.source_uuid = n.uuid WHERE e.target_uuid = ?",
							(row[0],)
						)
					]
					
					node["files"] = [
						{"name": r[0], "hash": r[1], "size": r[2], "ext": r[3], "parts": r[4]}
						for r in server.dlfi.conn.execute(
							"SELECT nf.original_name, nf.file_hash, b.size_bytes, b.ext, b.part_count FROM node_files nf JOIN blobs b ON nf.file_hash = b.hash WHERE nf.node_uuid = ? ORDER BY nf.display_order",
							(row[0],)
						)
					]
					
					node["children_count"] = server.dlfi.conn.execute(
						"SELECT COUNT(*) FROM nodes WHERE parent_uuid = ?", (row[0],)
					).fetchone()[0]
					
					self.send_json(node)
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_get_blob(self, blob_hash: str):
				if not self.require_archive():
					return
				try:
					data = server.dlfi.read_blob(blob_hash)
					if data is None:
						self.send_error_json("Blob not found", 404)
						return
					
					ext = server.dlfi.conn.execute("SELECT ext FROM blobs WHERE hash = ?", (blob_hash,)).fetchone()
					ext = ext[0] if ext else "bin"
					
					mime = mimetypes.guess_type(f"file.{ext}")[0] or "application/octet-stream"
					
					self.send_response(200)
					self.send_header("Content-Type", mime)
					self.send_header("Content-Length", len(data))
					self.send_header("Access-Control-Allow-Origin", "*")
					self.send_header("Cache-Control", "public, max-age=31536000")
					self.end_headers()
					self.wfile.write(data)
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			# === CRUD Operations ===
			
			def api_create_vault(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					metadata = body.get("metadata", {})
					if not path:
						self.send_error_json("Path is required")
						return
					uuid = server.dlfi.create_vault(path, metadata=metadata if metadata else None)
					server._invalidate_caches()
					self.send_json({"uuid": uuid, "path": path})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_create_record(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					metadata = body.get("metadata", {})
					if not path:
						self.send_error_json("Path is required")
						return
					uuid = server.dlfi.create_record(path, metadata=metadata if metadata else None)
					server._invalidate_caches()
					self.send_json({"uuid": uuid, "path": path})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_upload_file(self):
				if not self.require_archive():
					return
				try:
					fields, files = self.parse_multipart()
					if not fields or not files:
						self.send_error_json("Multipart form required")
						return
					record_path = fields.get("path", "").strip()
					if not record_path:
						self.send_error_json("Path required")
						return
					uploaded = []
					for f in files:
						stream = io.BytesIO(f["data"])
						server.dlfi.append_stream(record_path, stream, f["filename"])
						uploaded.append(f["filename"])
					self.send_json({"uploaded": uploaded})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_add_tag(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					tag = body.get("tag", "").strip()
					if not path or not tag:
						self.send_error_json("Path and tag required")
						return
					server.dlfi.add_tag(path, tag)
					self.send_json({"success": True})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_remove_tag(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					tag = body.get("tag", "").strip()
					if not path or not tag:
						self.send_error_json("Path and tag required")
						return
					uuid = server.dlfi._resolve_path(path)
					if uuid:
						with server.dlfi.conn:
							server.dlfi.conn.execute("DELETE FROM tags WHERE node_uuid = ? AND tag = ?", (uuid, tag.lower()))
					self.send_json({"success": True})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_create_link(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					source = body.get("source", "").strip()
					target = body.get("target", "").strip()
					relation = body.get("relation", "").strip()
					if not source or not target or not relation:
						self.send_error_json("Source, target, and relation required")
						return
					server.dlfi.link(source, target, relation)
					server._invalidate_caches()
					self.send_json({"success": True})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_remove_link(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					source = body.get("source", "").strip()
					target = body.get("target", "").strip()
					relation = body.get("relation", "").strip()
					if not source or not target or not relation:
						self.send_error_json("Source, target, and relation required")
						return
					src_uuid = server.dlfi._resolve_path(source)
					tgt_uuid = server.dlfi._resolve_path(target)
					if src_uuid and tgt_uuid:
						with server.dlfi.conn:
							server.dlfi.conn.execute(
								"DELETE FROM edges WHERE source_uuid = ? AND target_uuid = ? AND relation = ?",
								(src_uuid, tgt_uuid, relation.upper())
							)
					server._invalidate_caches()
					self.send_json({"success": True})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_update_node(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					metadata = body.get("metadata")
					if not path:
						self.send_error_json("Path required")
						return
					
					cursor = server.dlfi.conn.execute("SELECT uuid FROM nodes WHERE cached_path = ?", (path,))
					row = cursor.fetchone()
					if not row:
						self.send_error_json("Not found", 404)
						return
					
					uuid = row[0]
					if metadata is not None:
						with server.dlfi.conn:
							server.dlfi.conn.execute(
								"UPDATE nodes SET metadata = ?, last_modified = ? WHERE uuid = ?",
								(json.dumps(metadata) if metadata else None, time.time(), uuid)
							)
					server._invalidate_caches()
					self.send_json({"success": True, "uuid": uuid})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_delete_node(self, node_path: str):
				if not self.require_archive():
					return
				try:
					uuid = server.dlfi._resolve_path(node_path)
					if not uuid:
						self.send_error_json("Not found", 404)
						return
					with server.dlfi.conn:
						server.dlfi.conn.execute("DELETE FROM nodes WHERE uuid = ?", (uuid,))
					server._invalidate_caches()
					self.send_json({"success": True})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			# === Legacy Query ===
			
			def api_query(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					qb = server.dlfi.query()
					
					if body.get("inside"):
						qb.inside(body["inside"])
					if body.get("type"):
						qb.type(body["type"])
					if body.get("has_tag"):
						tags = body["has_tag"] if isinstance(body["has_tag"], list) else [body["has_tag"]]
						for tag in tags:
							qb.has_tag(tag)
					if body.get("meta_eq"):
						for key, value in body["meta_eq"].items():
							qb.meta_eq(key, value)
					if body.get("meta_contains"):
						for key, value in body["meta_contains"].items():
							qb.meta_contains(key, value)
					if body.get("meta_exists"):
						keys = body["meta_exists"] if isinstance(body["meta_exists"], list) else [body["meta_exists"]]
						for key in keys:
							qb.meta_exists(key)
					if body.get("related_to"):
						rel = body["related_to"]
						if isinstance(rel, dict):
							qb.related_to(rel.get("target", ""), rel.get("relation"))
						else:
							qb.related_to(rel)
					if body.get("limit"):
						qb.limit(int(body["limit"]))
					
					results = qb.execute()
					self.send_json({"results": results, "count": len(results)})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			# === Config ===
			
			def api_config_encryption(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					action = body.get("action", "")
					
					if action == "enable":
						password = body.get("password", "")
						if not password:
							self.send_error_json("Password required")
							return
						success = server.dlfi.config_manager.enable_encryption(password)
						self.send_json({"success": success})
					elif action == "disable":
						password = body.get("password", "")
						if not password:
							self.send_error_json("Password required")
							return
						success = server.dlfi.config_manager.disable_encryption(password)
						self.send_json({"success": success})
					elif action == "change_password":
						old = body.get("old_password", "")
						new = body.get("new_password", "")
						if not old or not new:
							self.send_error_json("Both passwords required")
							return
						success = server.dlfi.config_manager.change_password(old, new)
						self.send_json({"success": success})
					else:
						self.send_error_json("Invalid action")
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_config_partition(self):
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					size = body.get("size")
					if size is None:
						self.send_error_json("Size required")
						return
					success = server.dlfi.config_manager.change_partition_size(int(size))
					self.send_json({"success": success})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_generate_static(self):
				if not self.require_archive():
					return
				try:
					server.dlfi.generate_static_site()
					self.send_json({"success": True, "path": str(server.archive_path / "index.html")})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def serve_html(self):
				html = get_app_html()
				self.send_response(200)
				self.send_header("Content-Type", "text/html; charset=utf-8")
				self.end_headers()
				self.wfile.write(html.encode("utf-8"))
		
		return RequestHandler


def get_app_html() -> str:
	return '''<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>DLFI Archive Manager</title>
	<style>
		* { margin: 0; padding: 0; box-sizing: border-box; }
		:root {
			--bg-0: #000; --bg-1: #0a0a0a; --bg-2: #121212; --bg-3: #1a1a1a; --bg-4: #222; --bg-5: #2a2a2a;
			--text-0: #fff; --text-1: #ccc; --text-2: #888; --text-3: #555;
			--accent: #3b82f6; --accent-dim: #1e40af;
			--success: #22c55e; --warning: #f59e0b; --error: #ef4444;
			--border: #2a2a2a;
		}
		body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; background: var(--bg-1); color: var(--text-0); height: 100vh; overflow: hidden; }
		.app { display: flex; flex-direction: column; height: 100vh; }
		
		.header { background: var(--bg-0); border-bottom: 1px solid var(--border); padding: 0 20px; height: 52px; display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }
		.header-left { display: flex; align-items: center; gap: 20px; }
		.logo { font-weight: 700; font-size: 1rem; }
		.archive-info { display: flex; align-items: center; gap: 12px; font-size: 0.8rem; color: var(--text-2); }
		.archive-info .path { color: var(--text-1); max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.archive-badge { padding: 2px 8px; font-size: 0.65rem; text-transform: uppercase; background: var(--bg-3); }
		.archive-badge.encrypted { background: var(--accent-dim); color: var(--accent); }
		.header-actions { display: flex; gap: 8px; }
		
		.main { display: flex; flex: 1; overflow: hidden; }
		
		.sidebar { width: 220px; background: var(--bg-2); border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
		.sidebar-header { padding: 12px 16px; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-2); display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); }
		.tree-container { flex: 1; overflow-y: auto; padding: 4px 0; }
		.tree-item { display: flex; align-items: center; padding: 5px 12px; cursor: pointer; font-size: 0.8rem; color: var(--text-1); }
		.tree-item:hover { background: var(--bg-3); }
		.tree-item.selected { background: var(--accent); color: white; }
		.tree-toggle { width: 14px; height: 14px; font-size: 8px; display: flex; align-items: center; justify-content: center; color: var(--text-3); margin-right: 4px; transition: transform 0.1s; }
		.tree-toggle.expanded { transform: rotate(90deg); }
		.tree-toggle.hidden { visibility: hidden; }
		.tree-icon { margin-right: 6px; font-size: 12px; }
		.tree-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.tree-children { display: none; }
		.tree-children.expanded { display: block; }
		
		.content { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
		
		.search-container { padding: 16px 24px; background: var(--bg-2); border-bottom: 1px solid var(--border); }
		.search-row { display: flex; gap: 12px; align-items: stretch; }
		.search-input-wrap { flex: 1; position: relative; }
		.search-input { width: 100%; padding: 12px 16px; background: var(--bg-1); border: 1px solid var(--border); color: var(--text-0); font-size: 0.9rem; outline: none; }
		.search-input:focus { border-color: var(--accent); }
		.search-input::placeholder { color: var(--text-3); }
		
		.search-hints { font-size: 0.65rem; color: var(--text-3); margin-top: 10px; line-height: 1.8; }
		.search-hints code { background: var(--bg-3); padding: 2px 5px; font-family: monospace; margin-right: 6px; }
		.search-hints-row { margin-bottom: 2px; }
		
		.autocomplete-dropdown { position: absolute; top: 100%; left: 0; right: 0; background: var(--bg-3); border: 1px solid var(--border); border-top: none; max-height: 280px; overflow-y: auto; z-index: 100; display: none; }
		.autocomplete-dropdown.show { display: block; }
		.autocomplete-item { padding: 8px 12px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; font-size: 0.85rem; }
		.autocomplete-item:hover, .autocomplete-item.selected { background: var(--bg-4); }
		.autocomplete-item-hint { font-size: 0.65rem; color: var(--text-3); }
		
		.filter-bar { display: flex; gap: 8px; margin-top: 10px; flex-wrap: wrap; align-items: center; }
		.filter-chip { display: flex; align-items: center; gap: 6px; padding: 4px 10px; background: var(--bg-3); border: 1px solid var(--border); font-size: 0.7rem; color: var(--text-1); }
		.filter-chip .remove { cursor: pointer; opacity: 0.5; }
		.filter-chip .remove:hover { opacity: 1; }
		.filter-add { padding: 4px 10px; background: transparent; border: 1px dashed var(--border); font-size: 0.7rem; color: var(--text-2); cursor: pointer; }
		.filter-add:hover { border-color: var(--text-2); color: var(--text-1); }
		
		.results-container { flex: 1; overflow-y: auto; padding: 16px 24px; }
		.results-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; font-size: 0.75rem; color: var(--text-2); }
		.results-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 12px; }
		
		.result-card { background: var(--bg-2); border: 1px solid var(--border); cursor: pointer; transition: border-color 0.1s; display: flex; overflow: hidden; }
		.result-card:hover { border-color: var(--accent); }
		.result-preview { width: 80px; height: 80px; flex-shrink: 0; background: var(--bg-3); display: flex; align-items: center; justify-content: center; overflow: hidden; }
		.result-preview img, .result-preview video { width: 100%; height: 100%; object-fit: cover; }
		.result-preview-icon { font-size: 1.5rem; color: var(--text-3); }
		.result-body { flex: 1; padding: 10px 12px; min-width: 0; display: flex; flex-direction: column; }
		.result-header { display: flex; align-items: flex-start; gap: 8px; margin-bottom: 4px; }
		.result-name { font-weight: 500; font-size: 0.85rem; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.result-type { font-size: 0.55rem; text-transform: uppercase; padding: 2px 5px; background: var(--bg-3); color: var(--text-2); flex-shrink: 0; }
		.result-path { font-size: 0.7rem; color: var(--text-2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; margin-bottom: 6px; }
		.result-meta { display: flex; flex-wrap: wrap; gap: 4px; }
		.result-tag { font-size: 0.6rem; padding: 1px 6px; background: var(--bg-4); color: var(--text-2); }
		.result-tag.is-tag { background: var(--accent-dim); color: var(--accent); }
		
		.detail-panel { width: 380px; background: var(--bg-2); border-left: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; overflow: hidden; }
		.detail-panel.hidden { display: none; }
		.detail-header { padding: 14px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: flex-start; }
		.detail-title { font-size: 0.95rem; font-weight: 600; word-break: break-word; }
		.detail-close { background: none; border: none; color: var(--text-2); font-size: 1.1rem; cursor: pointer; }
		.detail-body { flex: 1; overflow-y: auto; padding: 14px; }
		.detail-section { margin-bottom: 20px; }
		.detail-section-title { font-size: 0.6rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-2); margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; }
		
		.detail-meta-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
		.detail-meta-item { background: var(--bg-3); padding: 8px; }
		.detail-meta-item.full { grid-column: span 2; }
		.detail-meta-label { font-size: 0.6rem; color: var(--text-2); margin-bottom: 3px; text-transform: uppercase; }
		.detail-meta-value { font-size: 0.8rem; word-break: break-word; }
		
		.detail-tags { display: flex; flex-wrap: wrap; gap: 5px; }
		.detail-tag { display: flex; align-items: center; gap: 4px; padding: 3px 8px; background: var(--bg-3); font-size: 0.7rem; color: var(--text-1); }
		.detail-tag .remove { cursor: pointer; opacity: 0.5; font-size: 0.8rem; }
		.detail-tag .remove:hover { opacity: 1; }
		
		.detail-rel-item { display: flex; align-items: center; gap: 8px; padding: 8px; background: var(--bg-3); margin-bottom: 5px; cursor: pointer; font-size: 0.75rem; }
		.detail-rel-item:hover { background: var(--bg-4); }
		.detail-rel-type { font-size: 0.6rem; text-transform: uppercase; color: var(--accent); font-weight: 600; min-width: 70px; }
		.detail-rel-path { color: var(--text-1); flex: 1; word-break: break-all; }
		.detail-rel-dir { font-size: 0.55rem; color: var(--text-3); }
		.detail-rel-remove { cursor: pointer; color: var(--text-3); font-size: 0.75rem; }
		.detail-rel-remove:hover { color: var(--error); }
		
		.detail-files-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; }
		.detail-file { background: var(--bg-3); cursor: pointer; border: 1px solid transparent; }
		.detail-file:hover { border-color: var(--accent); }
		.detail-file-preview { aspect-ratio: 1; background: var(--bg-4); display: flex; align-items: center; justify-content: center; overflow: hidden; }
		.detail-file-preview img, .detail-file-preview video { width: 100%; height: 100%; object-fit: cover; }
		.detail-file-icon { font-size: 1.3rem; color: var(--text-3); }
		.detail-file-info { padding: 6px; }
		.detail-file-name { font-size: 0.7rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.detail-file-size { font-size: 0.6rem; color: var(--text-2); }
		
		.form-group { margin-bottom: 12px; }
		.form-label { display: block; font-size: 0.65rem; color: var(--text-2); margin-bottom: 5px; text-transform: uppercase; }
		.form-input, .form-textarea, .form-select { width: 100%; padding: 9px 11px; background: var(--bg-3); border: 1px solid var(--border); color: var(--text-0); font-size: 0.8rem; font-family: inherit; outline: none; }
		.form-input:focus, .form-textarea:focus, .form-select:focus { border-color: var(--accent); }
		.form-textarea { min-height: 70px; resize: vertical; font-family: monospace; font-size: 0.75rem; }
		.form-hint { font-size: 0.65rem; color: var(--text-3); margin-top: 4px; }
		
		.btn { padding: 7px 12px; font-size: 0.75rem; font-weight: 500; border: 1px solid var(--border); background: var(--bg-3); color: var(--text-0); cursor: pointer; font-family: inherit; }
		.btn:hover { background: var(--bg-4); border-color: var(--text-3); }
		.btn-primary { background: var(--accent); border-color: var(--accent); }
		.btn-primary:hover { background: var(--accent-dim); }
		.btn-sm { padding: 4px 8px; font-size: 0.65rem; }
		.btn-danger { border-color: var(--error); color: var(--error); }
		.btn-danger:hover { background: var(--error); color: white; }
		.btn-block { width: 100%; }
		
		.modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.85); display: flex; align-items: center; justify-content: center; z-index: 1000; }
		.modal { background: var(--bg-2); border: 1px solid var(--border); width: 100%; max-width: 480px; max-height: 90vh; overflow: auto; }
		.modal-lg { max-width: 650px; }
		.modal-header { padding: 14px 18px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }
		.modal-title { font-size: 0.95rem; font-weight: 600; }
		.modal-close { background: none; border: none; color: var(--text-2); font-size: 1.4rem; cursor: pointer; line-height: 1; }
		.modal-body { padding: 18px; }
		.modal-footer { padding: 14px 18px; border-top: 1px solid var(--border); display: flex; justify-content: flex-end; gap: 8px; }
		
		.tabs { display: flex; border-bottom: 1px solid var(--border); }
		.tab { padding: 10px 14px; font-size: 0.75rem; color: var(--text-2); cursor: pointer; border-bottom: 2px solid transparent; }
		.tab:hover { color: var(--text-1); }
		.tab.active { color: var(--accent); border-bottom-color: var(--accent); }
		.tab-content { display: none; }
		.tab-content.active { display: block; }
		
		.upload-zone { border: 2px dashed var(--border); padding: 25px; text-align: center; cursor: pointer; }
		.upload-zone:hover, .upload-zone.dragover { border-color: var(--accent); }
		.upload-zone-text { color: var(--text-2); font-size: 0.8rem; }
		
		.lightbox { position: fixed; inset: 0; background: rgba(0,0,0,0.95); display: flex; align-items: center; justify-content: center; z-index: 2000; }
		.lightbox img, .lightbox video { max-width: 95vw; max-height: 95vh; object-fit: contain; }
		.lightbox-close { position: absolute; top: 16px; right: 16px; background: none; border: none; color: white; font-size: 2rem; cursor: pointer; }
		
		.toast { position: fixed; bottom: 20px; right: 20px; padding: 10px 18px; background: var(--bg-2); border: 1px solid var(--border); font-size: 0.8rem; z-index: 3000; animation: slideIn 0.2s ease; }
		.toast.success { border-left: 3px solid var(--success); }
		.toast.error { border-left: 3px solid var(--error); }
		@keyframes slideIn { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
		
		.welcome { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; padding: 40px; text-align: center; }
		.welcome h2 { font-size: 1.4rem; margin-bottom: 8px; }
		.welcome p { color: var(--text-2); margin-bottom: 20px; font-size: 0.9rem; }
		.welcome-actions { display: flex; gap: 12px; }
		
		.empty-state { padding: 50px 20px; text-align: center; color: var(--text-2); }
		.empty-state h3 { color: var(--text-1); margin-bottom: 6px; font-size: 1rem; }
		
		.hidden { display: none !important; }
		::-webkit-scrollbar { width: 7px; height: 7px; }
		::-webkit-scrollbar-track { background: var(--bg-1); }
		::-webkit-scrollbar-thumb { background: var(--border); }
		::-webkit-scrollbar-thumb:hover { background: var(--text-3); }
	</style>
</head>
<body>
	<div class="app">
		<header class="header">
			<div class="header-left">
				<div class="logo">DLFI</div>
				<div class="archive-info" id="archiveInfo"><span style="color:var(--text-3)">No archive open</span></div>
			</div>
			<div class="header-actions">
				<button class="btn btn-sm" onclick="showExtractorModal()">Extract</button>
				<button class="btn btn-sm" onclick="showCreateModal()">Create</button>
				<button class="btn btn-sm" onclick="showSettingsModal()">Settings</button>
			</div>
		</header>
		
		<div class="main">
			<aside class="sidebar" id="sidebar">
				<div class="sidebar-header"><span>Browser</span><button class="btn btn-sm" style="padding:3px 6px" onclick="refreshTree()">↻</button></div>
				<div class="tree-container" id="treeContainer"></div>
			</aside>
			
			<main class="content" id="contentArea">
				<div class="welcome" id="welcomeScreen">
					<h2>Welcome to DLFI</h2>
					<p>Open an existing archive or create a new one.</p>
					<div class="welcome-actions">
						<button class="btn" onclick="showOpenArchiveModal()">Open Archive</button>
						<button class="btn btn-primary" onclick="showCreateArchiveModal()">Create Archive</button>
					</div>
				</div>
				
				<div class="hidden" id="mainUI">
					<div class="search-container">
						<div class="search-row">
							<div class="search-input-wrap">
								<input type="text" class="search-input" id="searchInput" placeholder="Search..." autocomplete="off">
								<div class="autocomplete-dropdown" id="autocompleteDropdown"></div>
							</div>
							<button class="btn btn-primary" onclick="executeSearch()">Search</button>
						</div>
						<div class="search-hints">
							<div class="search-hints-row"><code>tag:val</code> tag contains <code>tag=val</code> tag equals <code>key:val</code> metadata contains <code>key=val</code> metadata equals <code>key</code> key exists</div>
							<div class="search-hints-row"><code>RELATION</code> has relationship type <code>!path</code> related to path <code>!path:REL</code> with relation <code>!path:REL&gt;</code> outgoing <code>!path:REL&lt;</code> incoming</div>
							<div class="search-hints-row"><code>^...</code> deep search (children inherit) <code>%...</code> reverse deep (parents inherit)</div>
						</div>
						<div class="filter-bar" id="filterBar">
							<button class="filter-add" onclick="showAdvFilterModal()">+ Filter</button>
						</div>
					</div>
					
					<div class="results-container">
						<div class="results-header">
							<span id="resultsCount">0 results</span>
							<select class="form-select" style="width:auto;padding:3px 6px;font-size:0.7rem" onchange="setTypeFilter(this.value)">
								<option value="">All Types</option>
								<option value="VAULT">Vaults</option>
								<option value="RECORD">Records</option>
							</select>
						</div>
						<div class="results-grid" id="resultsGrid"><div class="empty-state"><h3>Start searching</h3><p>Type in the search box above.</p></div></div>
					</div>
				</div>
			</main>
			
			<aside class="detail-panel hidden" id="detailPanel">
				<div class="detail-header">
					<div><div class="detail-title" id="detailTitle"></div><div style="font-size:0.7rem;color:var(--text-2);margin-top:3px" id="detailPath"></div></div>
					<button class="detail-close" onclick="closeDetailPanel()">&times;</button>
				</div>
				<div class="detail-body" id="detailBody"></div>
			</aside>
		</div>
	</div>
	
	<div class="lightbox hidden" id="lightbox" onclick="closeLightbox(event)"><button class="lightbox-close">&times;</button><div id="lightboxContent"></div></div>
	
	<script>
	let archiveOpen = false, selectedPath = null, advFilters = [], typeFilter = '', currentNode = null, expandedNodes = new Set();
	let acIndex = -1, acItems = [];
	
	async function api(m, p, b = null) {
		const o = { method: m, headers: {} };
		if (b && !(b instanceof FormData)) { o.headers['Content-Type'] = 'application/json'; o.body = JSON.stringify(b); }
		else if (b) o.body = b;
		const r = await fetch(p, o);
		const d = await r.json();
		if (!r.ok) throw new Error(d.error || 'Failed');
		return d;
	}
	
	function toast(m, t = 'info') { const e = document.createElement('div'); e.className = `toast ${t}`; e.textContent = m; document.body.appendChild(e); setTimeout(() => e.remove(), 3000); }
	function formatSize(b) { if (!b) return '0 B'; const k = 1024, s = ['B','KB','MB','GB'], i = Math.floor(Math.log(b)/Math.log(k)); return parseFloat((b/Math.pow(k,i)).toFixed(1))+' '+s[i]; }
	function closeModal(e) { e.closest('.modal-overlay')?.remove(); }
	function esc(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
	function debounce(f, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => f(...a), ms); }; }
	
	// Autocomplete
	const sInput = document.getElementById('searchInput'), acDrop = document.getElementById('autocompleteDropdown');
	sInput.addEventListener('input', debounce(handleAcInput, 150));
	sInput.addEventListener('keydown', handleAcKey);
	sInput.addEventListener('blur', () => setTimeout(() => acDrop.classList.remove('show'), 150));
	
	async function handleAcInput() {
		const val = sInput.value, pos = sInput.selectionStart;
		const { context, query } = parseToken(val, pos);
		if (!context && !query) { acDrop.classList.remove('show'); return; }
		try {
			const d = await api('GET', `/api/autocomplete?context=${encodeURIComponent(context)}&q=${encodeURIComponent(query)}`);
			acItems = d.suggestions || [];
			renderAc();
		} catch { acItems = []; acDrop.classList.remove('show'); }
	}
	
	function parseToken(val, pos) {
		let start = pos, end = pos;
		while (start > 0 && val[start-1] !== ' ') start--;
		while (end < val.length && val[end] !== ' ') end++;
		let token = val.substring(start, end);
		
		// Strip modifiers
		let mod = '';
		if (token.startsWith('^') || token.startsWith('%')) { mod = token[0]; token = token.slice(1); }
		
		let context = 'start', query = token;
		
		if (token.startsWith('!')) {
			const inner = token.slice(1);
			if (inner.includes(':')) {
				context = '!:';
				query = inner.split(':')[1]?.replace(/[<>]$/, '') || '';
			} else {
				context = '!';
				query = inner.replace(/[<>]$/, '');
			}
		} else if (token.toLowerCase().startsWith('tag:')) { context = 'tag:'; query = token.slice(4); }
		else if (token.toLowerCase().startsWith('tag=')) { context = 'tag='; query = token.slice(4); }
		else if (token.includes(':')) { const [k, v] = token.split(':', 2); context = `meta:${k}:`; query = v || ''; }
		else if (token.includes('=')) { const [k, v] = token.split('=', 2); context = `meta:${k}=`; query = v || ''; }
		else { context = 'key'; query = token; }
		
		return { context, query, start, end, mod };
	}
	
	function renderAc() {
		if (!acItems.length) { acDrop.classList.remove('show'); return; }
		acIndex = -1;
		acDrop.innerHTML = acItems.map((it, i) => `<div class="autocomplete-item" data-i="${i}" onmousedown="selectAc(${i})"><span>${esc(it.label)}</span><span class="autocomplete-item-hint">${esc(it.hint||'')}</span></div>`).join('');
		acDrop.classList.add('show');
	}
	
	function handleAcKey(e) {
		if (!acDrop.classList.contains('show')) { if (e.key === 'Enter') { e.preventDefault(); executeSearch(); } return; }
		if (e.key === 'ArrowDown') { e.preventDefault(); acIndex = Math.min(acIndex + 1, acItems.length - 1); updateAcSel(); }
		else if (e.key === 'ArrowUp') { e.preventDefault(); acIndex = Math.max(acIndex - 1, 0); updateAcSel(); }
		else if (e.key === 'Enter' && acIndex >= 0) { e.preventDefault(); selectAc(acIndex); }
		else if (e.key === 'Escape') { acDrop.classList.remove('show'); }
		else if (e.key === 'Enter') { e.preventDefault(); executeSearch(); }
	}
	
	function updateAcSel() { acDrop.querySelectorAll('.autocomplete-item').forEach((el, i) => el.classList.toggle('selected', i === acIndex)); const s = acDrop.querySelector('.selected'); if (s) s.scrollIntoView({ block: 'nearest' }); }
	
	function selectAc(i) {
		const it = acItems[i]; if (!it) return;
		const val = sInput.value, pos = sInput.selectionStart;
		const { start, end, context, mod } = parseToken(val, pos);
		
		let rep = it.value;
		if (it.type === 'key' || it.type === 'prefix') rep = it.value + (it.value.endsWith(':') || it.value.endsWith('!') ? '' : ':');
		
		// Value completion
		if (context.startsWith('meta:') || context.startsWith('tag') || context === '!:') {
			const opIdx = Math.max(val.lastIndexOf(':', end), val.lastIndexOf('=', end));
			if (opIdx > start) {
				const before = val.substring(0, opIdx + 1), after = val.substring(end);
				sInput.value = before + rep + after;
				sInput.selectionStart = sInput.selectionEnd = before.length + rep.length;
				acDrop.classList.remove('show');
				return;
			}
		}
		
		// Path completion for !
		if (context === '!') {
			const before = val.substring(0, start) + mod + '!', after = val.substring(end);
			sInput.value = before + rep + after;
			sInput.selectionStart = sInput.selectionEnd = before.length + rep.length;
			acDrop.classList.remove('show');
			return;
		}
		
		const before = val.substring(0, start) + mod, after = val.substring(end);
		sInput.value = before + rep + after;
		sInput.selectionStart = sInput.selectionEnd = before.length + rep.length;
		acDrop.classList.remove('show');
		
		if (it.type === 'key' || it.type === 'prefix') setTimeout(() => handleAcInput(), 50);
	}
	
	// Search
	async function executeSearch() {
		if (!archiveOpen) return;
		acDrop.classList.remove('show');
		const q = sInput.value.trim();
		const filters = {};
		if (typeFilter) filters.type = typeFilter;
		for (const f of advFilters) {
			if (f.type === 'inside') filters.inside = f.value;
		}
		try {
			const d = await api('POST', '/api/smart-search', { q, filters, limit: 200 });
			renderResults(d.results);
		} catch (e) { toast(e.message, 'error'); }
	}
	
	function renderResults(results) {
		const grid = document.getElementById('resultsGrid');
		document.getElementById('resultsCount').textContent = `${results.length} result${results.length !== 1 ? 's' : ''}`;
		if (!results.length) { grid.innerHTML = '<div class="empty-state"><h3>No results</h3><p>Try different search terms.</p></div>'; return; }
		
		grid.innerHTML = results.map(r => {
			const meta = r.metadata || {}, tags = r.tags || [];
			const metaKeys = Object.keys(meta).slice(0, 2);
			const hasPreview = r.preview_hash && r.preview_ext;
			const isImg = hasPreview && ['jpg','jpeg','png','gif','webp','bmp'].includes(r.preview_ext);
			const isVid = hasPreview && ['mp4','webm','mov'].includes(r.preview_ext);
			
			return `<div class="result-card" onclick="selectNode('${esc(r.path)}')">
				<div class="result-preview">
					${isImg ? `<img src="/api/blob/${r.preview_hash}" loading="lazy">` : isVid ? `<video src="/api/blob/${r.preview_hash}" muted></video>` : `<span class="result-preview-icon">${r.type === 'VAULT' ? '📁' : '📄'}</span>`}
				</div>
				<div class="result-body">
					<div class="result-header"><span class="result-name">${esc(r.name)}</span><span class="result-type">${r.type}</span></div>
					<div class="result-path">${esc(r.path)}</div>
					<div class="result-meta">
						${tags.slice(0,2).map(t => `<span class="result-tag is-tag">${esc(t)}</span>`).join('')}
						${metaKeys.map(k => `<span class="result-tag">${esc(k)}: ${esc(String(meta[k]).substring(0,15))}</span>`).join('')}
					</div>
				</div>
			</div>`;
		}).join('');
	}
	
	function setTypeFilter(t) { typeFilter = t; executeSearch(); }
	
	function showAdvFilterModal() {
		const m = document.createElement('div'); m.className = 'modal-overlay';
		m.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Add Filter</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">Filter Type</label><select class="form-select" id="advFType"><option value="inside">Inside Path</option></select></div>
			<div class="form-group"><label class="form-label">Value</label><input type="text" class="form-input" id="advFVal"></div></div>
			<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="addAdvFilter()">Add</button></div></div>`;
		document.body.appendChild(m);
	}
	function addAdvFilter() { const t = document.getElementById('advFType').value, v = document.getElementById('advFVal').value.trim(); if (!v) { toast('Value required', 'error'); return; } advFilters.push({type:t,value:v}); renderAdvFilters(); closeModal(document.querySelector('.modal-overlay .modal')); executeSearch(); }
	function removeAdvFilter(i) { advFilters.splice(i, 1); renderAdvFilters(); executeSearch(); }
	function renderAdvFilters() { document.getElementById('filterBar').innerHTML = advFilters.map((f, i) => `<span class="filter-chip"><b>${f.type}:</b> ${esc(f.value)}<span class="remove" onclick="removeAdvFilter(${i})">×</span></span>`).join('') + '<button class="filter-add" onclick="showAdvFilterModal()">+ Filter</button>'; }
	
	// Archive
	async function checkStatus() { const d = await api('GET', '/api/status'); archiveOpen = d.archive_open; updateUI(d); }
	function updateUI(s) {
		const info = document.getElementById('archiveInfo'), wel = document.getElementById('welcomeScreen'), main = document.getElementById('mainUI'), side = document.getElementById('sidebar');
		if (s.archive_open) {
			const n = s.archive_path.split(/[/\\\\]/).pop();
			info.innerHTML = `<span class="path" title="${esc(s.archive_path)}">${esc(n)}</span>${s.encrypted ? '<span class="archive-badge encrypted">Encrypted</span>' : ''}<span style="color:var(--text-2)">${s.stats.nodes} nodes · ${formatSize(s.stats.total_size)}</span>`;
			wel.classList.add('hidden'); main.classList.remove('hidden'); side.style.display = 'flex'; refreshTree();
		} else { info.innerHTML = '<span style="color:var(--text-3)">No archive open</span>'; wel.classList.remove('hidden'); main.classList.add('hidden'); side.style.display = 'none'; closeDetailPanel(); }
	}
	
	function showOpenArchiveModal() { const m = document.createElement('div'); m.className = 'modal-overlay'; m.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Open Archive</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div><div class="modal-body"><div class="form-group"><label class="form-label">Path</label><input type="text" class="form-input" id="openPath"></div><div class="form-group"><label class="form-label">Password</label><input type="password" class="form-input" id="openPwd"></div></div><div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="openArchive()">Open</button></div></div>`; document.body.appendChild(m); m.querySelector('#openPath').focus(); }
	async function openArchive() { const p = document.getElementById('openPath').value.trim(), w = document.getElementById('openPwd').value || null; if (!p) { toast('Path required', 'error'); return; } try { await api('POST', '/api/archive/open', { path: p, password: w }); closeModal(document.querySelector('.modal-overlay .modal')); await checkStatus(); toast('Opened', 'success'); } catch (e) { toast(e.message, 'error'); } }
	
	function showCreateArchiveModal() { const m = document.createElement('div'); m.className = 'modal-overlay'; m.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Create Archive</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div><div class="modal-body"><div class="form-group"><label class="form-label">Path</label><input type="text" class="form-input" id="createPath"></div><div class="form-group"><label class="form-label">Password</label><input type="password" class="form-input" id="createPwd"></div><div class="form-group"><label class="form-label">Partition</label><select class="form-select" id="createPart"><option value="0">Disabled</option><option value="26214400">25MB</option><option value="52428800" selected>50MB</option><option value="104857600">100MB</option></select></div></div><div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="createArchive()">Create</button></div></div>`; document.body.appendChild(m); }
	async function createArchive() { const p = document.getElementById('createPath').value.trim(), w = document.getElementById('createPwd').value || null, s = parseInt(document.getElementById('createPart').value); if (!p) { toast('Path required', 'error'); return; } try { await api('POST', '/api/archive/create', { path: p, password: w, partition_size: s }); closeModal(document.querySelector('.modal-overlay .modal')); await checkStatus(); toast('Created', 'success'); } catch (e) { toast(e.message, 'error'); } }
	
	// Tree
	async function loadChildren(pUuid, cont, depth = 0) { try { const d = await api('GET', `/api/children/${pUuid || ''}`); for (const c of d.children) { const n = document.createElement('div'); n.className = 'tree-node'; n.dataset.uuid = c.uuid; n.dataset.path = c.path; const r = document.createElement('div'); r.className = 'tree-item'; r.style.paddingLeft = `${10 + depth * 12}px`; const t = document.createElement('span'); t.className = `tree-toggle ${c.hasChildren ? '' : 'hidden'} ${expandedNodes.has(c.uuid) ? 'expanded' : ''}`; t.textContent = '▶'; const i = document.createElement('span'); i.className = 'tree-icon'; i.textContent = c.type === 'VAULT' ? '📁' : '📄'; const nm = document.createElement('span'); nm.className = 'tree-name'; nm.textContent = c.name; r.append(t, i, nm); r.onclick = e => { e.stopPropagation(); if (e.target === t && c.hasChildren) toggleTree(c.uuid, n, depth); else selectNode(c.path); }; const cc = document.createElement('div'); cc.className = `tree-children ${expandedNodes.has(c.uuid) ? 'expanded' : ''}`; n.append(r, cc); cont.appendChild(n); if (expandedNodes.has(c.uuid) && c.hasChildren) await loadChildren(c.uuid, cc, depth + 1); } } catch (e) { console.error(e); } }
	async function toggleTree(uuid, n, depth) { const t = n.querySelector('.tree-toggle'), cc = n.querySelector('.tree-children'); if (expandedNodes.has(uuid)) { expandedNodes.delete(uuid); t.classList.remove('expanded'); cc.classList.remove('expanded'); cc.innerHTML = ''; } else { expandedNodes.add(uuid); t.classList.add('expanded'); cc.classList.add('expanded'); await loadChildren(uuid, cc, depth + 1); } }
	async function refreshTree() { document.getElementById('treeContainer').innerHTML = ''; await loadChildren(null, document.getElementById('treeContainer'), 0); }
	
	// Detail
	async function selectNode(path) { selectedPath = path; document.querySelectorAll('.tree-item').forEach(e => e.classList.remove('selected')); const tn = document.querySelector(`[data-path="${path}"] > .tree-item`); if (tn) tn.classList.add('selected'); try { const n = await api('GET', `/api/node/${encodeURIComponent(path)}`); currentNode = n; renderDetail(n); } catch (e) { toast(e.message, 'error'); } }
	
	function renderDetail(n) {
		const p = document.getElementById('detailPanel'); p.classList.remove('hidden');
		document.getElementById('detailTitle').textContent = n.name;
		document.getElementById('detailPath').textContent = n.path;
		let h = '';
		const meta = n.metadata || {};
		h += `<div class="detail-section"><div class="detail-section-title">Metadata <button class="btn btn-sm" onclick="showEditMeta()">Edit</button></div>${Object.keys(meta).length ? `<div class="detail-meta-grid">${Object.entries(meta).map(([k,v]) => `<div class="detail-meta-item ${String(v).length > 40 ? 'full' : ''}"><div class="detail-meta-label">${esc(k)}</div><div class="detail-meta-value">${esc(typeof v === 'object' ? JSON.stringify(v) : String(v))}</div></div>`).join('')}</div>` : '<div style="color:var(--text-3);font-size:0.8rem">No metadata</div>'}</div>`;
		h += `<div class="detail-section"><div class="detail-section-title">Tags</div><div class="detail-tags">${n.tags.map(t => `<span class="detail-tag">${esc(t)}<span class="remove" onclick="remTag('${esc(t)}')">×</span></span>`).join('')}<button class="btn btn-sm" onclick="showAddTag()">+</button></div></div>`;
		h += `<div class="detail-section"><div class="detail-section-title">Relationships</div>`;
		for (const r of n.relationships) h += `<div class="detail-rel-item"><span class="detail-rel-type">${esc(r.relation)}</span><span class="detail-rel-path" onclick="selectNode('${esc(r.target_path)}')">${esc(r.target_path)}</span><span class="detail-rel-dir">→</span><span class="detail-rel-remove" onclick="remRel('${esc(n.path)}','${esc(r.target_path)}','${esc(r.relation)}')">×</span></div>`;
		for (const r of n.incoming_relationships) h += `<div class="detail-rel-item"><span class="detail-rel-type">${esc(r.relation)}</span><span class="detail-rel-path" onclick="selectNode('${esc(r.source_path)}')">${esc(r.source_path)}</span><span class="detail-rel-dir">←</span></div>`;
		h += `<button class="btn btn-sm" style="margin-top:6px" onclick="showAddRel()">+ Add</button></div>`;
		if (n.files.length || n.type === 'RECORD') { h += `<div class="detail-section"><div class="detail-section-title">Files (${n.files.length})</div>`; if (n.files.length) { h += '<div class="detail-files-grid">'; for (const f of n.files) { const isI = ['jpg','jpeg','png','gif','webp','bmp'].includes(f.ext), isV = ['mp4','webm','mov'].includes(f.ext); h += `<div class="detail-file" onclick="openFile('${f.hash}','${f.ext}','${esc(f.name)}')"><div class="detail-file-preview">${isI ? `<img src="/api/blob/${f.hash}" loading="lazy">` : isV ? `<video src="/api/blob/${f.hash}" muted></video>` : '<span class="detail-file-icon">📎</span>'}</div><div class="detail-file-info"><div class="detail-file-name" title="${esc(f.name)}">${esc(f.name)}</div><div class="detail-file-size">${formatSize(f.size)}</div></div></div>`; } h += '</div>'; } h += `<button class="btn btn-sm btn-block" style="margin-top:8px" onclick="showUpload()">+ Upload</button></div>`; }
		h += `<div class="detail-section"><div class="detail-section-title">Actions</div><button class="btn btn-sm btn-danger" onclick="delNode()">Delete</button></div>`;
		document.getElementById('detailBody').innerHTML = h;
	}
	function closeDetailPanel() { document.getElementById('detailPanel').classList.add('hidden'); selectedPath = null; currentNode = null; }
	
	// CRUD
	function showEditMeta() { if (!currentNode) return; const m = document.createElement('div'); m.className = 'modal-overlay'; m.innerHTML = `<div class="modal modal-lg"><div class="modal-header"><span class="modal-title">Edit Metadata</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div><div class="modal-body"><div class="form-group"><label class="form-label">Path</label><input class="form-input" value="${esc(currentNode.path)}" readonly></div><div class="form-group"><label class="form-label">Metadata (JSON)</label><textarea class="form-textarea" id="editMetaVal" style="min-height:200px">${esc(JSON.stringify(currentNode.metadata||{},null,2))}</textarea></div></div><div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="saveMeta()">Save</button></div></div>`; document.body.appendChild(m); }
	async function saveMeta() { if (!currentNode) return; try { const mt = JSON.parse(document.getElementById('editMetaVal').value); await api('POST', '/api/node/update', { path: currentNode.path, metadata: mt }); toast('Updated', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); selectNode(currentNode.path); } catch (e) { toast(e.message, 'error'); } }
	
	function showAddTag() { const t = prompt('Tag:'); if (t) addTag(t.trim()); }
	async function addTag(t) { if (!currentNode) return; try { await api('POST', '/api/tag', { path: currentNode.path, tag: t }); toast('Added', 'success'); selectNode(currentNode.path); } catch (e) { toast(e.message, 'error'); } }
	async function remTag(t) { if (!currentNode) return; try { await api('DELETE', '/api/tag', { path: currentNode.path, tag: t }); toast('Removed', 'success'); selectNode(currentNode.path); } catch (e) { toast(e.message, 'error'); } }
	
	function showAddRel() { const m = document.createElement('div'); m.className = 'modal-overlay'; m.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Add Relationship</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div><div class="modal-body"><div class="form-group"><label class="form-label">Source</label><input class="form-input" value="${esc(currentNode?.path||'')}" readonly></div><div class="form-group"><label class="form-label">Relation</label><input class="form-input" id="relN"></div><div class="form-group"><label class="form-label">Target Path</label><input class="form-input" id="relT"></div></div><div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="addRel()">Add</button></div></div>`; document.body.appendChild(m); }
	async function addRel() { if (!currentNode) return; const r = document.getElementById('relN').value.trim(), t = document.getElementById('relT').value.trim(); if (!r || !t) { toast('All fields required', 'error'); return; } try { await api('POST', '/api/link', { source: currentNode.path, target: t, relation: r }); toast('Created', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); selectNode(currentNode.path); } catch (e) { toast(e.message, 'error'); } }
	async function remRel(s, t, r) { try { await api('DELETE', '/api/link', { source: s, target: t, relation: r }); toast('Removed', 'success'); selectNode(s); } catch (e) { toast(e.message, 'error'); } }
	
	function showUpload() { const m = document.createElement('div'); m.className = 'modal-overlay'; m.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Upload</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div><div class="modal-body"><div class="form-group"><label class="form-label">Target</label><input class="form-input" id="upPath" value="${esc(currentNode?.path||'')}" readonly></div><div class="upload-zone" id="upZone" onclick="document.getElementById('upInput').click()"><div style="font-size:1.5rem;margin-bottom:6px">📁</div><div class="upload-zone-text">Drop files or click</div><input type="file" id="upInput" multiple style="display:none" onchange="handleUp()"></div></div></div>`; document.body.appendChild(m); const z = m.querySelector('#upZone'); z.ondragover = e => { e.preventDefault(); z.classList.add('dragover'); }; z.ondragleave = () => z.classList.remove('dragover'); z.ondrop = async e => { e.preventDefault(); z.classList.remove('dragover'); await upFiles(e.dataTransfer.files); }; }
	async function handleUp() { await upFiles(document.getElementById('upInput').files); }
	async function upFiles(files) { const p = document.getElementById('upPath').value, fd = new FormData(); fd.append('path', p); for (const f of files) fd.append('file', f, f.name); try { await api('POST', '/api/upload', fd); toast(`Uploaded ${files.length}`, 'success'); closeModal(document.querySelector('.modal-overlay .modal')); selectNode(p); } catch (e) { toast(e.message, 'error'); } }
	
	async function delNode() { if (!currentNode) return; if (!confirm(`Delete "${currentNode.path}"?`)) return; try { await api('DELETE', `/api/node/${encodeURIComponent(currentNode.path)}`); toast('Deleted', 'success'); closeDetailPanel(); refreshTree(); executeSearch(); } catch (e) { toast(e.message, 'error'); } }
	
	function showCreateModal() { if (!archiveOpen) { toast('Open archive first', 'error'); return; } const m = document.createElement('div'); m.className = 'modal-overlay'; m.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Create Node</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div><div class="modal-body"><div class="form-group"><label class="form-label">Type</label><select class="form-select" id="crType"><option value="VAULT">Vault</option><option value="RECORD">Record</option></select></div><div class="form-group"><label class="form-label">Path</label><input class="form-input" id="crPath"></div><div class="form-group"><label class="form-label">Metadata (JSON)</label><textarea class="form-textarea" id="crMeta"></textarea></div></div><div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="createNode()">Create</button></div></div>`; document.body.appendChild(m); }
	async function createNode() { const t = document.getElementById('crType').value, p = document.getElementById('crPath').value.trim(); let mt = {}; try { const m = document.getElementById('crMeta').value.trim(); if (m) mt = JSON.parse(m); } catch { toast('Invalid JSON', 'error'); return; } if (!p) { toast('Path required', 'error'); return; } try { await api('POST', t === 'VAULT' ? '/api/vault' : '/api/record', { path: p, metadata: mt }); toast('Created', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); refreshTree(); executeSearch(); } catch (e) { toast(e.message, 'error'); } }
	
	// Extract & Settings
	async function showExtractorModal() { if (!archiveOpen) { toast('Open archive first', 'error'); return; } let ex = []; try { ex = (await api('GET', '/api/extractors')).extractors; } catch { toast('Failed', 'error'); return; } const m = document.createElement('div'); m.className = 'modal-overlay'; m.innerHTML = `<div class="modal modal-lg"><div class="modal-header"><span class="modal-title">Extract</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div><div class="modal-body"><div class="form-group"><label class="form-label">URL</label><input class="form-input" id="exUrl"><div class="form-hint">Extractors: ${ex.map(e => e.name).join(', ')}</div></div><div class="form-group"><label class="form-label">Cookies</label><textarea class="form-textarea" id="exCk"></textarea></div><div class="form-group"><label class="form-label">Config (JSON)</label><textarea class="form-textarea" id="exCfg"></textarea></div></div><div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="runExtract()">Extract</button></div></div>`; document.body.appendChild(m); }
	async function runExtract() { const u = document.getElementById('exUrl').value.trim(), ck = document.getElementById('exCk').value; let cfg = {}; try { const c = document.getElementById('exCfg').value.trim(); if (c) cfg = JSON.parse(c); } catch { toast('Invalid JSON', 'error'); return; } if (!u) { toast('URL required', 'error'); return; } try { toast('Extracting...', 'info'); closeModal(document.querySelector('.modal-overlay .modal')); await api('POST', '/api/extract', { url: u, cookies: ck, config: cfg }); toast('Done', 'success'); refreshTree(); executeSearch(); } catch (e) { toast(e.message, 'error'); } }
	
	async function showSettingsModal() { if (!archiveOpen) { showOpenArchiveModal(); return; } let cfg; try { cfg = await api('GET', '/api/config'); } catch { toast('Failed', 'error'); return; } const m = document.createElement('div'); m.className = 'modal-overlay'; m.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Settings</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div><div class="modal-body"><div class="tabs"><div class="tab active" data-t="general" onclick="swTab('general')">General</div><div class="tab" data-t="enc" onclick="swTab('enc')">Encryption</div><div class="tab" data-t="act" onclick="swTab('act')">Actions</div></div><div class="tab-content active" id="tabGeneral" style="padding-top:14px"><div class="form-group"><label class="form-label">Path</label><input class="form-input" value="${esc(cfg.path)}" readonly></div><div class="form-group"><label class="form-label">Partition</label><select class="form-select" id="setPart"><option value="0" ${cfg.partition_size===0?'selected':''}>Disabled</option><option value="26214400" ${cfg.partition_size===26214400?'selected':''}>25MB</option><option value="52428800" ${cfg.partition_size===52428800?'selected':''}>50MB</option><option value="104857600" ${cfg.partition_size===104857600?'selected':''}>100MB</option></select><button class="btn btn-sm" style="margin-top:6px" onclick="updPart()">Update</button></div></div><div class="tab-content" id="tabEnc" style="padding-top:14px"><p style="margin-bottom:12px;color:var(--text-2)">Status: ${cfg.encrypted ? '<span style="color:var(--success)">Enabled</span>' : 'Disabled'}</p>${cfg.encrypted ? `<div class="form-group"><label class="form-label">Current Password</label><input type="password" class="form-input" id="encOld"></div><div class="form-group"><label class="form-label">New (empty to disable)</label><input type="password" class="form-input" id="encNew"></div><button class="btn" onclick="updEnc()">Update</button>` : `<div class="form-group"><label class="form-label">Password</label><input type="password" class="form-input" id="encNew"></div><button class="btn" onclick="enableEnc()">Enable</button>`}</div><div class="tab-content" id="tabAct" style="padding-top:14px"><div class="form-group"><button class="btn btn-block" onclick="genStatic()">Generate Static Site</button></div><div class="form-group"><button class="btn btn-block" onclick="closeArch()">Close Archive</button></div></div></div></div>`; document.body.appendChild(m); }
	function swTab(t) { document.querySelectorAll('.modal .tab').forEach(e => e.classList.toggle('active', e.dataset.t === t)); document.querySelectorAll('.modal .tab-content').forEach(c => c.classList.remove('active')); document.getElementById('tab' + t.charAt(0).toUpperCase() + t.slice(1)).classList.add('active'); }
	async function updPart() { try { await api('POST', '/api/config/partition', { size: parseInt(document.getElementById('setPart').value) }); toast('Updated', 'success'); } catch (e) { toast(e.message, 'error'); } }
	async function enableEnc() { const p = document.getElementById('encNew').value; if (!p) { toast('Password required', 'error'); return; } try { await api('POST', '/api/config/encryption', { action: 'enable', password: p }); toast('Enabled', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); checkStatus(); } catch (e) { toast(e.message, 'error'); } }
	async function updEnc() { const o = document.getElementById('encOld').value, n = document.getElementById('encNew').value; if (!o) { toast('Current password required', 'error'); return; } try { if (n) { await api('POST', '/api/config/encryption', { action: 'change_password', old_password: o, new_password: n }); toast('Changed', 'success'); } else { await api('POST', '/api/config/encryption', { action: 'disable', password: o }); toast('Disabled', 'success'); } closeModal(document.querySelector('.modal-overlay .modal')); checkStatus(); } catch (e) { toast(e.message, 'error'); } }
	async function genStatic() { try { await api('POST', '/api/generate-static'); toast('Generated', 'success'); } catch (e) { toast(e.message, 'error'); } }
	async function closeArch() { await api('POST', '/api/archive/close'); closeModal(document.querySelector('.modal-overlay .modal')); checkStatus(); }
	
	// Lightbox
	function openFile(h, e, n) { const isI = ['jpg','jpeg','png','gif','webp','bmp'].includes(e), isV = ['mp4','webm','mov'].includes(e); if (isI) { document.getElementById('lightboxContent').innerHTML = `<img src="/api/blob/${h}">`; document.getElementById('lightbox').classList.remove('hidden'); } else if (isV) { document.getElementById('lightboxContent').innerHTML = `<video src="/api/blob/${h}" controls autoplay></video>`; document.getElementById('lightbox').classList.remove('hidden'); } else { const a = document.createElement('a'); a.href = `/api/blob/${h}`; a.download = n; a.click(); } }
	function closeLightbox(e) { if (!e || e.target.id === 'lightbox' || e.target.classList.contains('lightbox-close')) { document.getElementById('lightbox').classList.add('hidden'); document.getElementById('lightboxContent').innerHTML = ''; } }
	
	document.addEventListener('keydown', e => { if (e.key === 'Escape') { closeLightbox(); document.querySelector('.modal-overlay')?.remove(); } });
	checkStatus();
	</script>
</body>
</html>'''