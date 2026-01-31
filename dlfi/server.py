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
		logger.info(f"Opened archive: {self.archive_path}")
	
	def close_archive(self):
		if self.dlfi:
			self.dlfi.close()
			self.dlfi = None
			self.archive_path = None
	
	def _get_all_metadata_keys(self) -> List[str]:
		"""Extract all unique metadata keys from the archive."""
		if not self.dlfi:
			return []
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
		return sorted(keys)
	
	def _collect_keys(self, obj: dict, prefix: str, keys: set):
		"""Recursively collect all keys from nested dict."""
		for k, v in obj.items():
			full_key = f"{prefix}.{k}" if prefix else k
			keys.add(full_key)
			if isinstance(v, dict):
				self._collect_keys(v, full_key, keys)
	
	def _get_metadata_values(self, key: str) -> List[str]:
		"""Get all unique values for a metadata key."""
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
					self.send_error_json("No archive open. Open an archive first.", 400)
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
				"""
				Context-aware autocomplete.
				context can be:
				- "start" - beginning of token, suggest prefixes and keys
				- "tag:" or "tag=" - suggest tag values
				- "meta:KEY:" or "meta:KEY=" - suggest values for KEY
				- "key" - suggest metadata keys
				"""
				if not self.require_archive():
					return
				
				try:
					suggestions = []
					
					if context == "start" or context == "key":
						# Suggest metadata keys and special prefixes
						meta_keys = server._get_all_metadata_keys()
						q_lower = query.lower()
						
						# Filter and format suggestions
						for key in meta_keys:
							if not query or q_lower in key.lower():
								suggestions.append({
									"value": key,
									"label": key,
									"type": "key",
									"hint": "metadata key"
								})
						
						# Also suggest tag: prefix if relevant
						if not query or "tag".startswith(q_lower):
							suggestions.insert(0, {
								"value": "tag:",
								"label": "tag:",
								"type": "prefix",
								"hint": "filter by tag"
							})
						
						# Limit
						suggestions = suggestions[:20]
					
					elif context.startswith("tag"):
						# Suggest tags
						cursor = server.dlfi.conn.execute(
							"SELECT DISTINCT tag FROM tags ORDER BY tag"
						)
						all_tags = [r[0] for r in cursor]
						q_lower = query.lower()
						
						for tag in all_tags:
							if not query or q_lower in tag.lower():
								suggestions.append({
									"value": tag,
									"label": tag,
									"type": "tag",
									"hint": "tag"
								})
						suggestions = suggestions[:20]
					
					elif context.startswith("meta:"):
						# Suggest values for a specific metadata key
						key = context[5:]
						if key.endswith(":") or key.endswith("="):
							key = key[:-1]
						
						values = server._get_metadata_values(key)
						q_lower = query.lower()
						
						for val in values:
							if not query or q_lower in val.lower():
								suggestions.append({
									"value": val,
									"label": val,
									"type": "value",
									"hint": f"value for {key}"
								})
						suggestions = suggestions[:20]
					
					elif context == "relation":
						cursor = server.dlfi.conn.execute(
							"SELECT DISTINCT relation FROM edges ORDER BY relation"
						)
						q_lower = query.lower()
						for r in cursor:
							if not query or q_lower in r[0].lower():
								suggestions.append({
									"value": r[0],
									"label": r[0],
									"type": "relation",
									"hint": "relationship"
								})
						suggestions = suggestions[:20]
					
					elif context == "path":
						cursor = server.dlfi.conn.execute(
							"SELECT cached_path FROM nodes ORDER BY cached_path LIMIT 100"
						)
						q_lower = query.lower()
						for r in cursor:
							if not query or q_lower in r[0].lower():
								suggestions.append({
									"value": r[0],
									"label": r[0],
									"type": "path",
									"hint": "node path"
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
				- key:value - metadata key contains value
				- key=value - metadata key equals value
				- key - metadata key exists
				- plain text - full text search
				
				Additional filters passed separately:
				- type: VAULT/RECORD
				- inside: path prefix
				- related_to: {target, relation}
				"""
				if not self.require_archive():
					return
				
				try:
					body = self.read_json_body()
					query_str = body.get("q", "").strip()
					filters = body.get("filters", {})
					limit = body.get("limit", 200)
					
					# Parse query string
					parsed = self._parse_smart_query(query_str)
					
					# Build SQL
					sql_parts = ["SELECT DISTINCT n.uuid, n.cached_path, n.type, n.name, n.metadata FROM nodes n"]
					joins = []
					conditions = []
					params = []
					
					# Tag conditions
					if parsed["tags_contain"] or parsed["tags_eq"]:
						joins.append("LEFT JOIN tags t ON n.uuid = t.node_uuid")
					
					for tag in parsed["tags_contain"]:
						conditions.append("t.tag LIKE ?")
						params.append(f"%{tag.lower()}%")
					
					for tag in parsed["tags_eq"]:
						conditions.append("t.tag = ?")
						params.append(tag.lower())
					
					# Metadata contains
					for key, value in parsed["meta_contain"].items():
						json_path = self._key_to_json_path(key)
						conditions.append(f"LOWER(json_extract(n.metadata, '$.{json_path}')) LIKE ?")
						params.append(f"%{value.lower()}%")
					
					# Metadata equals
					for key, value in parsed["meta_eq"].items():
						json_path = self._key_to_json_path(key)
						# Try numeric comparison if value looks like a number
						try:
							num_val = float(value)
							conditions.append(f"json_extract(n.metadata, '$.{json_path}') = ?")
							params.append(num_val if num_val != int(num_val) else int(num_val))
						except ValueError:
							conditions.append(f"json_extract(n.metadata, '$.{json_path}') = ?")
							params.append(value)
					
					# Metadata exists
					for key in parsed["meta_exists"]:
						json_path = self._key_to_json_path(key)
						conditions.append(f"json_extract(n.metadata, '$.{json_path}') IS NOT NULL")
					
					# Full text search
					if parsed["text"]:
						text_query = " ".join(parsed["text"]).lower()
						if "t" not in "".join(joins):
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
					
					if filters.get("related_to"):
						rel = filters["related_to"]
						target_path = rel.get("target", "")
						relation = rel.get("relation")
						
						target_uuid = server.dlfi._resolve_path(target_path)
						if target_uuid:
							joins.append("JOIN edges e ON n.uuid = e.source_uuid")
							conditions.append("e.target_uuid = ?")
							params.append(target_uuid)
							if relation:
								conditions.append("e.relation = ?")
								params.append(relation.upper())
						else:
							conditions.append("1=0")  # No results if target not found
					
					# Build final SQL
					sql = sql_parts[0]
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
						# Fetch tags for each result
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
							"tags": tags
						})
					
					self.send_json({"results": results, "count": len(results)})
					
				except Exception as e:
					logger.error(f"Smart search error: {e}", exc_info=True)
					self.send_error_json(str(e), 500)
			
			def _parse_smart_query(self, query: str) -> dict:
				"""Parse the smart search query string."""
				result = {
					"text": [],
					"tags_contain": [],
					"tags_eq": [],
					"meta_contain": {},
					"meta_eq": {},
					"meta_exists": []
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
				
				for token in tokens:
					token = token.strip()
					if not token:
						continue
					
					# Check for tag: or tag=
					if token.lower().startswith("tag:"):
						result["tags_contain"].append(token[4:])
					elif token.lower().startswith("tag="):
						result["tags_eq"].append(token[4:])
					elif ":" in token:
						# metadata contains
						key, value = token.split(":", 1)
						if key and value:
							result["meta_contain"][key] = value
						elif key:
							result["meta_exists"].append(key)
					elif "=" in token:
						# metadata equals
						key, value = token.split("=", 1)
						if key and value:
							result["meta_eq"][key] = value
						elif key:
							result["meta_exists"].append(key)
					else:
						# Could be metadata exists check or plain text
						# If it looks like a key path (has dots or matches known keys), treat as exists
						meta_keys = server._get_all_metadata_keys()
						if token in meta_keys or "." in token:
							result["meta_exists"].append(token)
						else:
							result["text"].append(token)
				
				return result
			
			def _key_to_json_path(self, key: str) -> str:
				"""Convert dotted key to JSON path."""
				# key like "source.url" becomes "source.url" for json_extract
				return key
			
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
						server._extraction_logs.append(f"[START] Extracting: {url}")
						job.run(url, extractor_config if extractor_config else None)
						server._extraction_logs.append(f"[DONE] Completed: {url}")
						self.send_json({"success": True, "message": f"Extraction completed for {url}"})
					finally:
						if cookie_file and os.path.exists(cookie_file):
							os.remove(cookie_file)
				
				except Exception as e:
					server._extraction_logs.append(f"[ERROR] {url}: {str(e)}")
					logger.error(f"Extraction failed: {e}", exc_info=True)
					self.send_error_json(str(e), 500)
			
			# === Tree & Navigation ===
			
			def api_get_tree(self):
				if not self.require_archive():
					return
				try:
					cursor = server.dlfi.conn.execute("""
						SELECT uuid, parent_uuid, type, name, cached_path 
						FROM nodes ORDER BY cached_path
					""")
					nodes = [{"uuid": r[0], "parent": r[1], "type": r[2], "name": r[3], "path": r[4]} 
							for r in cursor]
					self.send_json({"nodes": nodes})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_get_children(self, parent_uuid: Optional[str]):
				if not self.require_archive():
					return
				try:
					if parent_uuid in ("null", ""):
						parent_uuid = None
					
					cursor = server.dlfi.conn.execute("""
						SELECT uuid, type, name, cached_path 
						FROM nodes WHERE parent_uuid IS ?
						ORDER BY type DESC, name
					""", (parent_uuid,))
					
					children = []
					for row in cursor:
						count = server.dlfi.conn.execute(
							"SELECT COUNT(*) FROM nodes WHERE parent_uuid = ?", (row[0],)
						).fetchone()[0]
						children.append({
							"uuid": row[0], "type": row[1], "name": row[2],
							"path": row[3], "hasChildren": count > 0
						})
					
					self.send_json({"children": children})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_get_node(self, node_path: str):
				if not self.require_archive():
					return
				try:
					cursor = server.dlfi.conn.execute("""
						SELECT uuid, parent_uuid, type, name, cached_path, metadata, created_at, last_modified
						FROM nodes WHERE cached_path = ?
					""", (node_path,))
					
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
						for r in server.dlfi.conn.execute("""
							SELECT e.relation, n.cached_path, e.target_uuid
							FROM edges e LEFT JOIN nodes n ON e.target_uuid = n.uuid
							WHERE e.source_uuid = ?
						""", (row[0],))
					]
					
					node["incoming_relationships"] = [
						{"relation": r[0], "source_path": r[1], "source_uuid": r[2]}
						for r in server.dlfi.conn.execute("""
							SELECT e.relation, n.cached_path, e.source_uuid
							FROM edges e LEFT JOIN nodes n ON e.source_uuid = n.uuid
							WHERE e.target_uuid = ?
						""", (row[0],))
					]
					
					node["files"] = [
						{"name": r[0], "hash": r[1], "size": r[2], "ext": r[3], "parts": r[4]}
						for r in server.dlfi.conn.execute("""
							SELECT nf.original_name, nf.file_hash, b.size_bytes, b.ext, b.part_count
							FROM node_files nf JOIN blobs b ON nf.file_hash = b.hash
							WHERE nf.node_uuid = ? ORDER BY nf.display_order
						""", (row[0],))
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
					
					ext = server.dlfi.conn.execute(
						"SELECT ext FROM blobs WHERE hash = ?", (blob_hash,)
					).fetchone()
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
					self.send_json({"uuid": uuid, "path": path})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_upload_file(self):
				if not self.require_archive():
					return
				try:
					fields, files = self.parse_multipart()
					if not fields or not files:
						self.send_error_json("Multipart form with 'path' and file required")
						return
					record_path = fields.get("path", "").strip()
					if not record_path:
						self.send_error_json("Record path is required")
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
							server.dlfi.conn.execute(
								"DELETE FROM tags WHERE node_uuid = ? AND tag = ?", 
								(uuid, tag.lower())
							)
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
						self.send_error_json("Path is required")
						return
					
					cursor = server.dlfi.conn.execute(
						"SELECT uuid FROM nodes WHERE cached_path = ?", (path,)
					)
					row = cursor.fetchone()
					if not row:
						self.send_error_json("Node not found", 404)
						return
					
					uuid = row[0]
					if metadata is not None:
						with server.dlfi.conn:
							server.dlfi.conn.execute(
								"UPDATE nodes SET metadata = ?, last_modified = ? WHERE uuid = ?",
								(json.dumps(metadata) if metadata else None, time.time(), uuid)
							)
					self.send_json({"success": True, "uuid": uuid})
				except Exception as e:
					logger.error(f"Update node error: {e}", exc_info=True)
					self.send_error_json(str(e), 500)
			
			def api_delete_node(self, node_path: str):
				if not self.require_archive():
					return
				try:
					uuid = server.dlfi._resolve_path(node_path)
					if not uuid:
						self.send_error_json("Node not found", 404)
						return
					with server.dlfi.conn:
						server.dlfi.conn.execute("DELETE FROM nodes WHERE uuid = ?", (uuid,))
					self.send_json({"success": True})
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			# === Legacy Query (for backward compat) ===
			
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
					logger.error(f"Query error: {e}", exc_info=True)
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
						old_pass = body.get("old_password", "")
						new_pass = body.get("new_password", "")
						if not old_pass or not new_pass:
							self.send_error_json("Both passwords required")
							return
						success = server.dlfi.config_manager.change_password(old_pass, new_pass)
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
		
		.sidebar { width: 240px; background: var(--bg-2); border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
		.sidebar-header { padding: 12px 16px; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-2); display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); }
		.tree-container { flex: 1; overflow-y: auto; padding: 4px 0; }
		.tree-item { display: flex; align-items: center; padding: 6px 12px; cursor: pointer; font-size: 0.82rem; color: var(--text-1); transition: background 0.1s; }
		.tree-item:hover { background: var(--bg-3); }
		.tree-item.selected { background: var(--accent); color: white; }
		.tree-toggle { width: 16px; height: 16px; font-size: 8px; display: flex; align-items: center; justify-content: center; color: var(--text-3); margin-right: 4px; transition: transform 0.1s; }
		.tree-toggle.expanded { transform: rotate(90deg); }
		.tree-toggle.hidden { visibility: hidden; }
		.tree-icon { margin-right: 8px; font-size: 13px; }
		.tree-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.tree-children { display: none; }
		.tree-children.expanded { display: block; }
		
		.content { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
		
		.search-container { padding: 16px 24px; background: var(--bg-2); border-bottom: 1px solid var(--border); }
		.search-row { display: flex; gap: 12px; align-items: stretch; }
		.search-input-wrap { flex: 1; position: relative; }
		.search-input { width: 100%; padding: 12px 16px; background: var(--bg-1); border: 1px solid var(--border); color: var(--text-0); font-size: 0.95rem; outline: none; }
		.search-input:focus { border-color: var(--accent); }
		.search-input::placeholder { color: var(--text-3); }
		.search-hint { font-size: 0.7rem; color: var(--text-3); margin-top: 8px; }
		.search-hint code { background: var(--bg-3); padding: 2px 6px; font-family: monospace; }
		
		.autocomplete-dropdown { position: absolute; top: 100%; left: 0; right: 0; background: var(--bg-3); border: 1px solid var(--border); border-top: none; max-height: 300px; overflow-y: auto; z-index: 100; display: none; }
		.autocomplete-dropdown.show { display: block; }
		.autocomplete-item { padding: 10px 14px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; }
		.autocomplete-item:hover, .autocomplete-item.selected { background: var(--bg-4); }
		.autocomplete-item-label { font-size: 0.85rem; }
		.autocomplete-item-hint { font-size: 0.7rem; color: var(--text-3); }
		
		.filter-bar { display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap; align-items: center; }
		.filter-chip { display: flex; align-items: center; gap: 6px; padding: 4px 10px; background: var(--bg-3); border: 1px solid var(--border); font-size: 0.75rem; color: var(--text-1); }
		.filter-chip .remove { cursor: pointer; opacity: 0.5; }
		.filter-chip .remove:hover { opacity: 1; }
		.filter-add { padding: 4px 10px; background: transparent; border: 1px dashed var(--border); font-size: 0.75rem; color: var(--text-2); cursor: pointer; }
		.filter-add:hover { border-color: var(--text-2); color: var(--text-1); }
		
		.results-container { flex: 1; overflow-y: auto; padding: 16px 24px; }
		.results-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; font-size: 0.8rem; color: var(--text-2); }
		.results-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 12px; }
		
		.result-card { background: var(--bg-2); border: 1px solid var(--border); padding: 14px; cursor: pointer; transition: border-color 0.1s; }
		.result-card:hover { border-color: var(--accent); }
		.result-header { display: flex; align-items: flex-start; gap: 10px; margin-bottom: 8px; }
		.result-icon { font-size: 1.2rem; }
		.result-info { flex: 1; min-width: 0; }
		.result-name { font-weight: 500; font-size: 0.9rem; margin-bottom: 2px; word-break: break-word; }
		.result-path { font-size: 0.75rem; color: var(--text-2); word-break: break-all; }
		.result-type { font-size: 0.6rem; text-transform: uppercase; padding: 2px 6px; background: var(--bg-3); color: var(--text-2); }
		.result-meta { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
		.result-tag { font-size: 0.7rem; padding: 2px 8px; background: var(--bg-4); color: var(--text-2); }
		.result-tag.is-tag { background: var(--accent-dim); color: var(--accent); }
		
		.detail-panel { width: 400px; background: var(--bg-2); border-left: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; overflow: hidden; }
		.detail-panel.hidden { display: none; }
		.detail-header { padding: 16px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: flex-start; }
		.detail-title { font-size: 1rem; font-weight: 600; word-break: break-word; }
		.detail-close { background: none; border: none; color: var(--text-2); font-size: 1.2rem; cursor: pointer; }
		.detail-body { flex: 1; overflow-y: auto; padding: 16px; }
		.detail-section { margin-bottom: 24px; }
		.detail-section-title { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-2); margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; }
		
		.detail-meta-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
		.detail-meta-item { background: var(--bg-3); padding: 10px; }
		.detail-meta-item.full { grid-column: span 2; }
		.detail-meta-label { font-size: 0.65rem; color: var(--text-2); margin-bottom: 4px; text-transform: uppercase; }
		.detail-meta-value { font-size: 0.85rem; word-break: break-word; }
		
		.detail-tags { display: flex; flex-wrap: wrap; gap: 6px; }
		.detail-tag { display: flex; align-items: center; gap: 4px; padding: 4px 10px; background: var(--bg-3); font-size: 0.75rem; color: var(--text-1); }
		.detail-tag .remove { cursor: pointer; opacity: 0.5; font-size: 0.9rem; }
		.detail-tag .remove:hover { opacity: 1; }
		
		.detail-rel-item { display: flex; align-items: center; gap: 10px; padding: 10px; background: var(--bg-3); margin-bottom: 6px; cursor: pointer; }
		.detail-rel-item:hover { background: var(--bg-4); }
		.detail-rel-type { font-size: 0.65rem; text-transform: uppercase; color: var(--accent); font-weight: 600; min-width: 80px; }
		.detail-rel-path { font-size: 0.8rem; color: var(--text-1); flex: 1; word-break: break-all; }
		.detail-rel-dir { font-size: 0.6rem; color: var(--text-3); }
		.detail-rel-remove { cursor: pointer; color: var(--text-3); font-size: 0.8rem; }
		.detail-rel-remove:hover { color: var(--error); }
		
		.detail-files-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; }
		.detail-file { background: var(--bg-3); cursor: pointer; border: 1px solid transparent; transition: border-color 0.1s; }
		.detail-file:hover { border-color: var(--accent); }
		.detail-file-preview { aspect-ratio: 1; background: var(--bg-4); display: flex; align-items: center; justify-content: center; overflow: hidden; }
		.detail-file-preview img, .detail-file-preview video { width: 100%; height: 100%; object-fit: cover; }
		.detail-file-icon { font-size: 1.5rem; color: var(--text-3); }
		.detail-file-info { padding: 8px; }
		.detail-file-name { font-size: 0.75rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.detail-file-size { font-size: 0.65rem; color: var(--text-2); }
		
		.form-group { margin-bottom: 14px; }
		.form-label { display: block; font-size: 0.7rem; color: var(--text-2); margin-bottom: 6px; text-transform: uppercase; }
		.form-input, .form-textarea, .form-select { width: 100%; padding: 10px 12px; background: var(--bg-3); border: 1px solid var(--border); color: var(--text-0); font-size: 0.85rem; font-family: inherit; outline: none; }
		.form-input:focus, .form-textarea:focus, .form-select:focus { border-color: var(--accent); }
		.form-textarea { min-height: 80px; resize: vertical; font-family: monospace; }
		.form-hint { font-size: 0.7rem; color: var(--text-3); margin-top: 4px; }
		
		.btn { padding: 8px 14px; font-size: 0.8rem; font-weight: 500; border: 1px solid var(--border); background: var(--bg-3); color: var(--text-0); cursor: pointer; transition: all 0.1s; font-family: inherit; }
		.btn:hover { background: var(--bg-4); border-color: var(--text-3); }
		.btn-primary { background: var(--accent); border-color: var(--accent); }
		.btn-primary:hover { background: var(--accent-dim); }
		.btn-sm { padding: 5px 10px; font-size: 0.7rem; }
		.btn-danger { border-color: var(--error); color: var(--error); }
		.btn-danger:hover { background: var(--error); color: white; }
		.btn-block { width: 100%; }
		
		.modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.85); display: flex; align-items: center; justify-content: center; z-index: 1000; }
		.modal { background: var(--bg-2); border: 1px solid var(--border); width: 100%; max-width: 500px; max-height: 90vh; overflow: auto; }
		.modal-lg { max-width: 700px; }
		.modal-header { padding: 16px 20px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }
		.modal-title { font-size: 1rem; font-weight: 600; }
		.modal-close { background: none; border: none; color: var(--text-2); font-size: 1.5rem; cursor: pointer; line-height: 1; }
		.modal-body { padding: 20px; }
		.modal-footer { padding: 16px 20px; border-top: 1px solid var(--border); display: flex; justify-content: flex-end; gap: 8px; }
		
		.tabs { display: flex; border-bottom: 1px solid var(--border); }
		.tab { padding: 12px 16px; font-size: 0.8rem; color: var(--text-2); cursor: pointer; border-bottom: 2px solid transparent; }
		.tab:hover { color: var(--text-1); }
		.tab.active { color: var(--accent); border-bottom-color: var(--accent); }
		.tab-content { display: none; }
		.tab-content.active { display: block; }
		
		.upload-zone { border: 2px dashed var(--border); padding: 30px; text-align: center; cursor: pointer; }
		.upload-zone:hover, .upload-zone.dragover { border-color: var(--accent); }
		.upload-zone-text { color: var(--text-2); font-size: 0.85rem; }
		
		.lightbox { position: fixed; inset: 0; background: rgba(0,0,0,0.95); display: flex; align-items: center; justify-content: center; z-index: 2000; }
		.lightbox img, .lightbox video { max-width: 95vw; max-height: 95vh; object-fit: contain; }
		.lightbox-close { position: absolute; top: 16px; right: 16px; background: none; border: none; color: white; font-size: 2rem; cursor: pointer; }
		
		.toast { position: fixed; bottom: 20px; right: 20px; padding: 12px 20px; background: var(--bg-2); border: 1px solid var(--border); font-size: 0.85rem; z-index: 3000; animation: slideIn 0.2s ease; }
		.toast.success { border-left: 3px solid var(--success); }
		.toast.error { border-left: 3px solid var(--error); }
		@keyframes slideIn { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
		
		.welcome { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; padding: 40px; text-align: center; }
		.welcome h2 { font-size: 1.5rem; margin-bottom: 8px; }
		.welcome p { color: var(--text-2); margin-bottom: 24px; }
		.welcome-actions { display: flex; gap: 12px; }
		
		.empty-state { padding: 60px 20px; text-align: center; color: var(--text-2); }
		.empty-state h3 { color: var(--text-1); margin-bottom: 8px; }
		
		.hidden { display: none !important; }
		::-webkit-scrollbar { width: 8px; height: 8px; }
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
				<div class="sidebar-header"><span>Browser</span><button class="btn btn-sm" onclick="refreshTree()">↻</button></div>
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
								<input type="text" class="search-input" id="searchInput" placeholder="Search... (tag:name, key:value, key=value)" autocomplete="off">
								<div class="autocomplete-dropdown" id="autocompleteDropdown"></div>
							</div>
							<button class="btn btn-primary" onclick="executeSearch()">Search</button>
						</div>
						<div class="search-hint">
							<code>tag:value</code> tag contains &nbsp;
							<code>tag=value</code> tag equals &nbsp;
							<code>key:value</code> metadata contains &nbsp;
							<code>key=value</code> metadata equals &nbsp;
							<code>key</code> metadata exists
						</div>
						<div class="filter-bar" id="filterBar">
							<button class="filter-add" onclick="showAdvancedFilterModal()">+ Advanced Filter</button>
						</div>
					</div>
					
					<div class="results-container">
						<div class="results-header">
							<span id="resultsCount">0 results</span>
							<select class="form-select" style="width:auto;padding:4px 8px;font-size:0.75rem" onchange="setTypeFilter(this.value)">
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
					<div><div class="detail-title" id="detailTitle"></div><div style="font-size:0.75rem;color:var(--text-2);margin-top:4px" id="detailPath"></div></div>
					<button class="detail-close" onclick="closeDetailPanel()">&times;</button>
				</div>
				<div class="detail-body" id="detailBody"></div>
			</aside>
		</div>
	</div>
	
	<div class="lightbox hidden" id="lightbox" onclick="closeLightbox(event)"><button class="lightbox-close">&times;</button><div id="lightboxContent"></div></div>
	
	<script>
	let archiveOpen = false, selectedPath = null, advancedFilters = [], typeFilter = '', currentNodeData = null, expandedNodes = new Set();
	let acIndex = -1, acItems = [];
	
	async function api(method, path, body = null) {
		const opts = { method, headers: {} };
		if (body && !(body instanceof FormData)) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
		else if (body) { opts.body = body; }
		const res = await fetch(path, opts);
		const data = await res.json();
		if (!res.ok) throw new Error(data.error || 'Request failed');
		return data;
	}
	
	function toast(msg, type = 'info') { const el = document.createElement('div'); el.className = `toast ${type}`; el.textContent = msg; document.body.appendChild(el); setTimeout(() => el.remove(), 3000); }
	function formatSize(b) { if (!b) return '0 B'; const k = 1024, s = ['B','KB','MB','GB'], i = Math.floor(Math.log(b)/Math.log(k)); return parseFloat((b/Math.pow(k,i)).toFixed(1))+' '+s[i]; }
	function closeModal(el) { el.closest('.modal-overlay')?.remove(); }
	function escapeHtml(s) { return s.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
	
	// === Autocomplete ===
	const searchInput = document.getElementById('searchInput');
	const acDropdown = document.getElementById('autocompleteDropdown');
	
	searchInput.addEventListener('input', debounce(handleSearchInput, 150));
	searchInput.addEventListener('keydown', handleSearchKeydown);
	searchInput.addEventListener('blur', () => setTimeout(() => acDropdown.classList.remove('show'), 150));
	
	function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }
	
	async function handleSearchInput() {
		const val = searchInput.value;
		const cursorPos = searchInput.selectionStart;
		const { token, context, query } = parseCurrentToken(val, cursorPos);
		
		if (!token && !query) { acDropdown.classList.remove('show'); return; }
		
		try {
			const data = await api('GET', `/api/autocomplete?context=${encodeURIComponent(context)}&q=${encodeURIComponent(query)}`);
			acItems = data.suggestions || [];
			renderAutocomplete();
		} catch (e) { acItems = []; acDropdown.classList.remove('show'); }
	}
	
	function parseCurrentToken(val, pos) {
		// Find token boundaries
		let start = pos, end = pos;
		while (start > 0 && val[start-1] !== ' ') start--;
		while (end < val.length && val[end] !== ' ') end++;
		
		const token = val.substring(start, end);
		let context = 'start', query = token;
		
		if (token.toLowerCase().startsWith('tag:')) { context = 'tag:'; query = token.slice(4); }
		else if (token.toLowerCase().startsWith('tag=')) { context = 'tag='; query = token.slice(4); }
		else if (token.includes(':')) {
			const [key, v] = token.split(':', 2);
			context = `meta:${key}:`; query = v || '';
		} else if (token.includes('=')) {
			const [key, v] = token.split('=', 2);
			context = `meta:${key}=`; query = v || '';
		} else {
			context = 'key'; query = token;
		}
		
		return { token, context, query, start, end };
	}
	
	function renderAutocomplete() {
		if (acItems.length === 0) { acDropdown.classList.remove('show'); return; }
		acIndex = -1;
		acDropdown.innerHTML = acItems.map((item, i) => `
			<div class="autocomplete-item" data-index="${i}" onmousedown="selectAutocomplete(${i})">
				<span class="autocomplete-item-label">${escapeHtml(item.label)}</span>
				<span class="autocomplete-item-hint">${escapeHtml(item.hint || '')}</span>
			</div>
		`).join('');
		acDropdown.classList.add('show');
	}
	
	function handleSearchKeydown(e) {
		if (!acDropdown.classList.contains('show')) {
			if (e.key === 'Enter') { e.preventDefault(); executeSearch(); }
			return;
		}
		if (e.key === 'ArrowDown') { e.preventDefault(); acIndex = Math.min(acIndex + 1, acItems.length - 1); updateAcSelection(); }
		else if (e.key === 'ArrowUp') { e.preventDefault(); acIndex = Math.max(acIndex - 1, 0); updateAcSelection(); }
		else if (e.key === 'Enter' && acIndex >= 0) { e.preventDefault(); selectAutocomplete(acIndex); }
		else if (e.key === 'Escape') { acDropdown.classList.remove('show'); }
		else if (e.key === 'Enter') { e.preventDefault(); executeSearch(); }
	}
	
	function updateAcSelection() {
		acDropdown.querySelectorAll('.autocomplete-item').forEach((el, i) => el.classList.toggle('selected', i === acIndex));
		const sel = acDropdown.querySelector('.autocomplete-item.selected');
		if (sel) sel.scrollIntoView({ block: 'nearest' });
	}
	
	function selectAutocomplete(index) {
		const item = acItems[index];
		if (!item) return;
		
		const val = searchInput.value;
		const pos = searchInput.selectionStart;
		const { start, end, context } = parseCurrentToken(val, pos);
		
		let replacement = item.value;
		if (item.type === 'key' || item.type === 'prefix') {
			replacement = item.value + (item.value.endsWith(':') ? '' : ':');
		}
		
		// If we're completing a value after : or =, just replace the value part
		if (context.startsWith('meta:') || context.startsWith('tag')) {
			const colonPos = val.lastIndexOf(':', end);
			const eqPos = val.lastIndexOf('=', end);
			const opPos = Math.max(colonPos, eqPos);
			if (opPos > start) {
				const before = val.substring(0, opPos + 1);
				const after = val.substring(end);
				searchInput.value = before + replacement + after;
				searchInput.selectionStart = searchInput.selectionEnd = before.length + replacement.length;
				acDropdown.classList.remove('show');
				return;
			}
		}
		
		const before = val.substring(0, start);
		const after = val.substring(end);
		searchInput.value = before + replacement + after;
		searchInput.selectionStart = searchInput.selectionEnd = before.length + replacement.length;
		acDropdown.classList.remove('show');
		
		// Trigger another autocomplete if we added a prefix
		if (item.type === 'key' || item.type === 'prefix') {
			setTimeout(() => handleSearchInput(), 50);
		}
	}
	
	// === Search ===
	async function executeSearch() {
		if (!archiveOpen) return;
		acDropdown.classList.remove('show');
		
		const q = searchInput.value.trim();
		const filters = {};
		
		if (typeFilter) filters.type = typeFilter;
		
		for (const f of advancedFilters) {
			if (f.type === 'inside') filters.inside = f.value;
			else if (f.type === 'related_to') {
				const parts = f.value.split(':');
				filters.related_to = { target: parts[0], relation: parts[1] || null };
			}
		}
		
		try {
			const data = await api('POST', '/api/smart-search', { q, filters, limit: 200 });
			renderResults(data.results);
		} catch (e) { toast(e.message, 'error'); }
	}
	
	function renderResults(results) {
		const grid = document.getElementById('resultsGrid');
		document.getElementById('resultsCount').textContent = `${results.length} result${results.length !== 1 ? 's' : ''}`;
		
		if (results.length === 0) {
			grid.innerHTML = '<div class="empty-state"><h3>No results</h3><p>Try adjusting your search.</p></div>';
			return;
		}
		
		grid.innerHTML = results.map(r => {
			const meta = r.metadata || {};
			const metaKeys = Object.keys(meta).slice(0, 2);
			const tags = r.tags || [];
			
			return `
				<div class="result-card" onclick="selectNode('${escapeHtml(r.path)}')">
					<div class="result-header">
						<span class="result-icon">${r.type === 'VAULT' ? '📁' : '📄'}</span>
						<div class="result-info">
							<div class="result-name">${escapeHtml(r.name)}</div>
							<div class="result-path">${escapeHtml(r.path)}</div>
						</div>
						<span class="result-type">${r.type}</span>
					</div>
					<div class="result-meta">
						${tags.slice(0,3).map(t => `<span class="result-tag is-tag">${escapeHtml(t)}</span>`).join('')}
						${metaKeys.map(k => `<span class="result-tag">${escapeHtml(k)}: ${escapeHtml(String(meta[k]).substring(0,20))}</span>`).join('')}
					</div>
				</div>
			`;
		}).join('');
	}
	
	function setTypeFilter(t) { typeFilter = t; executeSearch(); }
	
	// === Advanced Filters ===
	function showAdvancedFilterModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header"><span class="modal-title">Add Advanced Filter</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">Filter Type</label>
						<select class="form-select" id="advFilterType">
							<option value="inside">Inside Path (prefix)</option>
							<option value="related_to">Related To (path:RELATION)</option>
						</select>
					</div>
					<div class="form-group">
						<label class="form-label">Value</label>
						<input type="text" class="form-input" id="advFilterValue" placeholder="Enter value">
					</div>
				</div>
				<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="addAdvancedFilter()">Add</button></div>
			</div>
		`;
		document.body.appendChild(modal);
	}
	
	function addAdvancedFilter() {
		const type = document.getElementById('advFilterType').value;
		const value = document.getElementById('advFilterValue').value.trim();
		if (!value) { toast('Value required', 'error'); return; }
		advancedFilters.push({ type, value });
		renderAdvancedFilters();
		closeModal(document.querySelector('.modal-overlay .modal'));
		executeSearch();
	}
	
	function removeAdvancedFilter(i) { advancedFilters.splice(i, 1); renderAdvancedFilters(); executeSearch(); }
	
	function renderAdvancedFilters() {
		const bar = document.getElementById('filterBar');
		bar.innerHTML = advancedFilters.map((f, i) => `
			<span class="filter-chip"><strong>${f.type}:</strong> ${escapeHtml(f.value)}<span class="remove" onclick="removeAdvancedFilter(${i})">×</span></span>
		`).join('') + '<button class="filter-add" onclick="showAdvancedFilterModal()">+ Advanced Filter</button>';
	}
	
	// === Archive ===
	async function checkStatus() {
		const data = await api('GET', '/api/status');
		archiveOpen = data.archive_open;
		updateUI(data);
	}
	
	function updateUI(status) {
		const info = document.getElementById('archiveInfo');
		const welcome = document.getElementById('welcomeScreen');
		const mainUI = document.getElementById('mainUI');
		const sidebar = document.getElementById('sidebar');
		
		if (status.archive_open) {
			const name = status.archive_path.split(/[/\\\\]/).pop();
			info.innerHTML = `<span class="path" title="${escapeHtml(status.archive_path)}">${escapeHtml(name)}</span>
				${status.encrypted ? '<span class="archive-badge encrypted">Encrypted</span>' : ''}
				<span style="color:var(--text-2)">${status.stats.nodes} nodes · ${formatSize(status.stats.total_size)}</span>`;
			welcome.classList.add('hidden');
			mainUI.classList.remove('hidden');
			sidebar.style.display = 'flex';
			refreshTree();
		} else {
			info.innerHTML = '<span style="color:var(--text-3)">No archive open</span>';
			welcome.classList.remove('hidden');
			mainUI.classList.add('hidden');
			sidebar.style.display = 'none';
			closeDetailPanel();
		}
	}
	
	function showOpenArchiveModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Open Archive</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">Path</label><input type="text" class="form-input" id="openPath" placeholder="/path/to/archive"></div>
			<div class="form-group"><label class="form-label">Password (if encrypted)</label><input type="password" class="form-input" id="openPassword"></div></div>
			<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="openArchive()">Open</button></div></div>`;
		document.body.appendChild(modal);
		modal.querySelector('#openPath').focus();
	}
	
	async function openArchive() {
		const path = document.getElementById('openPath').value.trim();
		const password = document.getElementById('openPassword').value || null;
		if (!path) { toast('Path required', 'error'); return; }
		try { await api('POST', '/api/archive/open', { path, password }); closeModal(document.querySelector('.modal-overlay .modal')); await checkStatus(); toast('Opened', 'success'); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	function showCreateArchiveModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Create Archive</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">Path</label><input type="text" class="form-input" id="createArchivePath" placeholder="/path/to/archive"></div>
			<div class="form-group"><label class="form-label">Password</label><input type="password" class="form-input" id="createArchivePassword" placeholder="Leave empty for no encryption"></div>
			<div class="form-group"><label class="form-label">Partition Size</label><select class="form-select" id="createArchivePartition">
				<option value="0">Disabled</option><option value="26214400">25 MB</option><option value="52428800" selected>50 MB</option><option value="104857600">100 MB</option>
			</select></div></div>
			<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="createArchive()">Create</button></div></div>`;
		document.body.appendChild(modal);
	}
	
	async function createArchive() {
		const path = document.getElementById('createArchivePath').value.trim();
		const password = document.getElementById('createArchivePassword').value || null;
		const partition_size = parseInt(document.getElementById('createArchivePartition').value);
		if (!path) { toast('Path required', 'error'); return; }
		try { await api('POST', '/api/archive/create', { path, password, partition_size }); closeModal(document.querySelector('.modal-overlay .modal')); await checkStatus(); toast('Created', 'success'); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	// === Tree ===
	async function loadChildren(parentUuid, container, depth = 0) {
		try {
			const data = await api('GET', `/api/children/${parentUuid || ''}`);
			for (const c of data.children) {
				const node = document.createElement('div');
				node.className = 'tree-node';
				node.dataset.uuid = c.uuid;
				node.dataset.path = c.path;
				
				const row = document.createElement('div');
				row.className = 'tree-item';
				row.style.paddingLeft = `${12 + depth * 14}px`;
				
				const toggle = document.createElement('span');
				toggle.className = `tree-toggle ${c.hasChildren ? '' : 'hidden'} ${expandedNodes.has(c.uuid) ? 'expanded' : ''}`;
				toggle.textContent = '▶';
				
				const icon = document.createElement('span');
				icon.className = 'tree-icon';
				icon.textContent = c.type === 'VAULT' ? '📁' : '📄';
				
				const name = document.createElement('span');
				name.className = 'tree-name';
				name.textContent = c.name;
				
				row.append(toggle, icon, name);
				row.onclick = (e) => { e.stopPropagation(); if (e.target === toggle && c.hasChildren) toggleTreeNode(c.uuid, node, depth); else selectNode(c.path); };
				
				const cc = document.createElement('div');
				cc.className = `tree-children ${expandedNodes.has(c.uuid) ? 'expanded' : ''}`;
				node.append(row, cc);
				container.appendChild(node);
				
				if (expandedNodes.has(c.uuid) && c.hasChildren) await loadChildren(c.uuid, cc, depth + 1);
			}
		} catch (e) { console.error(e); }
	}
	
	async function toggleTreeNode(uuid, node, depth) {
		const toggle = node.querySelector('.tree-toggle');
		const cc = node.querySelector('.tree-children');
		if (expandedNodes.has(uuid)) { expandedNodes.delete(uuid); toggle.classList.remove('expanded'); cc.classList.remove('expanded'); cc.innerHTML = ''; }
		else { expandedNodes.add(uuid); toggle.classList.add('expanded'); cc.classList.add('expanded'); await loadChildren(uuid, cc, depth + 1); }
	}
	
	async function refreshTree() { document.getElementById('treeContainer').innerHTML = ''; await loadChildren(null, document.getElementById('treeContainer'), 0); }
	
	// === Detail Panel ===
	async function selectNode(path) {
		selectedPath = path;
		document.querySelectorAll('.tree-item').forEach(el => el.classList.remove('selected'));
		const tn = document.querySelector(`[data-path="${path}"] > .tree-item`);
		if (tn) tn.classList.add('selected');
		try { const node = await api('GET', `/api/node/${encodeURIComponent(path)}`); currentNodeData = node; renderDetailPanel(node); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	function renderDetailPanel(node) {
		const panel = document.getElementById('detailPanel');
		panel.classList.remove('hidden');
		document.getElementById('detailTitle').textContent = node.name;
		document.getElementById('detailPath').textContent = node.path;
		
		let html = '';
		const meta = node.metadata || {};
		
		html += `<div class="detail-section"><div class="detail-section-title">Metadata <button class="btn btn-sm" onclick="showEditMetaModal()">Edit</button></div>
			${Object.keys(meta).length > 0 ? `<div class="detail-meta-grid">${Object.entries(meta).map(([k,v]) => `
				<div class="detail-meta-item ${String(v).length > 50 ? 'full' : ''}"><div class="detail-meta-label">${escapeHtml(k)}</div>
				<div class="detail-meta-value">${escapeHtml(typeof v === 'object' ? JSON.stringify(v) : String(v))}</div></div>`).join('')}</div>` : '<div style="color:var(--text-3);font-size:0.85rem">No metadata</div>'}</div>`;
		
		html += `<div class="detail-section"><div class="detail-section-title">Tags</div><div class="detail-tags" id="detailTags">
			${node.tags.map(t => `<span class="detail-tag">${escapeHtml(t)}<span class="remove" onclick="removeTag('${escapeHtml(t)}')">×</span></span>`).join('')}
			<button class="btn btn-sm" onclick="showAddTagModal()">+ Add</button></div></div>`;
		
		html += `<div class="detail-section"><div class="detail-section-title">Relationships</div>`;
		for (const rel of node.relationships) {
			html += `<div class="detail-rel-item"><span class="detail-rel-type">${escapeHtml(rel.relation)}</span>
				<span class="detail-rel-path" onclick="selectNode('${escapeHtml(rel.target_path)}')">${escapeHtml(rel.target_path)}</span>
				<span class="detail-rel-dir">→</span><span class="detail-rel-remove" onclick="removeRelationship('${escapeHtml(node.path)}','${escapeHtml(rel.target_path)}','${escapeHtml(rel.relation)}')">×</span></div>`;
		}
		for (const rel of node.incoming_relationships) {
			html += `<div class="detail-rel-item"><span class="detail-rel-type">${escapeHtml(rel.relation)}</span>
				<span class="detail-rel-path" onclick="selectNode('${escapeHtml(rel.source_path)}')">${escapeHtml(rel.source_path)}</span>
				<span class="detail-rel-dir">←</span></div>`;
		}
		html += `<button class="btn btn-sm" style="margin-top:8px" onclick="showAddRelModal()">+ Add</button></div>`;
		
		if (node.files.length > 0 || node.type === 'RECORD') {
			html += `<div class="detail-section"><div class="detail-section-title">Files (${node.files.length})</div>`;
			if (node.files.length > 0) {
				html += `<div class="detail-files-grid">`;
				for (const f of node.files) {
					const isImg = ['jpg','jpeg','png','gif','webp','bmp'].includes(f.ext);
					const isVid = ['mp4','webm','mov'].includes(f.ext);
					html += `<div class="detail-file" onclick="openFile('${f.hash}','${f.ext}','${escapeHtml(f.name)}')">
						<div class="detail-file-preview">${isImg ? `<img src="/api/blob/${f.hash}" loading="lazy">` : isVid ? `<video src="/api/blob/${f.hash}" muted></video>` : '<span class="detail-file-icon">📎</span>'}</div>
						<div class="detail-file-info"><div class="detail-file-name" title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</div><div class="detail-file-size">${formatSize(f.size)}</div></div></div>`;
				}
				html += `</div>`;
			}
			html += `<button class="btn btn-sm btn-block" style="margin-top:12px" onclick="showUploadModal()">+ Upload</button></div>`;
		}
		
		html += `<div class="detail-section"><div class="detail-section-title">Actions</div><button class="btn btn-sm btn-danger" onclick="deleteNode()">Delete Node</button></div>`;
		
		document.getElementById('detailBody').innerHTML = html;
	}
	
	function closeDetailPanel() { document.getElementById('detailPanel').classList.add('hidden'); selectedPath = null; currentNodeData = null; }
	
	// === CRUD Modals ===
	function showEditMetaModal() {
		if (!currentNodeData) return;
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal modal-lg"><div class="modal-header"><span class="modal-title">Edit Metadata</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">Path</label><input type="text" class="form-input" value="${escapeHtml(currentNodeData.path)}" readonly></div>
			<div class="form-group"><label class="form-label">Metadata (JSON)</label><textarea class="form-textarea" id="editMetaValue" style="min-height:250px">${escapeHtml(JSON.stringify(currentNodeData.metadata || {}, null, 2))}</textarea></div></div>
			<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="saveMetadata()">Save</button></div></div>`;
		document.body.appendChild(modal);
	}
	
	async function saveMetadata() {
		if (!currentNodeData) return;
		try { const metadata = JSON.parse(document.getElementById('editMetaValue').value);
			await api('POST', '/api/node/update', { path: currentNodeData.path, metadata });
			toast('Updated', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); selectNode(currentNodeData.path); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	function showAddTagModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Add Tag</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">Tag</label><input type="text" class="form-input" id="newTagInput" placeholder="tag_name"></div></div>
			<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="addTag()">Add</button></div></div>`;
		document.body.appendChild(modal);
	}
	
	async function addTag() {
		if (!currentNodeData) return;
		const tag = document.getElementById('newTagInput').value.trim();
		if (!tag) { toast('Tag required', 'error'); return; }
		try { await api('POST', '/api/tag', { path: currentNodeData.path, tag }); toast('Added', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); selectNode(currentNodeData.path); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	async function removeTag(tag) { if (!currentNodeData) return; try { await api('DELETE', '/api/tag', { path: currentNodeData.path, tag }); toast('Removed', 'success'); selectNode(currentNodeData.path); } catch (e) { toast(e.message, 'error'); } }
	
	function showAddRelModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Add Relationship</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">Source</label><input type="text" class="form-input" value="${escapeHtml(currentNodeData?.path || '')}" readonly></div>
			<div class="form-group"><label class="form-label">Relation</label><input type="text" class="form-input" id="relName" placeholder="RELATED_TO"></div>
			<div class="form-group"><label class="form-label">Target Path</label><input type="text" class="form-input" id="relTarget" placeholder="path/to/target"></div></div>
			<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="addRelationship()">Add</button></div></div>`;
		document.body.appendChild(modal);
	}
	
	async function addRelationship() {
		if (!currentNodeData) return;
		const relation = document.getElementById('relName').value.trim();
		const target = document.getElementById('relTarget').value.trim();
		if (!relation || !target) { toast('All fields required', 'error'); return; }
		try { await api('POST', '/api/link', { source: currentNodeData.path, target, relation }); toast('Created', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); selectNode(currentNodeData.path); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	async function removeRelationship(source, target, relation) { try { await api('DELETE', '/api/link', { source, target, relation }); toast('Removed', 'success'); selectNode(source); } catch (e) { toast(e.message, 'error'); } }
	
	function showUploadModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Upload Files</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">Target</label><input type="text" class="form-input" id="uploadTargetPath" value="${escapeHtml(currentNodeData?.path || '')}" readonly></div>
			<div class="upload-zone" id="uploadZone" onclick="document.getElementById('uploadInput').click()"><div style="font-size:2rem;margin-bottom:8px">📁</div><div class="upload-zone-text">Drop files or click</div>
			<input type="file" id="uploadInput" multiple style="display:none" onchange="handleUpload()"></div></div></div>`;
		document.body.appendChild(modal);
		const zone = modal.querySelector('#uploadZone');
		zone.ondragover = e => { e.preventDefault(); zone.classList.add('dragover'); };
		zone.ondragleave = () => zone.classList.remove('dragover');
		zone.ondrop = async e => { e.preventDefault(); zone.classList.remove('dragover'); await uploadFiles(e.dataTransfer.files); };
	}
	
	async function handleUpload() { await uploadFiles(document.getElementById('uploadInput').files); }
	async function uploadFiles(files) {
		const path = document.getElementById('uploadTargetPath').value;
		const fd = new FormData();
		fd.append('path', path);
		for (const f of files) fd.append('file', f, f.name);
		try { await api('POST', '/api/upload', fd); toast(`Uploaded ${files.length}`, 'success'); closeModal(document.querySelector('.modal-overlay .modal')); selectNode(path); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	async function deleteNode() {
		if (!currentNodeData) return;
		if (!confirm(`Delete "${currentNodeData.path}"?`)) return;
		try { await api('DELETE', `/api/node/${encodeURIComponent(currentNodeData.path)}`); toast('Deleted', 'success'); closeDetailPanel(); refreshTree(); executeSearch(); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	function showCreateModal() {
		if (!archiveOpen) { toast('Open archive first', 'error'); return; }
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Create Node</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">Type</label><select class="form-select" id="createNodeType"><option value="VAULT">Vault</option><option value="RECORD">Record</option></select></div>
			<div class="form-group"><label class="form-label">Path</label><input type="text" class="form-input" id="createNodePath" placeholder="parent/name"></div>
			<div class="form-group"><label class="form-label">Metadata (JSON)</label><textarea class="form-textarea" id="createNodeMeta" placeholder='{"key":"value"}'></textarea></div></div>
			<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="createNode()">Create</button></div></div>`;
		document.body.appendChild(modal);
	}
	
	async function createNode() {
		const type = document.getElementById('createNodeType').value;
		const path = document.getElementById('createNodePath').value.trim();
		let metadata = {};
		try { const m = document.getElementById('createNodeMeta').value.trim(); if (m) metadata = JSON.parse(m); } catch { toast('Invalid JSON', 'error'); return; }
		if (!path) { toast('Path required', 'error'); return; }
		try { await api('POST', type === 'VAULT' ? '/api/vault' : '/api/record', { path, metadata }); toast('Created', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); refreshTree(); executeSearch(); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	// === Extractor & Settings ===
	async function showExtractorModal() {
		if (!archiveOpen) { toast('Open archive first', 'error'); return; }
		let extractors = [];
		try { extractors = (await api('GET', '/api/extractors')).extractors; } catch { toast('Failed to load extractors', 'error'); return; }
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal modal-lg"><div class="modal-header"><span class="modal-title">Extract</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="form-group"><label class="form-label">URL</label><input type="text" class="form-input" id="extractUrl" placeholder="https://...">
			<div class="form-hint">Extractors: ${extractors.map(e => e.name).join(', ')}</div></div>
			<div class="form-group"><label class="form-label">Cookies</label><textarea class="form-textarea" id="extractCookies" placeholder="Netscape format..."></textarea></div>
			<div class="form-group"><label class="form-label">Config (JSON)</label><textarea class="form-textarea" id="extractConfig" placeholder='{"password":"..."}'></textarea></div></div>
			<div class="modal-footer"><button class="btn" onclick="closeModal(this)">Cancel</button><button class="btn btn-primary" onclick="runExtractor()">Extract</button></div></div>`;
		document.body.appendChild(modal);
	}
	
	async function runExtractor() {
		const url = document.getElementById('extractUrl').value.trim();
		const cookies = document.getElementById('extractCookies').value;
		let config = {};
		try { const c = document.getElementById('extractConfig').value.trim(); if (c) config = JSON.parse(c); } catch { toast('Invalid JSON', 'error'); return; }
		if (!url) { toast('URL required', 'error'); return; }
		try { toast('Extracting...', 'info'); closeModal(document.querySelector('.modal-overlay .modal'));
			await api('POST', '/api/extract', { url, cookies, config }); toast('Done', 'success'); refreshTree(); executeSearch(); }
		catch (e) { toast(e.message, 'error'); }
	}
	
	async function showSettingsModal() {
		if (!archiveOpen) { showOpenArchiveModal(); return; }
		let config; try { config = await api('GET', '/api/config'); } catch { toast('Failed', 'error'); return; }
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `<div class="modal"><div class="modal-header"><span class="modal-title">Settings</span><button class="modal-close" onclick="closeModal(this)">&times;</button></div>
			<div class="modal-body"><div class="tabs"><div class="tab active" data-tab="general" onclick="switchTab('general')">General</div><div class="tab" data-tab="encryption" onclick="switchTab('encryption')">Encryption</div><div class="tab" data-tab="actions" onclick="switchTab('actions')">Actions</div></div>
			<div class="tab-content active" id="tabGeneral" style="padding-top:16px">
				<div class="form-group"><label class="form-label">Path</label><input class="form-input" value="${escapeHtml(config.path)}" readonly></div>
				<div class="form-group"><label class="form-label">Partition Size</label><select class="form-select" id="settingsPartition">
					<option value="0" ${config.partition_size===0?'selected':''}>Disabled</option><option value="26214400" ${config.partition_size===26214400?'selected':''}>25 MB</option>
					<option value="52428800" ${config.partition_size===52428800?'selected':''}>50 MB</option><option value="104857600" ${config.partition_size===104857600?'selected':''}>100 MB</option>
				</select><button class="btn btn-sm" style="margin-top:8px" onclick="updatePartition()">Update</button></div></div>
			<div class="tab-content" id="tabEncryption" style="padding-top:16px">
				<p style="margin-bottom:16px;color:var(--text-2)">Status: ${config.encrypted ? '<span style="color:var(--success)">Enabled</span>' : 'Disabled'}</p>
				${config.encrypted ? `<div class="form-group"><label class="form-label">Current Password</label><input type="password" class="form-input" id="encOldPass"></div>
					<div class="form-group"><label class="form-label">New Password (empty to disable)</label><input type="password" class="form-input" id="encNewPass"></div>
					<button class="btn" onclick="updateEncryption()">Update</button>`
				: `<div class="form-group"><label class="form-label">Password</label><input type="password" class="form-input" id="encNewPass"></div><button class="btn" onclick="enableEncryption()">Enable</button>`}</div>
			<div class="tab-content" id="tabActions" style="padding-top:16px">
				<div class="form-group"><button class="btn btn-block" onclick="generateStatic()">Generate Static Site</button></div>
				<div class="form-group"><button class="btn btn-block" onclick="closeArchiveAction()">Close Archive</button></div></div></div></div>`;
		document.body.appendChild(modal);
	}
	
	function switchTab(tab) { document.querySelectorAll('.modal .tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab)); document.querySelectorAll('.modal .tab-content').forEach(c => c.classList.remove('active')); document.getElementById('tab' + tab.charAt(0).toUpperCase() + tab.slice(1)).classList.add('active'); }
	async function updatePartition() { try { await api('POST', '/api/config/partition', { size: parseInt(document.getElementById('settingsPartition').value) }); toast('Updated', 'success'); } catch (e) { toast(e.message, 'error'); } }
	async function enableEncryption() { const p = document.getElementById('encNewPass').value; if (!p) { toast('Password required', 'error'); return; } try { await api('POST', '/api/config/encryption', { action: 'enable', password: p }); toast('Enabled', 'success'); closeModal(document.querySelector('.modal-overlay .modal')); checkStatus(); } catch (e) { toast(e.message, 'error'); } }
	async function updateEncryption() { const o = document.getElementById('encOldPass').value, n = document.getElementById('encNewPass').value; if (!o) { toast('Current password required', 'error'); return; } try { if (n) { await api('POST', '/api/config/encryption', { action: 'change_password', old_password: o, new_password: n }); toast('Changed', 'success'); } else { await api('POST', '/api/config/encryption', { action: 'disable', password: o }); toast('Disabled', 'success'); } closeModal(document.querySelector('.modal-overlay .modal')); checkStatus(); } catch (e) { toast(e.message, 'error'); } }
	async function generateStatic() { try { await api('POST', '/api/generate-static'); toast('Generated', 'success'); } catch (e) { toast(e.message, 'error'); } }
	async function closeArchiveAction() { await api('POST', '/api/archive/close'); closeModal(document.querySelector('.modal-overlay .modal')); checkStatus(); }
	
	// === Lightbox ===
	function openFile(hash, ext, name) {
		const isImg = ['jpg','jpeg','png','gif','webp','bmp'].includes(ext);
		const isVid = ['mp4','webm','mov'].includes(ext);
		if (isImg) { document.getElementById('lightboxContent').innerHTML = `<img src="/api/blob/${hash}">`; document.getElementById('lightbox').classList.remove('hidden'); }
		else if (isVid) { document.getElementById('lightboxContent').innerHTML = `<video src="/api/blob/${hash}" controls autoplay></video>`; document.getElementById('lightbox').classList.remove('hidden'); }
		else { const a = document.createElement('a'); a.href = `/api/blob/${hash}`; a.download = name; a.click(); }
	}
	function closeLightbox(e) { if (!e || e.target.id === 'lightbox' || e.target.classList.contains('lightbox-close')) { document.getElementById('lightbox').classList.add('hidden'); document.getElementById('lightboxContent').innerHTML = ''; } }
	
	document.addEventListener('keydown', e => { if (e.key === 'Escape') { closeLightbox(); document.querySelector('.modal-overlay')?.remove(); } });
	
	checkStatus();
	</script>
</body>
</html>'''