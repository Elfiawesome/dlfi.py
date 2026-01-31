import os
import json
import logging
import mimetypes
import io
import traceback
from pathlib import Path
from typing import Optional, Dict, Any
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse, unquote
import threading

logger = logging.getLogger(__name__)


class DLFIServer:
	"""Web server for DLFI archive management."""
	
	def __init__(self, host: str = "127.0.0.1", port: int = 8080):
		self.host = host
		self.port = port
		self.dlfi = None  # No archive loaded initially
		self.archive_path = None
		self._server = None
		self._extraction_logs = []
	
	def start(self, blocking: bool = True):
		"""Start the web server."""
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
		"""Stop the web server."""
		if self._server:
			self._server.shutdown()
			if self.dlfi:
				self.dlfi.close()
	
	def open_archive(self, path: str, password: Optional[str] = None):
		"""Open or create an archive."""
		from .core import DLFI
		
		if self.dlfi:
			self.dlfi.close()
		
		self.archive_path = Path(path).resolve()
		self.dlfi = DLFI(str(self.archive_path), password=password)
		logger.info(f"Opened archive: {self.archive_path}")
	
	def close_archive(self):
		"""Close current archive."""
		if self.dlfi:
			self.dlfi.close()
			self.dlfi = None
			self.archive_path = None
	
	def _create_handler(self):
		"""Create request handler with access to server instance."""
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
				"""Parse multipart form data."""
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
				"""Check if archive is open, send error if not."""
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
				elif path == "/api/query":
					self.api_query()
				elif path == "/api/search":
					self.api_search()
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
				elif path.startswith("/api/tag/"):
					self.api_remove_tag()
				else:
					self.send_error_json("Not found", 404)
			
			# === Archive Management ===
			
			def api_status(self):
				"""Get server and archive status."""
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
				"""Open an existing archive."""
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					password = body.get("password")
					
					if not path:
						self.send_error_json("Path is required")
						return
					
					if not Path(path).exists():
						self.send_error_json(f"Path does not exist: {path}")
						return
					
					server.open_archive(path, password)
					self.send_json({"success": True, "path": str(server.archive_path)})
				except Exception as e:
					logger.error(f"Failed to open archive: {e}")
					self.send_error_json(str(e), 500)
			
			def api_create_archive(self):
				"""Create a new archive."""
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
				"""Close current archive."""
				server.close_archive()
				self.send_json({"success": True})
			
			def api_get_config(self):
				"""Get archive configuration."""
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
				"""List available extractors."""
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
				"""Run an extractor job."""
				if not self.require_archive():
					return
				
				try:
					body = self.read_json_body()
					url = body.get("url", "").strip()
					cookies_content = body.get("cookies", "")  # Cookie file content as string
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
					
					# Write cookies to temp file if provided
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
						
						# Run extraction
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
				"""Get full tree structure."""
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
				"""Get children of a node."""
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
				"""Get detailed node info."""
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
					
					# Tags
					node["tags"] = [r[0] for r in server.dlfi.conn.execute(
						"SELECT tag FROM tags WHERE node_uuid = ?", (row[0],)
					)]
					
					# Outgoing relationships
					node["relationships"] = [
						{"relation": r[0], "target_path": r[1], "target_uuid": r[2]}
						for r in server.dlfi.conn.execute("""
							SELECT e.relation, n.cached_path, e.target_uuid
							FROM edges e LEFT JOIN nodes n ON e.target_uuid = n.uuid
							WHERE e.source_uuid = ?
						""", (row[0],))
					]
					
					# Incoming relationships
					node["incoming_relationships"] = [
						{"relation": r[0], "source_path": r[1], "source_uuid": r[2]}
						for r in server.dlfi.conn.execute("""
							SELECT e.relation, n.cached_path, e.source_uuid
							FROM edges e LEFT JOIN nodes n ON e.source_uuid = n.uuid
							WHERE e.target_uuid = ?
						""", (row[0],))
					]
					
					# Files
					node["files"] = [
						{"name": r[0], "hash": r[1], "size": r[2], "ext": r[3], "parts": r[4]}
						for r in server.dlfi.conn.execute("""
							SELECT nf.original_name, nf.file_hash, b.size_bytes, b.ext, b.part_count
							FROM node_files nf JOIN blobs b ON nf.file_hash = b.hash
							WHERE nf.node_uuid = ? ORDER BY nf.display_order
						""", (row[0],))
					]
					
					# Children count
					node["children_count"] = server.dlfi.conn.execute(
						"SELECT COUNT(*) FROM nodes WHERE parent_uuid = ?", (row[0],)
					).fetchone()[0]
					
					self.send_json(node)
				except Exception as e:
					self.send_error_json(str(e), 500)
			
			def api_get_blob(self, blob_hash: str):
				"""Stream blob content."""
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
						server.dlfi.conn.execute(
							"DELETE FROM tags WHERE node_uuid = ? AND tag = ?", (uuid, tag.lower())
						)
						server.dlfi.conn.commit()
					
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
			
			def api_update_node(self):
				"""Update node metadata."""
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					metadata = body.get("metadata", {})
					
					if not path:
						self.send_error_json("Path is required")
						return
					
					uuid = server.dlfi._resolve_path(path)
					if not uuid:
						self.send_error_json("Node not found", 404)
						return
					
					import time
					server.dlfi.conn.execute(
						"UPDATE nodes SET metadata = ?, last_modified = ? WHERE uuid = ?",
						(json.dumps(metadata), time.time(), uuid)
					)
					server.dlfi.conn.commit()
					
					self.send_json({"success": True})
				except Exception as e:
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
			
			# === Query & Search ===
			
			def api_query(self):
				"""Execute structured query."""
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
					
					if body.get("contains_related"):
						rel = body["contains_related"]
						if isinstance(rel, dict):
							qb.contains_related(rel.get("target", ""), rel.get("relation"))
						else:
							qb.contains_related(rel)
					
					if body.get("limit"):
						qb.limit(int(body["limit"]))
					
					if body.get("offset"):
						qb.offset(int(body["offset"]))
					
					results = qb.execute()
					self.send_json({"results": results, "count": len(results)})
				except Exception as e:
					logger.error(f"Query error: {e}", exc_info=True)
					self.send_error_json(str(e), 500)
			
			def api_search(self):
				"""Full-text search across paths, names, metadata, and tags."""
				if not self.require_archive():
					return
				try:
					body = self.read_json_body()
					query_text = body.get("q", "").strip().lower()
					node_type = body.get("type")
					limit = body.get("limit", 100)
					
					if not query_text:
						self.send_json({"results": [], "count": 0})
						return
					
					# Build search query
					sql = """
						SELECT DISTINCT n.uuid, n.cached_path, n.type, n.name, n.metadata
						FROM nodes n
						LEFT JOIN tags t ON n.uuid = t.node_uuid
						WHERE (
							LOWER(n.cached_path) LIKE ? OR
							LOWER(n.name) LIKE ? OR
							LOWER(n.metadata) LIKE ? OR
							LOWER(t.tag) LIKE ?
						)
					"""
					params = [f"%{query_text}%"] * 4
					
					if node_type:
						sql += " AND n.type = ?"
						params.append(node_type)
					
					sql += " ORDER BY n.cached_path LIMIT ?"
					params.append(limit)
					
					cursor = server.dlfi.conn.execute(sql, params)
					
					results = []
					for row in cursor:
						meta = json.loads(row[4]) if row[4] else {}
						results.append({
							"uuid": row[0],
							"path": row[1],
							"type": row[2],
							"name": row[3],
							"metadata": meta
						})
					
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
			
			# === HTML ===
			
			def serve_html(self):
				html = get_app_html()
				self.send_response(200)
				self.send_header("Content-Type", "text/html; charset=utf-8")
				self.end_headers()
				self.wfile.write(html.encode("utf-8"))
		
		return RequestHandler


def get_app_html() -> str:
	"""Return the complete application HTML."""
	return '''<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>DLFI Archive Manager</title>
	<style>
		* { margin: 0; padding: 0; box-sizing: border-box; }
		
		:root {
			--bg-0: #000000;
			--bg-1: #0a0a0a;
			--bg-2: #121212;
			--bg-3: #1a1a1a;
			--bg-4: #222222;
			--text-0: #ffffff;
			--text-1: #cccccc;
			--text-2: #888888;
			--text-3: #555555;
			--accent: #3b82f6;
			--accent-dim: #1e40af;
			--success: #22c55e;
			--warning: #f59e0b;
			--error: #ef4444;
			--border: #2a2a2a;
		}
		
		body {
			font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
			background: var(--bg-1);
			color: var(--text-0);
			height: 100vh;
			overflow: hidden;
		}
		
		/* Layout */
		.app { display: flex; flex-direction: column; height: 100vh; }
		
		.header {
			background: var(--bg-0);
			border-bottom: 1px solid var(--border);
			padding: 0 20px;
			height: 52px;
			display: flex;
			align-items: center;
			justify-content: space-between;
			flex-shrink: 0;
		}
		
		.header-left { display: flex; align-items: center; gap: 20px; }
		.logo { font-weight: 700; font-size: 1rem; letter-spacing: -0.03em; }
		
		.archive-info {
			display: flex; align-items: center; gap: 12px;
			font-size: 0.8rem; color: var(--text-2);
		}
		.archive-info .path { color: var(--text-1); max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.archive-badge { padding: 2px 8px; font-size: 0.65rem; text-transform: uppercase; background: var(--bg-3); }
		.archive-badge.encrypted { background: var(--accent-dim); color: var(--accent); }
		
		.header-actions { display: flex; gap: 8px; }
		
		.main { display: flex; flex: 1; overflow: hidden; }
		
		/* Sidebar */
		.sidebar {
			width: 260px;
			background: var(--bg-2);
			border-right: 1px solid var(--border);
			display: flex;
			flex-direction: column;
			flex-shrink: 0;
		}
		
		.sidebar-section { border-bottom: 1px solid var(--border); }
		.sidebar-header {
			padding: 12px 16px;
			font-size: 0.7rem;
			text-transform: uppercase;
			letter-spacing: 0.05em;
			color: var(--text-2);
			display: flex;
			justify-content: space-between;
			align-items: center;
		}
		
		.tree-container { flex: 1; overflow-y: auto; padding: 4px 0; }
		
		.tree-item {
			display: flex; align-items: center;
			padding: 6px 12px; cursor: pointer;
			font-size: 0.82rem; color: var(--text-1);
			transition: background 0.1s;
		}
		.tree-item:hover { background: var(--bg-3); }
		.tree-item.selected { background: var(--accent); color: white; }
		
		.tree-toggle {
			width: 16px; height: 16px; font-size: 8px;
			display: flex; align-items: center; justify-content: center;
			color: var(--text-3); margin-right: 4px;
			transition: transform 0.1s;
		}
		.tree-toggle.expanded { transform: rotate(90deg); }
		.tree-toggle.hidden { visibility: hidden; }
		.tree-icon { margin-right: 8px; font-size: 13px; }
		.tree-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.tree-children { display: none; }
		.tree-children.expanded { display: block; }
		
		/* Content */
		.content { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
		
		/* Search Bar */
		.search-container {
			padding: 16px 24px;
			background: var(--bg-2);
			border-bottom: 1px solid var(--border);
		}
		
		.search-bar {
			display: flex; gap: 12px; align-items: stretch;
		}
		
		.search-input-wrap {
			flex: 1; position: relative;
		}
		
		.search-input {
			width: 100%; padding: 12px 16px;
			background: var(--bg-1); border: 1px solid var(--border);
			color: var(--text-0); font-size: 0.95rem;
			outline: none; transition: border-color 0.1s;
		}
		.search-input:focus { border-color: var(--accent); }
		.search-input::placeholder { color: var(--text-3); }
		
		.search-filters { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
		
		.filter-chip {
			display: flex; align-items: center; gap: 6px;
			padding: 4px 10px; background: var(--bg-3);
			border: 1px solid var(--border); font-size: 0.75rem;
			color: var(--text-1); cursor: default;
		}
		.filter-chip .remove { cursor: pointer; opacity: 0.5; }
		.filter-chip .remove:hover { opacity: 1; }
		
		.add-filter-btn {
			padding: 4px 10px; background: transparent;
			border: 1px dashed var(--border); font-size: 0.75rem;
			color: var(--text-2); cursor: pointer;
		}
		.add-filter-btn:hover { border-color: var(--text-2); color: var(--text-1); }
		
		/* Results */
		.results-container { flex: 1; overflow-y: auto; padding: 16px 24px; }
		
		.results-header {
			display: flex; justify-content: space-between; align-items: center;
			margin-bottom: 16px; font-size: 0.8rem; color: var(--text-2);
		}
		
		.results-grid {
			display: grid;
			grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
			gap: 12px;
		}
		
		.result-card {
			background: var(--bg-2); border: 1px solid var(--border);
			padding: 14px; cursor: pointer; transition: border-color 0.1s;
		}
		.result-card:hover { border-color: var(--accent); }
		
		.result-header { display: flex; align-items: flex-start; gap: 10px; margin-bottom: 8px; }
		.result-icon { font-size: 1.2rem; }
		.result-info { flex: 1; min-width: 0; }
		.result-name { font-weight: 500; font-size: 0.9rem; margin-bottom: 2px; word-break: break-word; }
		.result-path { font-size: 0.75rem; color: var(--text-2); word-break: break-all; }
		.result-type { font-size: 0.6rem; text-transform: uppercase; padding: 2px 6px; background: var(--bg-3); color: var(--text-2); }
		
		.result-meta { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
		.result-tag { font-size: 0.7rem; padding: 2px 8px; background: var(--bg-4); color: var(--text-2); }
		
		/* Detail Panel */
		.detail-panel {
			width: 380px; background: var(--bg-2);
			border-left: 1px solid var(--border);
			display: flex; flex-direction: column;
			flex-shrink: 0; overflow: hidden;
		}
		.detail-panel.hidden { display: none; }
		
		.detail-header {
			padding: 16px; border-bottom: 1px solid var(--border);
			display: flex; justify-content: space-between; align-items: flex-start;
		}
		.detail-title { font-size: 1rem; font-weight: 600; word-break: break-word; }
		.detail-close { background: none; border: none; color: var(--text-2); font-size: 1.2rem; cursor: pointer; }
		
		.detail-body { flex: 1; overflow-y: auto; padding: 16px; }
		
		.detail-section { margin-bottom: 24px; }
		.detail-section-title {
			font-size: 0.65rem; text-transform: uppercase;
			letter-spacing: 0.05em; color: var(--text-2); margin-bottom: 10px;
		}
		
		.detail-meta-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
		.detail-meta-item { background: var(--bg-3); padding: 10px; }
		.detail-meta-item.full { grid-column: span 2; }
		.detail-meta-label { font-size: 0.65rem; color: var(--text-2); margin-bottom: 4px; text-transform: uppercase; }
		.detail-meta-value { font-size: 0.85rem; word-break: break-word; }
		
		.detail-tags { display: flex; flex-wrap: wrap; gap: 6px; }
		.detail-tag {
			display: flex; align-items: center; gap: 4px;
			padding: 4px 10px; background: var(--bg-3);
			font-size: 0.75rem; color: var(--text-1);
		}
		.detail-tag .remove { cursor: pointer; opacity: 0.5; font-size: 0.9rem; }
		.detail-tag .remove:hover { opacity: 1; }
		
		.detail-rel-item {
			display: flex; align-items: center; gap: 10px;
			padding: 10px; background: var(--bg-3); margin-bottom: 6px; cursor: pointer;
		}
		.detail-rel-item:hover { background: var(--bg-4); }
		.detail-rel-type { font-size: 0.65rem; text-transform: uppercase; color: var(--accent); font-weight: 600; min-width: 80px; }
		.detail-rel-path { font-size: 0.8rem; color: var(--text-1); flex: 1; word-break: break-all; }
		.detail-rel-dir { font-size: 0.6rem; color: var(--text-3); }
		
		.detail-files-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; }
		.detail-file {
			background: var(--bg-3); cursor: pointer;
			border: 1px solid transparent; transition: border-color 0.1s;
		}
		.detail-file:hover { border-color: var(--accent); }
		.detail-file-preview {
			aspect-ratio: 1; background: var(--bg-4);
			display: flex; align-items: center; justify-content: center;
			overflow: hidden;
		}
		.detail-file-preview img, .detail-file-preview video { width: 100%; height: 100%; object-fit: cover; }
		.detail-file-icon { font-size: 1.5rem; color: var(--text-3); }
		.detail-file-info { padding: 8px; }
		.detail-file-name { font-size: 0.75rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
		.detail-file-size { font-size: 0.65rem; color: var(--text-2); }
		
		/* Forms */
		.form-group { margin-bottom: 14px; }
		.form-label { display: block; font-size: 0.7rem; color: var(--text-2); margin-bottom: 6px; text-transform: uppercase; }
		.form-input, .form-textarea, .form-select {
			width: 100%; padding: 10px 12px;
			background: var(--bg-3); border: 1px solid var(--border);
			color: var(--text-0); font-size: 0.85rem; font-family: inherit;
			outline: none; transition: border-color 0.1s;
		}
		.form-input:focus, .form-textarea:focus, .form-select:focus { border-color: var(--accent); }
		.form-textarea { min-height: 80px; resize: vertical; }
		.form-hint { font-size: 0.7rem; color: var(--text-3); margin-top: 4px; }
		.form-row { display: flex; gap: 8px; }
		.form-row > * { flex: 1; }
		
		/* Buttons */
		.btn {
			padding: 8px 14px; font-size: 0.8rem; font-weight: 500;
			border: 1px solid var(--border); background: var(--bg-3);
			color: var(--text-0); cursor: pointer; transition: all 0.1s; font-family: inherit;
		}
		.btn:hover { background: var(--bg-4); border-color: var(--text-3); }
		.btn-primary { background: var(--accent); border-color: var(--accent); }
		.btn-primary:hover { background: var(--accent-dim); }
		.btn-sm { padding: 5px 10px; font-size: 0.7rem; }
		.btn-icon { padding: 6px 8px; }
		.btn-danger { border-color: var(--error); color: var(--error); }
		.btn-danger:hover { background: var(--error); color: white; }
		.btn-block { width: 100%; }
		
		/* Modal */
		.modal-overlay {
			position: fixed; inset: 0;
			background: rgba(0,0,0,0.85);
			display: flex; align-items: center; justify-content: center;
			z-index: 1000;
		}
		.modal { background: var(--bg-2); border: 1px solid var(--border); width: 100%; max-width: 500px; max-height: 90vh; overflow: auto; }
		.modal-lg { max-width: 700px; }
		.modal-header {
			padding: 16px 20px; border-bottom: 1px solid var(--border);
			display: flex; justify-content: space-between; align-items: center;
		}
		.modal-title { font-size: 1rem; font-weight: 600; }
		.modal-close { background: none; border: none; color: var(--text-2); font-size: 1.5rem; cursor: pointer; line-height: 1; }
		.modal-body { padding: 20px; }
		.modal-footer { padding: 16px 20px; border-top: 1px solid var(--border); display: flex; justify-content: flex-end; gap: 8px; }
		
		/* Tabs */
		.tabs { display: flex; border-bottom: 1px solid var(--border); }
		.tab {
			padding: 12px 16px; font-size: 0.8rem; color: var(--text-2);
			cursor: pointer; border-bottom: 2px solid transparent;
			transition: all 0.1s;
		}
		.tab:hover { color: var(--text-1); }
		.tab.active { color: var(--accent); border-bottom-color: var(--accent); }
		.tab-content { display: none; }
		.tab-content.active { display: block; }
		
		/* Upload */
		.upload-zone {
			border: 2px dashed var(--border); padding: 30px;
			text-align: center; cursor: pointer; transition: border-color 0.1s;
		}
		.upload-zone:hover, .upload-zone.dragover { border-color: var(--accent); }
		.upload-zone-text { color: var(--text-2); font-size: 0.85rem; }
		.upload-zone-icon { font-size: 2rem; margin-bottom: 8px; }
		
		/* Lightbox */
		.lightbox {
			position: fixed; inset: 0; background: rgba(0,0,0,0.95);
			display: flex; align-items: center; justify-content: center; z-index: 2000;
		}
		.lightbox img, .lightbox video { max-width: 95vw; max-height: 95vh; object-fit: contain; }
		.lightbox-close { position: absolute; top: 16px; right: 16px; background: none; border: none; color: white; font-size: 2rem; cursor: pointer; }
		
		/* Toast */
		.toast {
			position: fixed; bottom: 20px; right: 20px;
			padding: 12px 20px; background: var(--bg-2);
			border: 1px solid var(--border); font-size: 0.85rem; z-index: 3000;
			animation: slideIn 0.2s ease;
		}
		.toast.success { border-left: 3px solid var(--success); }
		.toast.error { border-left: 3px solid var(--error); }
		@keyframes slideIn { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
		
		/* Welcome Screen */
		.welcome {
			display: flex; flex-direction: column;
			align-items: center; justify-content: center;
			height: 100%; padding: 40px; text-align: center;
		}
		.welcome h2 { font-size: 1.5rem; margin-bottom: 8px; }
		.welcome p { color: var(--text-2); margin-bottom: 24px; }
		.welcome-actions { display: flex; gap: 12px; }
		
		/* Empty State */
		.empty-state { padding: 60px 20px; text-align: center; color: var(--text-2); }
		.empty-state h3 { color: var(--text-1); margin-bottom: 8px; }
		
		.hidden { display: none !important; }
		
		/* Scrollbar */
		::-webkit-scrollbar { width: 8px; height: 8px; }
		::-webkit-scrollbar-track { background: var(--bg-1); }
		::-webkit-scrollbar-thumb { background: var(--border); }
		::-webkit-scrollbar-thumb:hover { background: var(--text-3); }
		
		/* Extractor Config */
		.extractor-config { background: var(--bg-3); padding: 12px; margin-top: 12px; }
		.extractor-config-title { font-size: 0.7rem; color: var(--text-2); margin-bottom: 8px; text-transform: uppercase; }
	</style>
</head>
<body>
	<div class="app">
		<header class="header">
			<div class="header-left">
				<div class="logo">DLFI</div>
				<div class="archive-info" id="archiveInfo">
					<span class="text-muted">No archive open</span>
				</div>
			</div>
			<div class="header-actions">
				<button class="btn btn-sm" onclick="showExtractorModal()">Extract</button>
				<button class="btn btn-sm" onclick="showCreateModal()">Create</button>
				<button class="btn btn-sm" onclick="showSettingsModal()">Settings</button>
			</div>
		</header>
		
		<div class="main">
			<!-- Sidebar -->
			<aside class="sidebar" id="sidebar">
				<div class="sidebar-section">
					<div class="sidebar-header">
						<span>Browser</span>
						<button class="btn btn-sm btn-icon" onclick="refreshTree()">↻</button>
					</div>
					<div class="tree-container" id="treeContainer"></div>
				</div>
			</aside>
			
			<!-- Main Content -->
			<main class="content" id="contentArea">
				<div class="welcome" id="welcomeScreen">
					<h2>Welcome to DLFI</h2>
					<p>Open an existing archive or create a new one to get started.</p>
					<div class="welcome-actions">
						<button class="btn" onclick="showOpenArchiveModal()">Open Archive</button>
						<button class="btn btn-primary" onclick="showCreateArchiveModal()">Create Archive</button>
					</div>
				</div>
				
				<div class="hidden" id="mainUI">
					<!-- Search -->
					<div class="search-container">
						<div class="search-bar">
							<div class="search-input-wrap">
								<input type="text" class="search-input" id="searchInput" placeholder="Search paths, names, metadata, tags..." autocomplete="off">
							</div>
							<button class="btn btn-primary" onclick="executeSearch()">Search</button>
						</div>
						<div class="search-filters" id="searchFilters">
							<button class="add-filter-btn" onclick="showFilterModal()">+ Add Filter</button>
						</div>
					</div>
					
					<!-- Results -->
					<div class="results-container">
						<div class="results-header">
							<span id="resultsCount">0 results</span>
							<div>
								<select class="form-select" style="width: auto; padding: 4px 8px; font-size: 0.75rem;" onchange="setTypeFilter(this.value)">
									<option value="">All Types</option>
									<option value="VAULT">Vaults</option>
									<option value="RECORD">Records</option>
								</select>
							</div>
						</div>
						<div class="results-grid" id="resultsGrid">
							<div class="empty-state">
								<h3>Start searching</h3>
								<p>Enter a search term or add filters to find items.</p>
							</div>
						</div>
					</div>
				</div>
			</main>
			
			<!-- Detail Panel -->
			<aside class="detail-panel hidden" id="detailPanel">
				<div class="detail-header">
					<div>
						<div class="detail-title" id="detailTitle"></div>
						<div style="font-size: 0.75rem; color: var(--text-2); margin-top: 4px;" id="detailPath"></div>
					</div>
					<button class="detail-close" onclick="closeDetailPanel()">&times;</button>
				</div>
				<div class="detail-body" id="detailBody"></div>
			</aside>
		</div>
	</div>
	
	<!-- Lightbox -->
	<div class="lightbox hidden" id="lightbox" onclick="closeLightbox(event)">
		<button class="lightbox-close">&times;</button>
		<div id="lightboxContent"></div>
	</div>
	
	<script>
	// State
	let archiveOpen = false;
	let selectedPath = null;
	let searchFilters = [];
	let expandedNodes = new Set();
	let typeFilter = '';
	
	// API
	async function api(method, path, body = null) {
		const opts = { method, headers: {} };
		if (body && !(body instanceof FormData)) {
			opts.headers['Content-Type'] = 'application/json';
			opts.body = JSON.stringify(body);
		} else if (body) {
			opts.body = body;
		}
		const res = await fetch(path, opts);
		const data = await res.json();
		if (!res.ok) throw new Error(data.error || 'Request failed');
		return data;
	}
	
	function toast(msg, type = 'info') {
		const el = document.createElement('div');
		el.className = `toast ${type}`;
		el.textContent = msg;
		document.body.appendChild(el);
		setTimeout(() => el.remove(), 3000);
	}
	
	function formatSize(bytes) {
		if (!bytes) return '0 B';
		const k = 1024, sizes = ['B', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
	}
	
	function closeModal(el) {
		el.closest('.modal-overlay')?.remove();
	}
	
	// Archive Management
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
			info.innerHTML = `
				<span class="path" title="${status.archive_path}">${name}</span>
				${status.encrypted ? '<span class="archive-badge encrypted">Encrypted</span>' : ''}
				<span style="color: var(--text-2)">
					${status.stats.nodes} nodes · ${formatSize(status.stats.total_size)}
				</span>
			`;
			welcome.classList.add('hidden');
			mainUI.classList.remove('hidden');
			sidebar.style.display = 'flex';
			refreshTree();
		} else {
			info.innerHTML = '<span class="text-muted">No archive open</span>';
			welcome.classList.remove('hidden');
			mainUI.classList.add('hidden');
			sidebar.style.display = 'none';
			closeDetailPanel();
		}
	}
	
	function showOpenArchiveModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header">
					<span class="modal-title">Open Archive</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">Archive Path</label>
						<input type="text" class="form-input" id="openPath" placeholder="/path/to/archive">
						<div class="form-hint">Enter the full path to the archive directory</div>
					</div>
					<div class="form-group">
						<label class="form-label">Password (if encrypted)</label>
						<input type="password" class="form-input" id="openPassword" placeholder="Optional">
					</div>
				</div>
				<div class="modal-footer">
					<button class="btn" onclick="closeModal(this)">Cancel</button>
					<button class="btn btn-primary" onclick="openArchive()">Open</button>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
		modal.querySelector('#openPath').focus();
	}
	
	async function openArchive() {
		const path = document.getElementById('openPath').value.trim();
		const password = document.getElementById('openPassword').value || null;
		
		if (!path) { toast('Path is required', 'error'); return; }
		
		try {
			await api('POST', '/api/archive/open', { path, password });
			document.querySelector('.modal-overlay')?.remove();
			await checkStatus();
			toast('Archive opened', 'success');
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	function showCreateArchiveModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header">
					<span class="modal-title">Create Archive</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">Archive Path</label>
						<input type="text" class="form-input" id="createArchivePath" placeholder="/path/to/new/archive">
					</div>
					<div class="form-group">
						<label class="form-label">Password (for encryption)</label>
						<input type="password" class="form-input" id="createArchivePassword" placeholder="Leave empty for no encryption">
					</div>
					<div class="form-group">
						<label class="form-label">Partition Size</label>
						<select class="form-select" id="createArchivePartition">
							<option value="0">No partitioning</option>
							<option value="26214400">25 MB</option>
							<option value="52428800" selected>50 MB</option>
							<option value="104857600">100 MB</option>
						</select>
						<div class="form-hint">Split large files for GitHub compatibility</div>
					</div>
				</div>
				<div class="modal-footer">
					<button class="btn" onclick="closeModal(this)">Cancel</button>
					<button class="btn btn-primary" onclick="createArchive()">Create</button>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
	}
	
	async function createArchive() {
		const path = document.getElementById('createArchivePath').value.trim();
		const password = document.getElementById('createArchivePassword').value || null;
		const partition_size = parseInt(document.getElementById('createArchivePartition').value);
		
		if (!path) { toast('Path is required', 'error'); return; }
		
		try {
			await api('POST', '/api/archive/create', { path, password, partition_size });
			document.querySelector('.modal-overlay')?.remove();
			await checkStatus();
			toast('Archive created', 'success');
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	// Tree
	async function loadChildren(parentUuid, container, depth = 0) {
		try {
			const data = await api('GET', `/api/children/${parentUuid || ''}`);
			
			for (const child of data.children) {
				const node = document.createElement('div');
				node.className = 'tree-node';
				node.dataset.uuid = child.uuid;
				node.dataset.path = child.path;
				
				const row = document.createElement('div');
				row.className = 'tree-item';
				row.style.paddingLeft = `${12 + depth * 14}px`;
				
				const toggle = document.createElement('span');
				toggle.className = `tree-toggle ${child.hasChildren ? '' : 'hidden'} ${expandedNodes.has(child.uuid) ? 'expanded' : ''}`;
				toggle.textContent = '▶';
				
				const icon = document.createElement('span');
				icon.className = 'tree-icon';
				icon.textContent = child.type === 'VAULT' ? '📁' : '📄';
				
				const name = document.createElement('span');
				name.className = 'tree-name';
				name.textContent = child.name;
				
				row.append(toggle, icon, name);
				row.onclick = (e) => {
					e.stopPropagation();
					if (e.target === toggle && child.hasChildren) {
						toggleTreeNode(child.uuid, node, depth);
					} else {
						selectNode(child.path);
					}
				};
				
				const childContainer = document.createElement('div');
				childContainer.className = `tree-children ${expandedNodes.has(child.uuid) ? 'expanded' : ''}`;
				
				node.append(row, childContainer);
				container.appendChild(node);
				
				if (expandedNodes.has(child.uuid) && child.hasChildren) {
					await loadChildren(child.uuid, childContainer, depth + 1);
				}
			}
		} catch (e) {
			console.error('Tree load error:', e);
		}
	}
	
	async function toggleTreeNode(uuid, node, depth) {
		const toggle = node.querySelector('.tree-toggle');
		const childContainer = node.querySelector('.tree-children');
		
		if (expandedNodes.has(uuid)) {
			expandedNodes.delete(uuid);
			toggle.classList.remove('expanded');
			childContainer.classList.remove('expanded');
			childContainer.innerHTML = '';
		} else {
			expandedNodes.add(uuid);
			toggle.classList.add('expanded');
			childContainer.classList.add('expanded');
			await loadChildren(uuid, childContainer, depth + 1);
		}
	}
	
	async function refreshTree() {
		document.getElementById('treeContainer').innerHTML = '';
		await loadChildren(null, document.getElementById('treeContainer'), 0);
	}
	
	// Search & Query
	async function executeSearch() {
		if (!archiveOpen) return;
		
		const q = document.getElementById('searchInput').value.trim();
		
		try {
			let results;
			
			if (searchFilters.length > 0) {
				// Use structured query
				const query = {};
				for (const f of searchFilters) {
					if (f.type === 'has_tag') {
						if (!query.has_tag) query.has_tag = [];
						query.has_tag.push(f.value);
					} else if (f.type === 'meta_eq') {
						if (!query.meta_eq) query.meta_eq = {};
						const [key, val] = f.value.split('=');
						query.meta_eq[key.trim()] = val?.trim() || '';
					} else if (f.type === 'meta_contains') {
						if (!query.meta_contains) query.meta_contains = {};
						const [key, val] = f.value.split(':');
						query.meta_contains[key.trim()] = val?.trim() || '';
					} else if (f.type === 'meta_exists') {
						if (!query.meta_exists) query.meta_exists = [];
						query.meta_exists.push(f.value);
					} else if (f.type === 'related_to') {
						const parts = f.value.split(':');
						query.related_to = { target: parts[0], relation: parts[1] || null };
					} else if (f.type === 'inside') {
						query.inside = f.value;
					}
				}
				if (typeFilter) query.type = typeFilter;
				if (q) {
					// Combine text search with filters using inside for prefix
					query.inside = query.inside || q;
				}
				const data = await api('POST', '/api/query', query);
				results = data.results;
			} else if (q) {
				// Simple text search
				const data = await api('POST', '/api/search', { q, type: typeFilter || undefined });
				results = data.results;
			} else {
				// Show all
				const data = await api('POST', '/api/query', { type: typeFilter || undefined, limit: 200 });
				results = data.results;
			}
			
			renderResults(results);
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	function renderResults(results) {
		const grid = document.getElementById('resultsGrid');
		document.getElementById('resultsCount').textContent = `${results.length} result${results.length !== 1 ? 's' : ''}`;
		
		if (results.length === 0) {
			grid.innerHTML = '<div class="empty-state"><h3>No results</h3><p>Try adjusting your search or filters.</p></div>';
			return;
		}
		
		grid.innerHTML = results.map(r => {
			const meta = r.metadata || {};
			const metaKeys = Object.keys(meta).slice(0, 3);
			
			return `
				<div class="result-card" onclick="selectNode('${r.path}')">
					<div class="result-header">
						<span class="result-icon">${r.type === 'VAULT' ? '📁' : '📄'}</span>
						<div class="result-info">
							<div class="result-name">${r.path.split('/').pop()}</div>
							<div class="result-path">${r.path}</div>
						</div>
						<span class="result-type">${r.type}</span>
					</div>
					${metaKeys.length > 0 ? `
						<div class="result-meta">
							${metaKeys.map(k => `<span class="result-tag">${k}: ${String(meta[k]).substring(0, 30)}</span>`).join('')}
						</div>
					` : ''}
				</div>
			`;
		}).join('');
	}
	
	function setTypeFilter(type) {
		typeFilter = type;
		executeSearch();
	}
	
	function showFilterModal() {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header">
					<span class="modal-title">Add Filter</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">Filter Type</label>
						<select class="form-select" id="filterType">
							<option value="inside">Inside Path (prefix)</option>
							<option value="has_tag">Has Tag</option>
							<option value="meta_eq">Metadata Equals (key=value)</option>
							<option value="meta_contains">Metadata Contains (key:value)</option>
							<option value="meta_exists">Metadata Key Exists</option>
							<option value="related_to">Related To (path:RELATION)</option>
						</select>
					</div>
					<div class="form-group">
						<label class="form-label">Value</label>
						<input type="text" class="form-input" id="filterValue" placeholder="Enter value">
					</div>
				</div>
				<div class="modal-footer">
					<button class="btn" onclick="closeModal(this)">Cancel</button>
					<button class="btn btn-primary" onclick="addFilter()">Add Filter</button>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
	}
	
	function addFilter() {
		const type = document.getElementById('filterType').value;
		const value = document.getElementById('filterValue').value.trim();
		if (!value) { toast('Value required', 'error'); return; }
		
		searchFilters.push({ type, value });
		renderFilters();
		document.querySelector('.modal-overlay')?.remove();
		executeSearch();
	}
	
	function removeFilter(index) {
		searchFilters.splice(index, 1);
		renderFilters();
		executeSearch();
	}
	
	function renderFilters() {
		const container = document.getElementById('searchFilters');
		container.innerHTML = searchFilters.map((f, i) => `
			<span class="filter-chip">
				<strong>${f.type}:</strong> ${f.value}
				<span class="remove" onclick="removeFilter(${i})">×</span>
			</span>
		`).join('') + '<button class="add-filter-btn" onclick="showFilterModal()">+ Add Filter</button>';
	}
	
	// Detail Panel
	async function selectNode(path) {
		selectedPath = path;
		
		// Update tree selection
		document.querySelectorAll('.tree-item').forEach(el => el.classList.remove('selected'));
		const treeNode = document.querySelector(`[data-path="${path}"] > .tree-item`);
		if (treeNode) treeNode.classList.add('selected');
		
		try {
			const node = await api('GET', `/api/node/${encodeURIComponent(path)}`);
			renderDetailPanel(node);
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	function renderDetailPanel(node) {
		const panel = document.getElementById('detailPanel');
		panel.classList.remove('hidden');
		
		document.getElementById('detailTitle').textContent = node.name;
		document.getElementById('detailPath').textContent = node.path;
		
		let html = '';
		
		// Metadata
		const meta = node.metadata || {};
		if (Object.keys(meta).length > 0) {
			html += `
				<div class="detail-section">
					<div class="detail-section-title">Metadata</div>
					<div class="detail-meta-grid">
						${Object.entries(meta).map(([k, v]) => `
							<div class="detail-meta-item ${String(v).length > 50 ? 'full' : ''}">
								<div class="detail-meta-label">${k}</div>
								<div class="detail-meta-value">${typeof v === 'object' ? JSON.stringify(v, null, 2) : v}</div>
							</div>
						`).join('')}
					</div>
				</div>
			`;
		}
		
		// Tags
		html += `
			<div class="detail-section">
				<div class="detail-section-title">Tags</div>
				<div class="detail-tags">
					${node.tags.map(t => `
						<span class="detail-tag">${t}<span class="remove" onclick="removeTag('${node.path}', '${t}')">×</span></span>
					`).join('')}
					<button class="btn btn-sm" onclick="showAddTagModal('${node.path}')">+ Add</button>
				</div>
			</div>
		`;
		
		// Relationships
		if (node.relationships.length > 0 || node.incoming_relationships.length > 0) {
			html += `<div class="detail-section"><div class="detail-section-title">Relationships</div>`;
			
			for (const rel of node.relationships) {
				html += `
					<div class="detail-rel-item" onclick="selectNode('${rel.target_path}')">
						<span class="detail-rel-type">${rel.relation}</span>
						<span class="detail-rel-path">${rel.target_path}</span>
						<span class="detail-rel-dir">→</span>
					</div>
				`;
			}
			
			for (const rel of node.incoming_relationships) {
				html += `
					<div class="detail-rel-item" onclick="selectNode('${rel.source_path}')">
						<span class="detail-rel-type">${rel.relation}</span>
						<span class="detail-rel-path">${rel.source_path}</span>
						<span class="detail-rel-dir">←</span>
					</div>
				`;
			}
			
			html += `<button class="btn btn-sm" style="margin-top: 8px" onclick="showAddRelModal('${node.path}')">+ Add Relationship</button></div>`;
		}
		
		// Files
		if (node.files.length > 0) {
			html += `
				<div class="detail-section">
					<div class="detail-section-title">Files (${node.files.length})</div>
					<div class="detail-files-grid">
						${node.files.map(f => {
							const isImg = ['jpg','jpeg','png','gif','webp','bmp'].includes(f.ext);
							const isVid = ['mp4','webm','mov'].includes(f.ext);
							return `
								<div class="detail-file" onclick="openFile('${f.hash}', '${f.ext}', '${f.name}')">
									<div class="detail-file-preview">
										${isImg ? `<img src="/api/blob/${f.hash}" loading="lazy">` :
										isVid ? `<video src="/api/blob/${f.hash}" muted></video>` :
										`<span class="detail-file-icon">📎</span>`}
									</div>
									<div class="detail-file-info">
										<div class="detail-file-name" title="${f.name}">${f.name}</div>
										<div class="detail-file-size">${formatSize(f.size)}</div>
									</div>
								</div>
							`;
						}).join('')}
					</div>
					<button class="btn btn-sm btn-block" style="margin-top: 12px" onclick="showUploadModal('${node.path}')">+ Upload Files</button>
				</div>
			`;
		} else if (node.type === 'RECORD') {
			html += `
				<div class="detail-section">
					<div class="detail-section-title">Files</div>
					<button class="btn btn-sm btn-block" onclick="showUploadModal('${node.path}')">+ Upload Files</button>
				</div>
			`;
		}
		
		// Actions
		html += `
			<div class="detail-section">
				<div class="detail-section-title">Actions</div>
				<button class="btn btn-sm" onclick="showEditMetaModal('${node.path}', ${JSON.stringify(JSON.stringify(meta))})">Edit Metadata</button>
				<button class="btn btn-sm btn-danger" style="margin-left: 8px" onclick="deleteNode('${node.path}')">Delete</button>
			</div>
		`;
		
		document.getElementById('detailBody').innerHTML = html;
	}
	
	function closeDetailPanel() {
		document.getElementById('detailPanel').classList.add('hidden');
		selectedPath = null;
	}
	
	// Modals & Actions
	function showCreateModal() {
		if (!archiveOpen) { toast('Open an archive first', 'error'); return; }
		
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header">
					<span class="modal-title">Create Node</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">Type</label>
						<select class="form-select" id="createNodeType">
							<option value="VAULT">Vault (Folder)</option>
							<option value="RECORD">Record (Item)</option>
						</select>
					</div>
					<div class="form-group">
						<label class="form-label">Path</label>
						<input type="text" class="form-input" id="createNodePath" placeholder="parent/child/name">
						<div class="form-hint">Parents created automatically</div>
					</div>
					<div class="form-group">
						<label class="form-label">Metadata (JSON)</label>
						<textarea class="form-textarea" id="createNodeMeta" placeholder='{"key": "value"}'></textarea>
					</div>
				</div>
				<div class="modal-footer">
					<button class="btn" onclick="closeModal(this)">Cancel</button>
					<button class="btn btn-primary" onclick="createNode()">Create</button>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
	}
	
	async function createNode() {
		const type = document.getElementById('createNodeType').value;
		const path = document.getElementById('createNodePath').value.trim();
		let metadata = {};
		
		try {
			const metaStr = document.getElementById('createNodeMeta').value.trim();
			if (metaStr) metadata = JSON.parse(metaStr);
		} catch (e) {
			toast('Invalid JSON', 'error');
			return;
		}
		
		if (!path) { toast('Path required', 'error'); return; }
		
		try {
			const endpoint = type === 'VAULT' ? '/api/vault' : '/api/record';
			await api('POST', endpoint, { path, metadata });
			toast('Created', 'success');
			document.querySelector('.modal-overlay')?.remove();
			refreshTree();
			executeSearch();
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	function showUploadModal(recordPath) {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header">
					<span class="modal-title">Upload Files</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">Target Record</label>
						<input type="text" class="form-input" id="uploadTargetPath" value="${recordPath}" readonly>
					</div>
					<div class="upload-zone" id="uploadZone" onclick="document.getElementById('uploadInput').click()">
						<div class="upload-zone-icon">📁</div>
						<div class="upload-zone-text">Drop files here or click to browse</div>
						<input type="file" id="uploadInput" multiple style="display:none" onchange="handleUpload()">
					</div>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
		
		const zone = modal.querySelector('#uploadZone');
		zone.ondragover = (e) => { e.preventDefault(); zone.classList.add('dragover'); };
		zone.ondragleave = () => zone.classList.remove('dragover');
		zone.ondrop = async (e) => { e.preventDefault(); zone.classList.remove('dragover'); await uploadFiles(e.dataTransfer.files); };
	}
	
	async function handleUpload() {
		await uploadFiles(document.getElementById('uploadInput').files);
	}
	
	async function uploadFiles(files) {
		const path = document.getElementById('uploadTargetPath').value;
		const formData = new FormData();
		formData.append('path', path);
		for (const f of files) formData.append('file', f, f.name);
		
		try {
			await api('POST', '/api/upload', formData);
			toast(`Uploaded ${files.length} file(s)`, 'success');
			document.querySelector('.modal-overlay')?.remove();
			if (selectedPath === path) selectNode(path);
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	function showAddTagModal(path) {
		const tag = prompt('Enter tag:');
		if (tag) addTag(path, tag);
	}
	
	async function addTag(path, tag) {
		try {
			await api('POST', '/api/tag', { path, tag });
			toast('Tag added', 'success');
			selectNode(path);
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	async function removeTag(path, tag) {
		try {
			await api('DELETE', '/api/tag/', { path, tag });
			toast('Tag removed', 'success');
			selectNode(path);
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	function showAddRelModal(sourcePath) {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header">
					<span class="modal-title">Add Relationship</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">Source</label>
						<input type="text" class="form-input" value="${sourcePath}" readonly>
					</div>
					<div class="form-group">
						<label class="form-label">Relation</label>
						<input type="text" class="form-input" id="relName" placeholder="RELATED_TO">
					</div>
					<div class="form-group">
						<label class="form-label">Target Path</label>
						<input type="text" class="form-input" id="relTarget" placeholder="path/to/target">
					</div>
				</div>
				<div class="modal-footer">
					<button class="btn" onclick="closeModal(this)">Cancel</button>
					<button class="btn btn-primary" onclick="addRelationship('${sourcePath}')">Add</button>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
	}
	
	async function addRelationship(source) {
		const relation = document.getElementById('relName').value.trim();
		const target = document.getElementById('relTarget').value.trim();
		
		if (!relation || !target) { toast('All fields required', 'error'); return; }
		
		try {
			await api('POST', '/api/link', { source, target, relation });
			toast('Relationship created', 'success');
			document.querySelector('.modal-overlay')?.remove();
			selectNode(source);
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	function showEditMetaModal(path, currentMeta) {
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header">
					<span class="modal-title">Edit Metadata</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">Metadata (JSON)</label>
						<textarea class="form-textarea" id="editMetaValue" style="min-height: 200px; font-family: monospace;">${JSON.stringify(JSON.parse(currentMeta), null, 2)}</textarea>
					</div>
				</div>
				<div class="modal-footer">
					<button class="btn" onclick="closeModal(this)">Cancel</button>
					<button class="btn btn-primary" onclick="updateMeta('${path}')">Save</button>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
	}
	
	async function updateMeta(path) {
		try {
			const metadata = JSON.parse(document.getElementById('editMetaValue').value);
			await api('POST', '/api/node/update', { path, metadata });
			toast('Metadata updated', 'success');
			document.querySelector('.modal-overlay')?.remove();
			selectNode(path);
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	async function deleteNode(path) {
		if (!confirm(`Delete "${path}" and all its children?`)) return;
		
		try {
			await api('DELETE', `/api/node/${encodeURIComponent(path)}`);
			toast('Deleted', 'success');
			closeDetailPanel();
			refreshTree();
			executeSearch();
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	// Extractor Modal
	async function showExtractorModal() {
		if (!archiveOpen) { toast('Open an archive first', 'error'); return; }
		
		let extractors = [];
		try {
			const data = await api('GET', '/api/extractors');
			extractors = data.extractors;
		} catch (e) {
			toast('Failed to load extractors', 'error');
			return;
		}
		
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal modal-lg">
				<div class="modal-header">
					<span class="modal-title">Extract Content</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="form-group">
						<label class="form-label">URL</label>
						<input type="text" class="form-input" id="extractUrl" placeholder="https://...">
						<div class="form-hint">Available extractors: ${extractors.map(e => e.name).join(', ')}</div>
					</div>
					<div class="form-group">
						<label class="form-label">Cookies (Netscape format, optional)</label>
						<textarea class="form-textarea" id="extractCookies" placeholder="Paste cookie file content here..."></textarea>
					</div>
					<div class="form-group">
						<label class="form-label">Extractor Config (JSON, optional)</label>
						<textarea class="form-textarea" id="extractConfig" placeholder='{"password": "secret"}'></textarea>
						<div class="form-hint">Poipiku: password, password_list</div>
					</div>
				</div>
				<div class="modal-footer">
					<button class="btn" onclick="closeModal(this)">Cancel</button>
					<button class="btn btn-primary" onclick="runExtractor()">Extract</button>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
	}
	
	async function runExtractor() {
		const url = document.getElementById('extractUrl').value.trim();
		const cookies = document.getElementById('extractCookies').value;
		let config = {};
		
		try {
			const configStr = document.getElementById('extractConfig').value.trim();
			if (configStr) config = JSON.parse(configStr);
		} catch (e) {
			toast('Invalid JSON in config', 'error');
			return;
		}
		
		if (!url) { toast('URL required', 'error'); return; }
		
		try {
			toast('Extraction started...', 'info');
			document.querySelector('.modal-overlay')?.remove();
			
			await api('POST', '/api/extract', { url, cookies, config });
			
			toast('Extraction complete', 'success');
			refreshTree();
			executeSearch();
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	// Settings Modal
	async function showSettingsModal() {
		if (!archiveOpen) { showOpenArchiveModal(); return; }
		
		let config;
		try {
			config = await api('GET', '/api/config');
		} catch (e) {
			toast(e.message, 'error');
			return;
		}
		
		const modal = document.createElement('div');
		modal.className = 'modal-overlay';
		modal.innerHTML = `
			<div class="modal">
				<div class="modal-header">
					<span class="modal-title">Archive Settings</span>
					<button class="modal-close" onclick="closeModal(this)">&times;</button>
				</div>
				<div class="modal-body">
					<div class="tabs">
						<div class="tab active" data-tab="general" onclick="switchSettingsTab('general')">General</div>
						<div class="tab" data-tab="encryption" onclick="switchSettingsTab('encryption')">Encryption</div>
						<div class="tab" data-tab="actions" onclick="switchSettingsTab('actions')">Actions</div>
					</div>
					
					<div class="tab-content active" id="tabGeneral" style="padding-top: 16px;">
						<div class="form-group">
							<label class="form-label">Archive Path</label>
							<input type="text" class="form-input" value="${config.path}" readonly>
						</div>
						<div class="form-group">
							<label class="form-label">Partition Size</label>
							<select class="form-select" id="settingsPartition">
								<option value="0" ${config.partition_size === 0 ? 'selected' : ''}>Disabled</option>
								<option value="10485760" ${config.partition_size === 10485760 ? 'selected' : ''}>10 MB</option>
								<option value="26214400" ${config.partition_size === 26214400 ? 'selected' : ''}>25 MB</option>
								<option value="52428800" ${config.partition_size === 52428800 ? 'selected' : ''}>50 MB</option>
								<option value="104857600" ${config.partition_size === 104857600 ? 'selected' : ''}>100 MB</option>
							</select>
							<button class="btn btn-sm" style="margin-top: 8px" onclick="updatePartitionSize()">Update</button>
						</div>
					</div>
					
					<div class="tab-content" id="tabEncryption" style="padding-top: 16px;">
						<div style="margin-bottom: 16px; color: var(--text-2);">
							Status: ${config.encrypted ? '<span style="color: var(--success)">Enabled</span>' : 'Disabled'}
						</div>
						${config.encrypted ? `
							<div class="form-group">
								<label class="form-label">Current Password</label>
								<input type="password" class="form-input" id="encOldPass">
							</div>
							<div class="form-group">
								<label class="form-label">New Password (leave empty to disable)</label>
								<input type="password" class="form-input" id="encNewPass">
							</div>
							<button class="btn" onclick="updateEncryption()">Update Encryption</button>
						` : `
							<div class="form-group">
								<label class="form-label">Password</label>
								<input type="password" class="form-input" id="encNewPass">
							</div>
							<button class="btn" onclick="enableEncryption()">Enable Encryption</button>
						`}
					</div>
					
					<div class="tab-content" id="tabActions" style="padding-top: 16px;">
						<div class="form-group">
							<button class="btn btn-block" onclick="generateStatic()">Generate Static Site</button>
							<div class="form-hint">Creates index.html for static viewing</div>
						</div>
						<div class="form-group">
							<button class="btn btn-block" onclick="closeArchiveAction()">Close Archive</button>
						</div>
					</div>
				</div>
			</div>
		`;
		document.body.appendChild(modal);
	}
	
	function switchSettingsTab(tab) {
		document.querySelectorAll('.modal .tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
		document.querySelectorAll('.modal .tab-content').forEach(c => c.classList.remove('active'));
		document.getElementById('tab' + tab.charAt(0).toUpperCase() + tab.slice(1)).classList.add('active');
	}
	
	async function updatePartitionSize() {
		const size = parseInt(document.getElementById('settingsPartition').value);
		try {
			await api('POST', '/api/config/partition', { size });
			toast('Partition size updated', 'success');
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	async function enableEncryption() {
		const password = document.getElementById('encNewPass').value;
		if (!password) { toast('Password required', 'error'); return; }
		
		try {
			await api('POST', '/api/config/encryption', { action: 'enable', password });
			toast('Encryption enabled', 'success');
			document.querySelector('.modal-overlay')?.remove();
			checkStatus();
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	async function updateEncryption() {
		const oldPass = document.getElementById('encOldPass').value;
		const newPass = document.getElementById('encNewPass').value;
		
		if (!oldPass) { toast('Current password required', 'error'); return; }
		
		try {
			if (newPass) {
				await api('POST', '/api/config/encryption', { action: 'change_password', old_password: oldPass, new_password: newPass });
				toast('Password changed', 'success');
			} else {
				await api('POST', '/api/config/encryption', { action: 'disable', password: oldPass });
				toast('Encryption disabled', 'success');
			}
			document.querySelector('.modal-overlay')?.remove();
			checkStatus();
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	async function generateStatic() {
		try {
			await api('POST', '/api/generate-static');
			toast('Static site generated', 'success');
		} catch (e) {
			toast(e.message, 'error');
		}
	}
	
	async function closeArchiveAction() {
		await api('POST', '/api/archive/close');
		document.querySelector('.modal-overlay')?.remove();
		checkStatus();
	}
	
	// Lightbox
	function openFile(hash, ext, name) {
		const isImg = ['jpg','jpeg','png','gif','webp','bmp'].includes(ext);
		const isVid = ['mp4','webm','mov'].includes(ext);
		
		if (isImg) {
			document.getElementById('lightboxContent').innerHTML = `<img src="/api/blob/${hash}">`;
			document.getElementById('lightbox').classList.remove('hidden');
		} else if (isVid) {
			document.getElementById('lightboxContent').innerHTML = `<video src="/api/blob/${hash}" controls autoplay></video>`;
			document.getElementById('lightbox').classList.remove('hidden');
		} else {
			const a = document.createElement('a');
			a.href = `/api/blob/${hash}`;
			a.download = name;
			a.click();
		}
	}
	
	function closeLightbox(e) {
		if (!e || e.target.id === 'lightbox' || e.target.classList.contains('lightbox-close')) {
			document.getElementById('lightbox').classList.add('hidden');
			document.getElementById('lightboxContent').innerHTML = '';
		}
	}
	
	// Keyboard
	document.addEventListener('keydown', (e) => {
		if (e.key === 'Escape') {
			closeLightbox();
			document.querySelector('.modal-overlay')?.remove();
		}
		if (e.key === 'Enter' && e.target.id === 'searchInput') {
			executeSearch();
		}
	});
	
	// Init
	checkStatus();
	</script>
</body>
</html>'''