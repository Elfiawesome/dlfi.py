import os
import json
import logging
import mimetypes
from pathlib import Path
from typing import Optional
from functools import wraps
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse, unquote
import threading
import io

logger = logging.getLogger(__name__)


class DLFIServer:
	"""Web server for DLFI archive management."""
	
	def __init__(self, dlfi_instance, host: str = "127.0.0.1", port: int = 8080):
		self.dlfi = dlfi_instance
		self.host = host
		self.port = port
		self._server = None
		self._thread = None
	
	def start(self, blocking: bool = True):
		"""Start the web server."""
		handler = self._create_handler()
		self._server = HTTPServer((self.host, self.port), handler)
		
		logger.info(f"Starting DLFI server at http://{self.host}:{self.port}")
		
		if blocking:
			try:
				self._server.serve_forever()
			except KeyboardInterrupt:
				logger.info("Server stopped by user")
		else:
			self._thread = threading.Thread(target=self._server.serve_forever)
			self._thread.daemon = True
			self._thread.start()
	
	def stop(self):
		"""Stop the web server."""
		if self._server:
			self._server.shutdown()
			logger.info("Server stopped")
	
	def _create_handler(self):
		"""Create request handler with access to DLFI instance."""
		dlfi = self.dlfi
		
		class DLFIRequestHandler(BaseHTTPRequestHandler):
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
			
			def read_json_body(self):
				content_length = int(self.headers.get("Content-Length", 0))
				if content_length == 0:
					return {}
				body = self.rfile.read(content_length)
				return json.loads(body.decode("utf-8"))
			
			def parse_multipart(self):
				"""Parse multipart form data for file uploads."""
				content_type = self.headers.get("Content-Type", "")
				if "multipart/form-data" not in content_type:
					return None, None
				
				# Extract boundary
				boundary = None
				for part in content_type.split(";"):
					part = part.strip()
					if part.startswith("boundary="):
						boundary = part[9:].strip('"')
						break
				
				if not boundary:
					return None, None
				
				content_length = int(self.headers.get("Content-Length", 0))
				body = self.rfile.read(content_length)
				
				# Parse multipart
				boundary_bytes = f"--{boundary}".encode()
				parts = body.split(boundary_bytes)
				
				files = []
				fields = {}
				
				for part in parts[1:]:  # Skip first empty part
					if part.strip() == b"--" or not part.strip():
						continue
					
					# Split headers from content
					try:
						header_end = part.index(b"\r\n\r\n")
						headers_raw = part[:header_end].decode("utf-8", errors="ignore")
						content = part[header_end + 4:]
						
						# Remove trailing \r\n
						if content.endswith(b"\r\n"):
							content = content[:-2]
						
						# Parse Content-Disposition
						name = None
						filename = None
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
				
				# Serve main page
				if path == "/" or path == "/index.html":
					self.serve_html()
					return
				
				# API Routes
				if path == "/api/tree":
					self.api_get_tree()
				elif path == "/api/config":
					self.api_get_config()
				elif path.startswith("/api/node/"):
					node_path = unquote(path[10:])
					self.api_get_node(node_path)
				elif path.startswith("/api/blob/"):
					blob_hash = path[10:]
					self.api_get_blob(blob_hash)
				elif path.startswith("/api/children/"):
					parent_uuid = path[14:] if len(path) > 14 else None
					self.api_get_children(parent_uuid if parent_uuid else None)
				else:
					self.send_error_json("Not found", 404)
			
			def do_POST(self):
				parsed = urlparse(self.path)
				path = parsed.path
				
				if path == "/api/vault":
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
				elif path == "/api/config/encryption":
					self.api_config_encryption()
				elif path == "/api/config/partition":
					self.api_config_partition()
				elif path == "/api/generate-static":
					self.api_generate_static()
				else:
					self.send_error_json("Not found", 404)
			
			def do_DELETE(self):
				parsed = urlparse(self.path)
				path = parsed.path
				
				if path.startswith("/api/node/"):
					node_path = unquote(path[10:])
					self.api_delete_node(node_path)
				else:
					self.send_error_json("Not found", 404)
			
			# === API Handlers ===
			
			def api_get_tree(self):
				"""Get hierarchical tree structure."""
				try:
					cursor = dlfi.conn.execute("""
						SELECT uuid, parent_uuid, type, name, cached_path 
						FROM nodes 
						ORDER BY cached_path
					""")
					
					nodes = []
					for row in cursor:
						nodes.append({
							"uuid": row[0],
							"parent": row[1],
							"type": row[2],
							"name": row[3],
							"path": row[4]
						})
					
					self.send_json({"nodes": nodes})
				except Exception as e:
					logger.error(f"Error getting tree: {e}")
					self.send_error_json(str(e), 500)
			
			def api_get_children(self, parent_uuid: Optional[str]):
				"""Get direct children of a node."""
				try:
					if parent_uuid == "null" or parent_uuid == "":
						parent_uuid = None
					
					cursor = dlfi.conn.execute("""
						SELECT uuid, type, name, cached_path 
						FROM nodes 
						WHERE parent_uuid IS ?
						ORDER BY type DESC, name
					""", (parent_uuid,))
					
					children = []
					for row in cursor:
						# Count grandchildren
						count_cur = dlfi.conn.execute(
							"SELECT COUNT(*) FROM nodes WHERE parent_uuid = ?", (row[0],)
						)
						child_count = count_cur.fetchone()[0]
						
						children.append({
							"uuid": row[0],
							"type": row[1],
							"name": row[2],
							"path": row[3],
							"hasChildren": child_count > 0
						})
					
					self.send_json({"children": children})
				except Exception as e:
					logger.error(f"Error getting children: {e}")
					self.send_error_json(str(e), 500)
			
			def api_get_node(self, node_path: str):
				"""Get detailed node information."""
				try:
					cursor = dlfi.conn.execute("""
						SELECT uuid, parent_uuid, type, name, cached_path, metadata, created_at, last_modified
						FROM nodes WHERE cached_path = ?
					""", (node_path,))
					
					row = cursor.fetchone()
					if not row:
						self.send_error_json("Node not found", 404)
						return
					
					node = {
						"uuid": row[0],
						"parent": row[1],
						"type": row[2],
						"name": row[3],
						"path": row[4],
						"metadata": json.loads(row[5]) if row[5] else {},
						"created_at": row[6],
						"last_modified": row[7]
					}
					
					# Get tags
					tags_cur = dlfi.conn.execute(
						"SELECT tag FROM tags WHERE node_uuid = ?", (row[0],)
					)
					node["tags"] = [r[0] for r in tags_cur]
					
					# Get relationships
					edges_cur = dlfi.conn.execute("""
						SELECT e.relation, n.cached_path, e.target_uuid
						FROM edges e
						LEFT JOIN nodes n ON e.target_uuid = n.uuid
						WHERE e.source_uuid = ?
					""", (row[0],))
					node["relationships"] = [
						{"relation": r[0], "target_path": r[1], "target_uuid": r[2]}
						for r in edges_cur
					]
					
					# Get incoming relationships
					incoming_cur = dlfi.conn.execute("""
						SELECT e.relation, n.cached_path, e.source_uuid
						FROM edges e
						LEFT JOIN nodes n ON e.source_uuid = n.uuid
						WHERE e.target_uuid = ?
					""", (row[0],))
					node["incoming_relationships"] = [
						{"relation": r[0], "source_path": r[1], "source_uuid": r[2]}
						for r in incoming_cur
					]
					
					# Get files
					files_cur = dlfi.conn.execute("""
						SELECT nf.original_name, nf.file_hash, b.size_bytes, b.ext, b.part_count
						FROM node_files nf
						JOIN blobs b ON nf.file_hash = b.hash
						WHERE nf.node_uuid = ?
						ORDER BY nf.display_order
					""", (row[0],))
					node["files"] = [
						{"name": r[0], "hash": r[1], "size": r[2], "ext": r[3], "parts": r[4]}
						for r in files_cur
					]
					
					# Get children count
					count_cur = dlfi.conn.execute(
						"SELECT COUNT(*) FROM nodes WHERE parent_uuid = ?", (row[0],)
					)
					node["children_count"] = count_cur.fetchone()[0]
					
					self.send_json(node)
				except Exception as e:
					logger.error(f"Error getting node: {e}")
					self.send_error_json(str(e), 500)
			
			def api_get_blob(self, blob_hash: str):
				"""Stream blob content."""
				try:
					data = dlfi.read_blob(blob_hash)
					if data is None:
						self.send_error_json("Blob not found", 404)
						return
					
					# Get extension for mime type
					cursor = dlfi.conn.execute(
						"SELECT ext FROM blobs WHERE hash = ?", (blob_hash,)
					)
					row = cursor.fetchone()
					ext = row[0] if row else "bin"
					
					mime_type = mimetypes.guess_type(f"file.{ext}")[0] or "application/octet-stream"
					
					self.send_response(200)
					self.send_header("Content-Type", mime_type)
					self.send_header("Content-Length", len(data))
					self.send_header("Access-Control-Allow-Origin", "*")
					self.send_header("Cache-Control", "public, max-age=31536000")
					self.end_headers()
					self.wfile.write(data)
				except Exception as e:
					logger.error(f"Error getting blob: {e}")
					self.send_error_json(str(e), 500)
			
			def api_get_config(self):
				"""Get vault configuration."""
				try:
					config = {
						"encrypted": dlfi.config.encrypted,
						"partition_size": dlfi.config.partition_size,
						"version": dlfi.config.version
					}
					
					# Get stats
					node_count = dlfi.conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
					blob_count = dlfi.conn.execute("SELECT COUNT(*) FROM blobs").fetchone()[0]
					total_size = dlfi.conn.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM blobs").fetchone()[0]
					
					config["stats"] = {
						"nodes": node_count,
						"blobs": blob_count,
						"total_size": total_size
					}
					
					self.send_json(config)
				except Exception as e:
					logger.error(f"Error getting config: {e}")
					self.send_error_json(str(e), 500)
			
			def api_create_vault(self):
				"""Create a new vault."""
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					metadata = body.get("metadata", {})
					
					if not path:
						self.send_error_json("Path is required")
						return
					
					uuid = dlfi.create_vault(path, metadata=metadata if metadata else None)
					self.send_json({"uuid": uuid, "path": path})
				except Exception as e:
					logger.error(f"Error creating vault: {e}")
					self.send_error_json(str(e), 500)
			
			def api_create_record(self):
				"""Create a new record."""
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					metadata = body.get("metadata", {})
					
					if not path:
						self.send_error_json("Path is required")
						return
					
					uuid = dlfi.create_record(path, metadata=metadata if metadata else None)
					self.send_json({"uuid": uuid, "path": path})
				except Exception as e:
					logger.error(f"Error creating record: {e}")
					self.send_error_json(str(e), 500)
			
			def api_upload_file(self):
				"""Upload a file to a record."""
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
					for file_info in files:
						filename = file_info["filename"]
						data = file_info["data"]
						
						# Create a stream from bytes
						stream = io.BytesIO(data)
						dlfi.append_stream(record_path, stream, filename)
						uploaded.append(filename)
					
					self.send_json({"uploaded": uploaded})
				except Exception as e:
					logger.error(f"Error uploading file: {e}")
					self.send_error_json(str(e), 500)
			
			def api_add_tag(self):
				"""Add a tag to a node."""
				try:
					body = self.read_json_body()
					path = body.get("path", "").strip()
					tag = body.get("tag", "").strip()
					
					if not path or not tag:
						self.send_error_json("Path and tag are required")
						return
					
					dlfi.add_tag(path, tag)
					self.send_json({"success": True})
				except Exception as e:
					logger.error(f"Error adding tag: {e}")
					self.send_error_json(str(e), 500)
			
			def api_create_link(self):
				"""Create a relationship between nodes."""
				try:
					body = self.read_json_body()
					source = body.get("source", "").strip()
					target = body.get("target", "").strip()
					relation = body.get("relation", "").strip()
					
					if not source or not target or not relation:
						self.send_error_json("Source, target, and relation are required")
						return
					
					dlfi.link(source, target, relation)
					self.send_json({"success": True})
				except Exception as e:
					logger.error(f"Error creating link: {e}")
					self.send_error_json(str(e), 500)
			
			def api_query(self):
				"""Execute a complex query."""
				try:
					body = self.read_json_body()
					
					qb = dlfi.query()
					
					# Apply filters
					if body.get("inside"):
						qb.inside(body["inside"])
					
					if body.get("type"):
						qb.type(body["type"])
					
					if body.get("has_tag"):
						for tag in (body["has_tag"] if isinstance(body["has_tag"], list) else [body["has_tag"]]):
							qb.has_tag(tag)
					
					if body.get("meta_eq"):
						for key, value in body["meta_eq"].items():
							qb.meta_eq(key, value)
					
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
					
					results = qb.execute()
					self.send_json({"results": results, "count": len(results)})
				except Exception as e:
					logger.error(f"Error executing query: {e}")
					self.send_error_json(str(e), 500)
			
			def api_delete_node(self, node_path: str):
				"""Delete a node and its children."""
				try:
					cursor = dlfi.conn.execute(
						"SELECT uuid FROM nodes WHERE cached_path = ?", (node_path,)
					)
					row = cursor.fetchone()
					if not row:
						self.send_error_json("Node not found", 404)
						return
					
					# CASCADE will handle children due to FK
					with dlfi.conn:
						dlfi.conn.execute("DELETE FROM nodes WHERE uuid = ?", (row[0],))
					
					self.send_json({"success": True, "deleted": node_path})
				except Exception as e:
					logger.error(f"Error deleting node: {e}")
					self.send_error_json(str(e), 500)
			
			def api_config_encryption(self):
				"""Manage encryption settings."""
				try:
					body = self.read_json_body()
					action = body.get("action", "")
					
					if action == "enable":
						password = body.get("password", "")
						if not password:
							self.send_error_json("Password required")
							return
						success = dlfi.config_manager.enable_encryption(password)
						self.send_json({"success": success})
					
					elif action == "disable":
						password = body.get("password", "")
						if not password:
							self.send_error_json("Current password required")
							return
						success = dlfi.config_manager.disable_encryption(password)
						self.send_json({"success": success})
					
					elif action == "change_password":
						old_password = body.get("old_password", "")
						new_password = body.get("new_password", "")
						if not old_password or not new_password:
							self.send_error_json("Both old and new passwords required")
							return
						success = dlfi.config_manager.change_password(old_password, new_password)
						self.send_json({"success": success})
					
					else:
						self.send_error_json("Invalid action. Use: enable, disable, change_password")
						
				except Exception as e:
					logger.error(f"Error managing encryption: {e}")
					self.send_error_json(str(e), 500)
			
			def api_config_partition(self):
				"""Change partition size."""
				try:
					body = self.read_json_body()
					size = body.get("size")
					
					if size is None:
						self.send_error_json("Size is required (bytes, 0 to disable)")
						return
					
					success = dlfi.config_manager.change_partition_size(int(size))
					self.send_json({"success": success})
				except Exception as e:
					logger.error(f"Error changing partition size: {e}")
					self.send_error_json(str(e), 500)
			
			def api_generate_static(self):
				"""Generate static site."""
				try:
					dlfi.generate_static_site()
					self.send_json({"success": True, "path": str(dlfi.root / "index.html")})
				except Exception as e:
					logger.error(f"Error generating static site: {e}")
					self.send_error_json(str(e), 500)
			
			def serve_html(self):
				"""Serve the main application HTML."""
				html = get_app_html()
				self.send_response(200)
				self.send_header("Content-Type", "text/html; charset=utf-8")
				self.end_headers()
				self.wfile.write(html.encode("utf-8"))
		
		return DLFIRequestHandler


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
			--bg-primary: #0a0a0a;
			--bg-secondary: #111111;
			--bg-tertiary: #1a1a1a;
			--bg-hover: #222222;
			--text-primary: #ffffff;
			--text-secondary: #888888;
			--text-muted: #555555;
			--accent: #3b82f6;
			--accent-hover: #2563eb;
			--border: #2a2a2a;
			--success: #22c55e;
			--warning: #eab308;
			--error: #ef4444;
		}
		
		body {
			font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
			background: var(--bg-primary);
			color: var(--text-primary);
			height: 100vh;
			overflow: hidden;
		}
		
		/* Layout */
		.app {
			display: grid;
			grid-template-rows: 48px 1fr;
			height: 100vh;
		}
		
		.header {
			background: var(--bg-secondary);
			border-bottom: 1px solid var(--border);
			display: flex;
			align-items: center;
			justify-content: space-between;
			padding: 0 16px;
		}
		
		.header-left {
			display: flex;
			align-items: center;
			gap: 16px;
		}
		
		.logo {
			font-weight: 600;
			font-size: 1.1rem;
			letter-spacing: -0.02em;
		}
		
		.header-stats {
			display: flex;
			gap: 16px;
			font-size: 0.75rem;
			color: var(--text-secondary);
		}
		
		.stat {
			display: flex;
			align-items: center;
			gap: 4px;
		}
		
		.stat-value {
			color: var(--text-primary);
			font-weight: 500;
		}
		
		.header-actions {
			display: flex;
			gap: 8px;
		}
		
		.main {
			display: grid;
			grid-template-columns: 280px 1fr 320px;
			overflow: hidden;
		}
		
		/* Sidebar */
		.sidebar {
			background: var(--bg-secondary);
			border-right: 1px solid var(--border);
			display: flex;
			flex-direction: column;
			overflow: hidden;
		}
		
		.sidebar-header {
			padding: 12px;
			border-bottom: 1px solid var(--border);
			display: flex;
			justify-content: space-between;
			align-items: center;
		}
		
		.sidebar-title {
			font-size: 0.75rem;
			text-transform: uppercase;
			letter-spacing: 0.05em;
			color: var(--text-secondary);
		}
		
		.tree-container {
			flex: 1;
			overflow-y: auto;
			padding: 8px 0;
		}
		
		.tree-item {
			display: flex;
			align-items: center;
			padding: 6px 12px;
			cursor: pointer;
			user-select: none;
			font-size: 0.875rem;
			transition: background 0.1s;
		}
		
		.tree-item:hover {
			background: var(--bg-hover);
		}
		
		.tree-item.selected {
			background: var(--accent);
		}
		
		.tree-toggle {
			width: 16px;
			height: 16px;
			display: flex;
			align-items: center;
			justify-content: center;
			margin-right: 4px;
			color: var(--text-muted);
			font-size: 10px;
			flex-shrink: 0;
		}
		
		.tree-toggle.expanded {
			transform: rotate(90deg);
		}
		
		.tree-toggle.hidden {
			visibility: hidden;
		}
		
		.tree-icon {
			margin-right: 8px;
			font-size: 14px;
		}
		
		.tree-name {
			flex: 1;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
		}
		
		.tree-children {
			display: none;
		}
		
		.tree-children.expanded {
			display: block;
		}
		
		/* Content */
		.content {
			display: flex;
			flex-direction: column;
			overflow: hidden;
			background: var(--bg-primary);
		}
		
		.content-header {
			padding: 16px 24px;
			border-bottom: 1px solid var(--border);
			background: var(--bg-secondary);
		}
		
		.content-breadcrumb {
			font-size: 0.75rem;
			color: var(--text-secondary);
			margin-bottom: 4px;
		}
		
		.content-title {
			font-size: 1.25rem;
			font-weight: 600;
			display: flex;
			align-items: center;
			gap: 8px;
		}
		
		.content-type {
			font-size: 0.625rem;
			text-transform: uppercase;
			padding: 2px 6px;
			background: var(--bg-tertiary);
			color: var(--text-secondary);
		}
		
		.content-body {
			flex: 1;
			overflow-y: auto;
			padding: 24px;
		}
		
		.content-section {
			margin-bottom: 32px;
		}
		
		.section-title {
			font-size: 0.7rem;
			text-transform: uppercase;
			letter-spacing: 0.05em;
			color: var(--text-secondary);
			margin-bottom: 12px;
			display: flex;
			align-items: center;
			justify-content: space-between;
		}
		
		/* Metadata Grid */
		.meta-grid {
			display: grid;
			grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
			gap: 8px;
		}
		
		.meta-item {
			background: var(--bg-secondary);
			padding: 12px;
			border: 1px solid var(--border);
		}
		
		.meta-label {
			font-size: 0.7rem;
			color: var(--text-secondary);
			margin-bottom: 4px;
			text-transform: uppercase;
		}
		
		.meta-value {
			font-size: 0.875rem;
			word-break: break-word;
		}
		
		/* Tags */
		.tags {
			display: flex;
			flex-wrap: wrap;
			gap: 6px;
		}
		
		.tag {
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			padding: 4px 10px;
			font-size: 0.75rem;
			color: var(--text-secondary);
		}
		
		/* Relationships */
		.rel-list {
			display: flex;
			flex-direction: column;
			gap: 6px;
		}
		
		.rel-item {
			display: flex;
			align-items: center;
			gap: 12px;
			padding: 10px 12px;
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			cursor: pointer;
			transition: border-color 0.1s;
		}
		
		.rel-item:hover {
			border-color: var(--accent);
		}
		
		.rel-type {
			font-size: 0.65rem;
			text-transform: uppercase;
			color: var(--accent);
			font-weight: 600;
			min-width: 100px;
		}
		
		.rel-target {
			font-size: 0.8rem;
			color: var(--text-secondary);
		}
		
		.rel-direction {
			font-size: 0.65rem;
			color: var(--text-muted);
			margin-left: auto;
		}
		
		/* Files Grid */
		.files-grid {
			display: grid;
			grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
			gap: 12px;
		}
		
		.file-card {
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			cursor: pointer;
			transition: border-color 0.1s;
			overflow: hidden;
		}
		
		.file-card:hover {
			border-color: var(--accent);
		}
		
		.file-preview {
			aspect-ratio: 1;
			background: var(--bg-tertiary);
			display: flex;
			align-items: center;
			justify-content: center;
			overflow: hidden;
		}
		
		.file-preview img,
		.file-preview video {
			width: 100%;
			height: 100%;
			object-fit: cover;
		}
		
		.file-icon {
			font-size: 2rem;
			color: var(--text-muted);
		}
		
		.file-info {
			padding: 10px;
		}
		
		.file-name {
			font-size: 0.8rem;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
			margin-bottom: 2px;
		}
		
		.file-size {
			font-size: 0.7rem;
			color: var(--text-secondary);
		}
		
		/* Children Grid */
		.children-grid {
			display: grid;
			grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
			gap: 8px;
		}
		
		.child-card {
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			padding: 12px;
			cursor: pointer;
			transition: border-color 0.1s;
			text-align: center;
		}
		
		.child-card:hover {
			border-color: var(--accent);
		}
		
		.child-icon {
			font-size: 1.5rem;
			margin-bottom: 8px;
		}
		
		.child-name {
			font-size: 0.8rem;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
		}
		
		/* Right Panel */
		.panel {
			background: var(--bg-secondary);
			border-left: 1px solid var(--border);
			display: flex;
			flex-direction: column;
			overflow: hidden;
		}
		
		.panel-tabs {
			display: flex;
			border-bottom: 1px solid var(--border);
		}
		
		.panel-tab {
			flex: 1;
			padding: 12px;
			text-align: center;
			font-size: 0.75rem;
			text-transform: uppercase;
			letter-spacing: 0.03em;
			color: var(--text-secondary);
			cursor: pointer;
			border-bottom: 2px solid transparent;
			transition: all 0.1s;
		}
		
		.panel-tab:hover {
			color: var(--text-primary);
		}
		
		.panel-tab.active {
			color: var(--accent);
			border-bottom-color: var(--accent);
		}
		
		.panel-content {
			flex: 1;
			overflow-y: auto;
			padding: 16px;
		}
		
		.panel-section {
			display: none;
		}
		
		.panel-section.active {
			display: block;
		}
		
		/* Forms */
		.form-group {
			margin-bottom: 16px;
		}
		
		.form-label {
			display: block;
			font-size: 0.75rem;
			color: var(--text-secondary);
			margin-bottom: 6px;
			text-transform: uppercase;
		}
		
		.form-input,
		.form-textarea,
		.form-select {
			width: 100%;
			padding: 10px 12px;
			background: var(--bg-tertiary);
			border: 1px solid var(--border);
			color: var(--text-primary);
			font-size: 0.875rem;
			font-family: inherit;
			outline: none;
			transition: border-color 0.1s;
		}
		
		.form-input:focus,
		.form-textarea:focus,
		.form-select:focus {
			border-color: var(--accent);
		}
		
		.form-textarea {
			min-height: 80px;
			resize: vertical;
		}
		
		.form-hint {
			font-size: 0.7rem;
			color: var(--text-muted);
			margin-top: 4px;
		}
		
		/* Buttons */
		.btn {
			padding: 8px 16px;
			font-size: 0.8rem;
			font-weight: 500;
			border: 1px solid var(--border);
			background: var(--bg-tertiary);
			color: var(--text-primary);
			cursor: pointer;
			transition: all 0.1s;
			font-family: inherit;
		}
		
		.btn:hover {
			background: var(--bg-hover);
			border-color: var(--text-muted);
		}
		
		.btn-primary {
			background: var(--accent);
			border-color: var(--accent);
		}
		
		.btn-primary:hover {
			background: var(--accent-hover);
			border-color: var(--accent-hover);
		}
		
		.btn-sm {
			padding: 4px 8px;
			font-size: 0.7rem;
		}
		
		.btn-danger {
			border-color: var(--error);
			color: var(--error);
		}
		
		.btn-danger:hover {
			background: var(--error);
			color: white;
		}
		
		/* Query Builder */
		.query-filters {
			display: flex;
			flex-direction: column;
			gap: 12px;
		}
		
		.query-filter {
			display: grid;
			grid-template-columns: 100px 1fr auto;
			gap: 8px;
			align-items: start;
		}
		
		.query-results {
			margin-top: 16px;
			max-height: 300px;
			overflow-y: auto;
		}
		
		.query-result-item {
			padding: 8px;
			border-bottom: 1px solid var(--border);
			font-size: 0.8rem;
			cursor: pointer;
		}
		
		.query-result-item:hover {
			background: var(--bg-hover);
		}
		
		.query-result-path {
			color: var(--text-secondary);
		}
		
		.query-result-type {
			font-size: 0.65rem;
			color: var(--text-muted);
		}
		
		/* File Upload */
		.upload-zone {
			border: 2px dashed var(--border);
			padding: 24px;
			text-align: center;
			cursor: pointer;
			transition: border-color 0.1s;
		}
		
		.upload-zone:hover,
		.upload-zone.dragover {
			border-color: var(--accent);
		}
		
		.upload-zone-text {
			color: var(--text-secondary);
			font-size: 0.875rem;
		}
		
		.upload-input {
			display: none;
		}
		
		/* Modal */
		.modal-overlay {
			position: fixed;
			inset: 0;
			background: rgba(0, 0, 0, 0.8);
			display: flex;
			align-items: center;
			justify-content: center;
			z-index: 1000;
		}
		
		.modal {
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			max-width: 90vw;
			max-height: 90vh;
			overflow: auto;
		}
		
		.modal-header {
			padding: 16px;
			border-bottom: 1px solid var(--border);
			display: flex;
			justify-content: space-between;
			align-items: center;
		}
		
		.modal-title {
			font-size: 1rem;
			font-weight: 600;
		}
		
		.modal-close {
			background: none;
			border: none;
			color: var(--text-secondary);
			font-size: 1.5rem;
			cursor: pointer;
			line-height: 1;
		}
		
		.modal-body {
			padding: 16px;
		}
		
		/* Lightbox */
		.lightbox {
			position: fixed;
			inset: 0;
			background: rgba(0, 0, 0, 0.95);
			display: flex;
			align-items: center;
			justify-content: center;
			z-index: 2000;
		}
		
		.lightbox img,
		.lightbox video {
			max-width: 95vw;
			max-height: 95vh;
			object-fit: contain;
		}
		
		.lightbox-close {
			position: absolute;
			top: 16px;
			right: 16px;
			background: none;
			border: none;
			color: white;
			font-size: 2rem;
			cursor: pointer;
		}
		
		/* Utilities */
		.hidden {
			display: none !important;
		}
		
		.text-muted {
			color: var(--text-muted);
		}
		
		.text-success {
			color: var(--success);
		}
		
		.text-error {
			color: var(--error);
		}
		
		.empty-state {
			display: flex;
			flex-direction: column;
			align-items: center;
			justify-content: center;
			padding: 48px;
			color: var(--text-secondary);
			text-align: center;
		}
		
		.empty-state h3 {
			color: var(--text-primary);
			margin-bottom: 8px;
		}
		
		.toast {
			position: fixed;
			bottom: 24px;
			right: 24px;
			padding: 12px 20px;
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			font-size: 0.875rem;
			z-index: 3000;
			animation: slideIn 0.2s ease;
		}
		
		.toast.success {
			border-color: var(--success);
		}
		
		.toast.error {
			border-color: var(--error);
		}
		
		@keyframes slideIn {
			from { transform: translateY(20px); opacity: 0; }
			to { transform: translateY(0); opacity: 1; }
		}
		
		/* Scrollbar */
		::-webkit-scrollbar {
			width: 8px;
			height: 8px;
		}
		
		::-webkit-scrollbar-track {
			background: var(--bg-primary);
		}
		
		::-webkit-scrollbar-thumb {
			background: var(--border);
		}
		
		::-webkit-scrollbar-thumb:hover {
			background: var(--text-muted);
		}
	</style>
</head>
<body>
	<div class="app">
		<header class="header">
			<div class="header-left">
				<div class="logo">DLFI Archive</div>
				<div class="header-stats">
					<span class="stat">Nodes: <span class="stat-value" id="statNodes">0</span></span>
					<span class="stat">Blobs: <span class="stat-value" id="statBlobs">0</span></span>
					<span class="stat">Size: <span class="stat-value" id="statSize">0 B</span></span>
					<span class="stat">Encrypted: <span class="stat-value" id="statEncrypted">No</span></span>
				</div>
			</div>
			<div class="header-actions">
				<button class="btn btn-sm" onclick="generateStatic()">Generate Static</button>
				<button class="btn btn-sm" onclick="showSettings()">Settings</button>
			</div>
		</header>
		
		<div class="main">
			<!-- Sidebar Tree -->
			<aside class="sidebar">
				<div class="sidebar-header">
					<span class="sidebar-title">Explorer</span>
					<button class="btn btn-sm" onclick="refreshTree()">↻</button>
				</div>
				<div class="tree-container" id="treeContainer"></div>
			</aside>
			
			<!-- Main Content -->
			<main class="content" id="contentArea">
				<div class="empty-state">
					<h3>Welcome to DLFI</h3>
					<p>Select a node from the tree or create a new vault.</p>
				</div>
			</main>
			
			<!-- Right Panel -->
			<aside class="panel">
				<div class="panel-tabs">
					<div class="panel-tab active" data-tab="create" onclick="switchTab('create')">Create</div>
					<div class="panel-tab" data-tab="query" onclick="switchTab('query')">Query</div>
					<div class="panel-tab" data-tab="actions" onclick="switchTab('actions')">Actions</div>
				</div>
				<div class="panel-content">
					<!-- Create Tab -->
					<div class="panel-section active" id="tabCreate">
						<div class="form-group">
							<label class="form-label">Type</label>
							<select class="form-select" id="createType">
								<option value="VAULT">Vault (Folder)</option>
								<option value="RECORD">Record (Item)</option>
							</select>
						</div>
						<div class="form-group">
							<label class="form-label">Path</label>
							<input type="text" class="form-input" id="createPath" placeholder="parent/child/name">
							<div class="form-hint">Use forward slashes. Parents created automatically.</div>
						</div>
						<div class="form-group">
							<label class="form-label">Metadata (JSON)</label>
							<textarea class="form-textarea" id="createMeta" placeholder='{"key": "value"}'></textarea>
						</div>
						<button class="btn btn-primary" style="width:100%" onclick="createNode()">Create</button>
						
						<hr style="margin: 24px 0; border: none; border-top: 1px solid var(--border)">
						
						<div class="form-group">
							<label class="form-label">Upload Files</label>
							<input type="text" class="form-input" id="uploadPath" placeholder="Record path">
						</div>
						<div class="upload-zone" id="uploadZone">
							<input type="file" class="upload-input" id="uploadInput" multiple>
							<div class="upload-zone-text">Drop files here or click to upload</div>
						</div>
					</div>
					
					<!-- Query Tab -->
					<div class="panel-section" id="tabQuery">
						<div class="query-filters" id="queryFilters">
							<div class="query-filter">
								<select class="form-select" id="qFilterType">
									<option value="">Filter...</option>
									<option value="inside">Inside Path</option>
									<option value="type">Node Type</option>
									<option value="has_tag">Has Tag</option>
									<option value="related_to">Related To</option>
									<option value="contains_related">Contains Related</option>
								</select>
								<input type="text" class="form-input" id="qFilterValue" placeholder="Value">
								<button class="btn btn-sm" onclick="addQueryFilter()">+</button>
							</div>
						</div>
						
						<div id="activeFilters" style="margin-top: 12px; display: flex; flex-wrap: wrap; gap: 6px;"></div>
						
						<button class="btn btn-primary" style="width: 100%; margin-top: 16px" onclick="executeQuery()">Run Query</button>
						
						<div class="query-results" id="queryResults"></div>
					</div>
					
					<!-- Actions Tab -->
					<div class="panel-section" id="tabActions">
						<div class="form-group">
							<label class="form-label">Add Tag to Selected</label>
							<div style="display: flex; gap: 8px;">
								<input type="text" class="form-input" id="actionTag" placeholder="tag_name">
								<button class="btn" onclick="addTagToSelected()">Add</button>
							</div>
						</div>
						
						<hr style="margin: 24px 0; border: none; border-top: 1px solid var(--border)">
						
						<div class="form-group">
							<label class="form-label">Create Relationship</label>
							<input type="text" class="form-input" id="linkSource" placeholder="Source path" style="margin-bottom: 8px">
							<input type="text" class="form-input" id="linkTarget" placeholder="Target path" style="margin-bottom: 8px">
							<input type="text" class="form-input" id="linkRelation" placeholder="RELATION_NAME" style="margin-bottom: 8px">
							<button class="btn" style="width: 100%" onclick="createLink()">Link</button>
						</div>
						
						<hr style="margin: 24px 0; border: none; border-top: 1px solid var(--border)">
						
						<div class="form-group">
							<label class="form-label">Delete Selected Node</label>
							<button class="btn btn-danger" style="width: 100%" onclick="deleteSelected()">Delete Node</button>
							<div class="form-hint">This will delete all children and linked files.</div>
						</div>
					</div>
				</div>
			</aside>
		</div>
	</div>
	
	<!-- Lightbox -->
	<div class="lightbox hidden" id="lightbox" onclick="closeLightbox(event)">
		<button class="lightbox-close" onclick="closeLightbox()">&times;</button>
		<div id="lightboxContent"></div>
	</div>
	
	<script>
		// State
		let selectedPath = null;
		let queryFilters = [];
		let expandedNodes = new Set();
		
		// API Helpers
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
		
		function toast(message, type = 'info') {
			const el = document.createElement('div');
			el.className = `toast ${type}`;
			el.textContent = message;
			document.body.appendChild(el);
			setTimeout(() => el.remove(), 3000);
		}
		
		function formatSize(bytes) {
			if (bytes === 0) return '0 B';
			const k = 1024;
			const sizes = ['B', 'KB', 'MB', 'GB'];
			const i = Math.floor(Math.log(bytes) / Math.log(k));
			return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
		}
		
		// Tree View
		async function loadChildren(parentUuid, container, depth = 0) {
			try {
				const data = await api('GET', `/api/children/${parentUuid || ''}`);
				
				for (const child of data.children) {
					const item = document.createElement('div');
					item.className = 'tree-node';
					item.dataset.uuid = child.uuid;
					item.dataset.path = child.path;
					
					const row = document.createElement('div');
					row.className = 'tree-item';
					row.style.paddingLeft = `${12 + depth * 16}px`;
					
					const toggle = document.createElement('span');
					toggle.className = `tree-toggle ${child.hasChildren ? '' : 'hidden'}`;
					toggle.textContent = '▶';
					if (expandedNodes.has(child.uuid)) {
						toggle.classList.add('expanded');
					}
					
					const icon = document.createElement('span');
					icon.className = 'tree-icon';
					icon.textContent = child.type === 'VAULT' ? '📁' : '📄';
					
					const name = document.createElement('span');
					name.className = 'tree-name';
					name.textContent = child.name;
					
					row.appendChild(toggle);
					row.appendChild(icon);
					row.appendChild(name);
					
					row.onclick = (e) => {
						e.stopPropagation();
						if (e.target === toggle && child.hasChildren) {
							toggleNode(child.uuid, item, depth);
						} else {
							selectNode(child.path);
						}
					};
					
					item.appendChild(row);
					
					const childContainer = document.createElement('div');
					childContainer.className = `tree-children ${expandedNodes.has(child.uuid) ? 'expanded' : ''}`;
					item.appendChild(childContainer);
					
					container.appendChild(item);
					
					// Load expanded children
					if (expandedNodes.has(child.uuid) && child.hasChildren) {
						await loadChildren(child.uuid, childContainer, depth + 1);
					}
				}
			} catch (e) {
				console.error('Failed to load children:', e);
			}
		}
		
		async function toggleNode(uuid, item, depth) {
			const toggle = item.querySelector('.tree-toggle');
			const childContainer = item.querySelector('.tree-children');
			
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
			const container = document.getElementById('treeContainer');
			container.innerHTML = '';
			await loadChildren(null, container, 0);
		}
		
		async function selectNode(path) {
			selectedPath = path;
			
			// Update tree selection
			document.querySelectorAll('.tree-item').forEach(el => el.classList.remove('selected'));
			const node = document.querySelector(`[data-path="${path}"] > .tree-item`);
			if (node) node.classList.add('selected');
			
			// Update forms
			document.getElementById('uploadPath').value = path;
			document.getElementById('linkSource').value = path;
			
			// Load node details
			try {
				const data = await api('GET', `/api/node/${encodeURIComponent(path)}`);
				renderNodeDetails(data);
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		function renderNodeDetails(node) {
			const content = document.getElementById('contentArea');
			
			let html = `
				<div class="content-header">
					<div class="content-breadcrumb">${node.path}</div>
					<div class="content-title">
						${node.type === 'VAULT' ? '📁' : '📄'} ${node.name}
						<span class="content-type">${node.type}</span>
					</div>
				</div>
				<div class="content-body">
			`;
			
			// Metadata
			if (Object.keys(node.metadata).length > 0) {
				html += `
					<div class="content-section">
						<div class="section-title">Metadata</div>
						<div class="meta-grid">
							${Object.entries(node.metadata).map(([k, v]) => `
								<div class="meta-item">
									<div class="meta-label">${k}</div>
									<div class="meta-value">${typeof v === 'object' ? JSON.stringify(v) : v}</div>
								</div>
							`).join('')}
						</div>
					</div>
				`;
			}
			
			// Tags
			if (node.tags.length > 0) {
				html += `
					<div class="content-section">
						<div class="section-title">Tags</div>
						<div class="tags">
							${node.tags.map(t => `<span class="tag">${t}</span>`).join('')}
						</div>
					</div>
				`;
			}
			
			// Relationships
			if (node.relationships.length > 0 || node.incoming_relationships.length > 0) {
				html += `<div class="content-section"><div class="section-title">Relationships</div><div class="rel-list">`;
				
				for (const rel of node.relationships) {
					html += `
						<div class="rel-item" onclick="selectNode('${rel.target_path}')">
							<span class="rel-type">${rel.relation}</span>
							<span class="rel-target">${rel.target_path}</span>
							<span class="rel-direction">→ outgoing</span>
						</div>
					`;
				}
				
				for (const rel of node.incoming_relationships) {
					html += `
						<div class="rel-item" onclick="selectNode('${rel.source_path}')">
							<span class="rel-type">${rel.relation}</span>
							<span class="rel-target">${rel.source_path}</span>
							<span class="rel-direction">← incoming</span>
						</div>
					`;
				}
				
				html += `</div></div>`;
			}
			
			// Files
			if (node.files.length > 0) {
				html += `
					<div class="content-section">
						<div class="section-title">Files (${node.files.length})</div>
						<div class="files-grid">
							${node.files.map(f => {
								const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(f.ext);
								const isVideo = ['mp4', 'webm', 'mov'].includes(f.ext);
								return `
									<div class="file-card" onclick="openFile('${f.hash}', '${f.ext}', '${f.name}')">
										<div class="file-preview">
											${isImage ? `<img src="/api/blob/${f.hash}" loading="lazy">` : 
											isVideo ? `<video src="/api/blob/${f.hash}" muted></video>` :
											`<span class="file-icon">📎</span>`}
										</div>
										<div class="file-info">
											<div class="file-name" title="${f.name}">${f.name}</div>
											<div class="file-size">${formatSize(f.size)}</div>
										</div>
									</div>
								`;
							}).join('')}
						</div>
					</div>
				`;
			}
			
			// Children
			if (node.children_count > 0) {
				html += `
					<div class="content-section">
						<div class="section-title">Children (${node.children_count})</div>
						<div id="childrenGrid" class="children-grid"></div>
					</div>
				`;
			}
			
			html += '</div>';
			content.innerHTML = html;
			
			// Load children grid
			if (node.children_count > 0) {
				loadChildrenGrid(node.uuid);
			}
		}
		
		async function loadChildrenGrid(parentUuid) {
			try {
				const data = await api('GET', `/api/children/${parentUuid}`);
				const grid = document.getElementById('childrenGrid');
				
				grid.innerHTML = data.children.map(c => `
					<div class="child-card" onclick="selectNode('${c.path}')">
						<div class="child-icon">${c.type === 'VAULT' ? '📁' : '📄'}</div>
						<div class="child-name">${c.name}</div>
					</div>
				`).join('');
			} catch (e) {
				console.error('Failed to load children grid:', e);
			}
		}
		
		// Lightbox
		function openFile(hash, ext, name) {
			const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(ext);
			const isVideo = ['mp4', 'webm', 'mov'].includes(ext);
			
			if (isImage) {
				document.getElementById('lightboxContent').innerHTML = `<img src="/api/blob/${hash}">`;
				document.getElementById('lightbox').classList.remove('hidden');
			} else if (isVideo) {
				document.getElementById('lightboxContent').innerHTML = `<video src="/api/blob/${hash}" controls autoplay></video>`;
				document.getElementById('lightbox').classList.remove('hidden');
			} else {
				// Download
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
		
		// Panel Tabs
		function switchTab(tab) {
			document.querySelectorAll('.panel-tab').forEach(t => t.classList.remove('active'));
			document.querySelector(`[data-tab="${tab}"]`).classList.add('active');
			
			document.querySelectorAll('.panel-section').forEach(s => s.classList.remove('active'));
			document.getElementById(`tab${tab.charAt(0).toUpperCase() + tab.slice(1)}`).classList.add('active');
		}
		
		// Create Node
		async function createNode() {
			const type = document.getElementById('createType').value;
			const path = document.getElementById('createPath').value.trim();
			let metadata = {};
			
			try {
				const metaStr = document.getElementById('createMeta').value.trim();
				if (metaStr) metadata = JSON.parse(metaStr);
			} catch (e) {
				toast('Invalid JSON in metadata', 'error');
				return;
			}
			
			if (!path) {
				toast('Path is required', 'error');
				return;
			}
			
			try {
				const endpoint = type === 'VAULT' ? '/api/vault' : '/api/record';
				await api('POST', endpoint, { path, metadata });
				toast(`${type} created: ${path}`, 'success');
				document.getElementById('createPath').value = '';
				document.getElementById('createMeta').value = '';
				refreshTree();
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		// File Upload
		const uploadZone = document.getElementById('uploadZone');
		const uploadInput = document.getElementById('uploadInput');
		
		uploadZone.onclick = () => uploadInput.click();
		
		uploadZone.ondragover = (e) => {
			e.preventDefault();
			uploadZone.classList.add('dragover');
		};
		
		uploadZone.ondragleave = () => uploadZone.classList.remove('dragover');
		
		uploadZone.ondrop = async (e) => {
			e.preventDefault();
			uploadZone.classList.remove('dragover');
			await uploadFiles(e.dataTransfer.files);
		};
		
		uploadInput.onchange = async () => {
			await uploadFiles(uploadInput.files);
			uploadInput.value = '';
		};
		
		async function uploadFiles(files) {
			const path = document.getElementById('uploadPath').value.trim();
			if (!path) {
				toast('Enter a record path first', 'error');
				return;
			}
			
			const formData = new FormData();
			formData.append('path', path);
			for (const file of files) {
				formData.append('file', file, file.name);
			}
			
			try {
				await api('POST', '/api/upload', formData);
				toast(`Uploaded ${files.length} file(s)`, 'success');
				if (selectedPath === path) {
					selectNode(path);
				}
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		// Query Builder
		function addQueryFilter() {
			const type = document.getElementById('qFilterType').value;
			const value = document.getElementById('qFilterValue').value.trim();
			
			if (!type || !value) return;
			
			queryFilters.push({ type, value });
			renderQueryFilters();
			
			document.getElementById('qFilterType').value = '';
			document.getElementById('qFilterValue').value = '';
		}
		
		function renderQueryFilters() {
			const container = document.getElementById('activeFilters');
			container.innerHTML = queryFilters.map((f, i) => `
				<span class="tag" style="cursor: pointer" onclick="removeQueryFilter(${i})">
					${f.type}: ${f.value} ✕
				</span>
			`).join('');
		}
		
		function removeQueryFilter(index) {
			queryFilters.splice(index, 1);
			renderQueryFilters();
		}
		
		async function executeQuery() {
			const query = {};
			
			for (const f of queryFilters) {
				if (f.type === 'has_tag') {
					if (!query.has_tag) query.has_tag = [];
					query.has_tag.push(f.value);
				} else if (f.type === 'related_to') {
					// Parse "path:RELATION" or just "path"
					const parts = f.value.split(':');
					query.related_to = { target: parts[0], relation: parts[1] || null };
				} else if (f.type === 'contains_related') {
					const parts = f.value.split(':');
					query.contains_related = { target: parts[0], relation: parts[1] || null };
				} else {
					query[f.type] = f.value;
				}
			}
			
			try {
				const data = await api('POST', '/api/query', query);
				renderQueryResults(data.results);
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		function renderQueryResults(results) {
			const container = document.getElementById('queryResults');
			
			if (results.length === 0) {
				container.innerHTML = '<div class="empty-state" style="padding: 24px"><p>No results found</p></div>';
				return;
			}
			
			container.innerHTML = results.map(r => `
				<div class="query-result-item" onclick="selectNode('${r.path}')">
					<div class="query-result-path">${r.path}</div>
					<div class="query-result-type">${r.type}</div>
				</div>
			`).join('');
		}
		
		// Actions
		async function addTagToSelected() {
			if (!selectedPath) {
				toast('Select a node first', 'error');
				return;
			}
			
			const tag = document.getElementById('actionTag').value.trim();
			if (!tag) {
				toast('Enter a tag', 'error');
				return;
			}
			
			try {
				await api('POST', '/api/tag', { path: selectedPath, tag });
				toast('Tag added', 'success');
				document.getElementById('actionTag').value = '';
				selectNode(selectedPath);
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		async function createLink() {
			const source = document.getElementById('linkSource').value.trim();
			const target = document.getElementById('linkTarget').value.trim();
			const relation = document.getElementById('linkRelation').value.trim();
			
			if (!source || !target || !relation) {
				toast('All fields required', 'error');
				return;
			}
			
			try {
				await api('POST', '/api/link', { source, target, relation });
				toast('Relationship created', 'success');
				if (selectedPath === source || selectedPath === target) {
					selectNode(selectedPath);
				}
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		async function deleteSelected() {
			if (!selectedPath) {
				toast('Select a node first', 'error');
				return;
			}
			
			if (!confirm(`Delete "${selectedPath}" and all its children?`)) return;
			
			try {
				await api('DELETE', `/api/node/${encodeURIComponent(selectedPath)}`);
				toast('Node deleted', 'success');
				selectedPath = null;
				document.getElementById('contentArea').innerHTML = '<div class="empty-state"><h3>Node deleted</h3></div>';
				refreshTree();
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		// Settings
		async function showSettings() {
			const config = await api('GET', '/api/config');
			
			const overlay = document.createElement('div');
			overlay.className = 'modal-overlay';
			overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
			
			overlay.innerHTML = `
				<div class="modal" style="width: 400px">
					<div class="modal-header">
						<span class="modal-title">Vault Settings</span>
						<button class="modal-close" onclick="this.closest('.modal-overlay').remove()">&times;</button>
					</div>
					<div class="modal-body">
						<div class="form-group">
							<label class="form-label">Encryption</label>
							<div style="margin-bottom: 8px; color: var(--text-secondary)">
								Status: ${config.encrypted ? '<span class="text-success">Enabled</span>' : 'Disabled'}
							</div>
							${config.encrypted ? `
								<input type="password" class="form-input" id="settingsOldPass" placeholder="Current password" style="margin-bottom: 8px">
								<input type="password" class="form-input" id="settingsNewPass" placeholder="New password (blank to disable)">
								<button class="btn" style="width: 100%; margin-top: 8px" onclick="updateEncryption()">Update</button>
							` : `
								<input type="password" class="form-input" id="settingsNewPass" placeholder="Password to enable">
								<button class="btn" style="width: 100%; margin-top: 8px" onclick="enableEncryption()">Enable Encryption</button>
							`}
						</div>
						
						<hr style="margin: 20px 0; border: none; border-top: 1px solid var(--border)">
						
						<div class="form-group">
							<label class="form-label">Partition Size</label>
							<div style="margin-bottom: 8px; color: var(--text-secondary)">
								Current: ${config.partition_size ? formatSize(config.partition_size) : 'Disabled'}
							</div>
							<select class="form-select" id="settingsPartition">
								<option value="0" ${config.partition_size === 0 ? 'selected' : ''}>Disabled</option>
								<option value="10485760" ${config.partition_size === 10485760 ? 'selected' : ''}>10 MB</option>
								<option value="26214400" ${config.partition_size === 26214400 ? 'selected' : ''}>25 MB</option>
								<option value="52428800" ${config.partition_size === 52428800 ? 'selected' : ''}>50 MB</option>
								<option value="104857600" ${config.partition_size === 104857600 ? 'selected' : ''}>100 MB</option>
							</select>
							<button class="btn" style="width: 100%; margin-top: 8px" onclick="updatePartition()">Update Partition Size</button>
						</div>
					</div>
				</div>
			`;
			
			document.body.appendChild(overlay);
		}
		
		async function enableEncryption() {
			const password = document.getElementById('settingsNewPass').value;
			if (!password) {
				toast('Password required', 'error');
				return;
			}
			
			try {
				await api('POST', '/api/config/encryption', { action: 'enable', password });
				toast('Encryption enabled', 'success');
				document.querySelector('.modal-overlay').remove();
				loadStats();
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		async function updateEncryption() {
			const oldPass = document.getElementById('settingsOldPass').value;
			const newPass = document.getElementById('settingsNewPass').value;
			
			if (!oldPass) {
				toast('Current password required', 'error');
				return;
			}
			
			try {
				if (newPass) {
					await api('POST', '/api/config/encryption', { 
						action: 'change_password', 
						old_password: oldPass, 
						new_password: newPass 
					});
					toast('Password changed', 'success');
				} else {
					await api('POST', '/api/config/encryption', { action: 'disable', password: oldPass });
					toast('Encryption disabled', 'success');
				}
				document.querySelector('.modal-overlay').remove();
				loadStats();
			} catch (e) {
				toast(e.message, 'error');
			}
		}
		
		async function updatePartition() {
			const size = parseInt(document.getElementById('settingsPartition').value);
			
			try {
				await api('POST', '/api/config/partition', { size });
				toast('Partition size updated', 'success');
				document.querySelector('.modal-overlay').remove();
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
		
		// Stats
		async function loadStats() {
			try {
				const config = await api('GET', '/api/config');
				document.getElementById('statNodes').textContent = config.stats.nodes;
				document.getElementById('statBlobs').textContent = config.stats.blobs;
				document.getElementById('statSize').textContent = formatSize(config.stats.total_size);
				document.getElementById('statEncrypted').textContent = config.encrypted ? 'Yes' : 'No';
			} catch (e) {
				console.error('Failed to load stats:', e);
			}
		}
		
		// Initialize
		(async () => {
			await loadStats();
			await refreshTree();
		})();
		
		// Keyboard shortcuts
		document.addEventListener('keydown', (e) => {
			if (e.key === 'Escape') {
				closeLightbox();
				document.querySelector('.modal-overlay')?.remove();
			}
		});
	</script>
</body>
</html>'''