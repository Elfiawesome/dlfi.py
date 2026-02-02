import json
import logging
import time
from pathlib import Path
from typing import Optional
from flask import Blueprint, request, jsonify, current_app, Response
from io import BytesIO
from dlfi_server.query import QueryParser, QueryExecutor, AutocompleteProvider, ParseError

logger = logging.getLogger(__name__)

api_bp = Blueprint("api", __name__)


def get_dlfi():
	"""Get the current DLFI instance."""
	return current_app.config.get("DLFI_INSTANCE")


def require_vault(f):
	"""Decorator to require an open vault."""
	from functools import wraps
	@wraps(f)
	def decorated(*args, **kwargs):
		if get_dlfi() is None:
			return jsonify({"error": "No vault open"}), 400
		return f(*args, **kwargs)
	return decorated


# ============ Vault Management ============

@api_bp.route("/vault/open", methods=["POST"])
def open_vault():
	"""Open an existing vault from any path."""
	from dlfi import DLFI
	
	data = request.get_json() or {}
	vault_path = data.get("path", "").strip()
	password = data.get("password") or None  # Convert empty string to None
	
	if not vault_path:
		return jsonify({"error": "Vault path required"}), 400
	
	# Resolve and normalize path
	try:
		vault_path = Path(vault_path).resolve()
	except Exception as e:
		return jsonify({"error": f"Invalid path: {e}"}), 400
	
	logger.info(f"Attempting to open vault at: {vault_path}")
	
	if not vault_path.exists():
		return jsonify({"error": f"Path does not exist: {vault_path}"}), 404
	
	if not vault_path.is_dir():
		return jsonify({"error": "Path is not a directory"}), 400
	
	if not (vault_path / ".dlfi").exists():
		return jsonify({"error": "Not a valid DLFI vault (no .dlfi folder found)"}), 400
	
	# Close existing vault if open
	existing = current_app.config.get("DLFI_INSTANCE")
	if existing:
		try:
			existing.close()
		except:
			pass
		current_app.config["DLFI_INSTANCE"] = None
	
	try:
		dlfi = DLFI(str(vault_path), password=password)
		current_app.config["DLFI_INSTANCE"] = dlfi
		current_app.config["DLFI_PASSWORD"] = password
		
		# Add to recent vaults
		config = current_app.config["DLFI_CONFIG"]
		config.add_recent_vault(str(vault_path))
		
		logger.info(f"Successfully opened vault: {vault_path}")
		
		return jsonify({
			"success": True,
			"name": vault_path.name,
			"path": str(vault_path),
			"encrypted": dlfi.config.encrypted
		})
	except ValueError as e:
		logger.warning(f"Failed to open vault (auth error): {e}")
		return jsonify({"error": str(e)}), 401
	except Exception as e:
		logger.exception("Failed to open vault")
		return jsonify({"error": f"Failed to open vault: {str(e)}"}), 500


@api_bp.route("/vault/create", methods=["POST"])
def create_vault():
	"""Create a new vault at any path."""
	from dlfi import DLFI
	
	data = request.get_json() or {}
	vault_path = data.get("path", "").strip()
	vault_name = data.get("name", "").strip()
	password = data.get("password") or None  # Convert empty string to None
	use_default_dir = data.get("use_default_dir", True)
	
	config = current_app.config["DLFI_CONFIG"]
	
	# Determine the full path
	if vault_path:
		# Full path provided
		try:
			full_path = Path(vault_path).resolve()
		except Exception as e:
			return jsonify({"error": f"Invalid path: {e}"}), 400
	elif vault_name:
		if use_default_dir:
			# Sanitize name - allow alphanumeric, dots, underscores, hyphens, spaces
			safe_name = "".join(c for c in vault_name if c.isalnum() or c in "._- ")
			safe_name = safe_name.strip()
			if not safe_name:
				return jsonify({"error": "Invalid vault name"}), 400
			full_path = config.default_vaults_dir / safe_name
		else:
			return jsonify({"error": "Either path or name with use_default_dir must be provided"}), 400
	else:
		return jsonify({"error": "Vault path or name required"}), 400
	
	logger.info(f"Attempting to create vault at: {full_path}")
	
	# Check if already exists as a vault
	if full_path.exists() and (full_path / ".dlfi").exists():
		return jsonify({"error": "Vault already exists at this location"}), 409
	
	# Close existing vault if open
	existing = current_app.config.get("DLFI_INSTANCE")
	if existing:
		try:
			existing.close()
		except:
			pass
		current_app.config["DLFI_INSTANCE"] = None
	
	try:
		# Create parent directories if needed
		full_path.mkdir(parents=True, exist_ok=True)
		
		dlfi = DLFI(str(full_path), password=password)
		current_app.config["DLFI_INSTANCE"] = dlfi
		current_app.config["DLFI_PASSWORD"] = password
		
		# Add to recent vaults
		config.add_recent_vault(str(full_path))
		
		logger.info(f"Successfully created vault: {full_path}")
		
		return jsonify({
			"success": True,
			"name": full_path.name,
			"path": str(full_path),
			"encrypted": dlfi.config.encrypted
		})
	except Exception as e:
		logger.exception("Failed to create vault")
		return jsonify({"error": f"Failed to create vault: {str(e)}"}), 500


@api_bp.route("/vault/info", methods=["GET"])
@require_vault
def vault_info():
	"""Get current vault information."""
	dlfi = get_dlfi()
	
	# Count nodes
	cursor = dlfi.conn.execute("SELECT type, COUNT(*) FROM nodes GROUP BY type")
	counts = dict(cursor.fetchall())
	
	# Count blobs
	cursor = dlfi.conn.execute("SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM blobs")
	row = cursor.fetchone()
	blob_count = row[0] or 0
	total_size = row[1] or 0
	
	return jsonify({
		"name": Path(dlfi.root).name,
		"path": str(dlfi.root),
		"encrypted": dlfi.config.encrypted,
		"partition_size": dlfi.config.partition_size,
		"vault_count": counts.get("VAULT", 0),
		"record_count": counts.get("RECORD", 0),
		"blob_count": blob_count,
		"total_size": total_size
	})


@api_bp.route("/vault/browse", methods=["POST"])
def browse_path():
	"""Browse filesystem to find vaults or select a directory."""
	data = request.get_json() or {}
	path = data.get("path", "")
	
	if not path:
		# Return drives on Windows, root on Unix
		import platform
		if platform.system() == "Windows":
			import string
			drives = []
			for letter in string.ascii_uppercase:
				drive = f"{letter}:\\"
				if Path(drive).exists():
					drives.append({
						"name": drive,
						"path": drive,
						"is_dir": True,
						"is_vault": False
					})
			return jsonify({"items": drives, "current": ""})
		else:
			path = "/"
	
	current = Path(path).resolve()
	
	if not current.exists():
		return jsonify({"error": "Path does not exist"}), 404
	
	if not current.is_dir():
		current = current.parent
	
	items = []
	
	# Add parent directory option
	if current.parent != current:
		items.append({
			"name": "..",
			"path": str(current.parent),
			"is_dir": True,
			"is_vault": False
		})
	
	try:
		for item in sorted(current.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
			if item.name.startswith('.') and item.name != ".dlfi":
				continue  # Skip hidden files except .dlfi
			
			is_vault = item.is_dir() and (item / ".dlfi").exists()
			
			items.append({
				"name": item.name,
				"path": str(item),
				"is_dir": item.is_dir(),
				"is_vault": is_vault
			})
	except PermissionError:
		return jsonify({"error": "Permission denied"}), 403
	
	return jsonify({
		"items": items,
		"current": str(current),
		"is_vault": (current / ".dlfi").exists()
	})


# ============ Node Operations ============

@api_bp.route("/nodes", methods=["GET"])
@require_vault
def list_nodes():
	"""List all nodes in the vault."""
	dlfi = get_dlfi()
	
	cursor = dlfi.conn.execute("""
		SELECT uuid, type, name, cached_path, metadata, parent_uuid, created_at
		FROM nodes
		ORDER BY cached_path
	""")
	
	nodes = []
	for row in cursor:
		uuid, node_type, name, path, metadata, parent, created = row
		
		# Get tags
		tags_cursor = dlfi.conn.execute("SELECT tag FROM tags WHERE node_uuid = ?", (uuid,))
		tags = [t[0] for t in tags_cursor]
		
		# Get file count
		files_cursor = dlfi.conn.execute("SELECT COUNT(*) FROM node_files WHERE node_uuid = ?", (uuid,))
		file_count = files_cursor.fetchone()[0]
		
		nodes.append({
			"uuid": uuid,
			"type": node_type,
			"name": name,
			"path": path,
			"parent": parent,
			"metadata": json.loads(metadata) if metadata else {},
			"tags": tags,
			"file_count": file_count,
			"created_at": created
		})
	
	return jsonify({"nodes": nodes})


@api_bp.route("/nodes/<uuid>", methods=["GET"])
@require_vault
def get_node(uuid: str):
	"""Get detailed node information."""
	dlfi = get_dlfi()
	
	cursor = dlfi.conn.execute("""
		SELECT uuid, type, name, cached_path, metadata, parent_uuid, created_at, last_modified
		FROM nodes WHERE uuid = ?
	""", (uuid,))
	
	row = cursor.fetchone()
	if not row:
		return jsonify({"error": "Node not found"}), 404
	
	node_uuid, node_type, name, path, metadata, parent, created, modified = row
	
	# Get tags
	tags_cursor = dlfi.conn.execute("SELECT tag FROM tags WHERE node_uuid = ?", (uuid,))
	tags = [t[0] for t in tags_cursor]
	
	# Get files
	files_cursor = dlfi.conn.execute("""
		SELECT nf.original_name, nf.file_hash, b.size_bytes, b.ext, nf.display_order
		FROM node_files nf
		JOIN blobs b ON nf.file_hash = b.hash
		WHERE nf.node_uuid = ?
		ORDER BY nf.display_order
	""", (uuid,))
	
	files = []
	for fname, fhash, fsize, fext, forder in files_cursor:
		files.append({
			"name": fname,
			"hash": fhash,
			"size": fsize,
			"ext": fext,
			"order": forder
		})
	
	# Get relationships
	rels_cursor = dlfi.conn.execute("""
		SELECT e.relation, e.target_uuid, n.cached_path, n.name
		FROM edges e
		JOIN nodes n ON e.target_uuid = n.uuid
		WHERE e.source_uuid = ?
	""", (uuid,))
	
	relationships = []
	for rel, target_uuid, target_path, target_name in rels_cursor:
		relationships.append({
			"relation": rel,
			"target_uuid": target_uuid,
			"target_path": target_path,
			"target_name": target_name
		})
	
	# Get children
	children_cursor = dlfi.conn.execute("""
		SELECT uuid, type, name, cached_path
		FROM nodes WHERE parent_uuid = ?
		ORDER BY type DESC, name
	""", (uuid,))
	
	children = []
	for c_uuid, c_type, c_name, c_path in children_cursor:
		children.append({
			"uuid": c_uuid,
			"type": c_type,
			"name": c_name,
			"path": c_path
		})
	
	return jsonify({
		"uuid": node_uuid,
		"type": node_type,
		"name": name,
		"path": path,
		"parent": parent,
		"metadata": json.loads(metadata) if metadata else {},
		"tags": tags,
		"files": files,
		"relationships": relationships,
		"children": children,
		"created_at": created,
		"last_modified": modified
	})


@api_bp.route("/nodes", methods=["POST"])
@require_vault
def create_node():
	"""Create a new vault or record."""
	dlfi = get_dlfi()
	data = request.get_json() or {}
	
	path = data.get("path")
	node_type = data.get("type", "RECORD")
	metadata = data.get("metadata", {})
	tags = data.get("tags", [])
	
	if not path:
		return jsonify({"error": "Path required"}), 400
	
	if node_type not in ("VAULT", "RECORD"):
		return jsonify({"error": "Type must be VAULT or RECORD"}), 400
	
	try:
		if node_type == "VAULT":
			uuid = dlfi.create_vault(path, metadata=metadata)
		else:
			uuid = dlfi.create_record(path, metadata=metadata)
		
		for tag in tags:
			dlfi.add_tag(path, tag)
		
		return jsonify({"success": True, "uuid": uuid, "path": path})
	except Exception as e:
		return jsonify({"error": str(e)}), 500


@api_bp.route("/nodes/<uuid>", methods=["PUT"])
@require_vault
def update_node(uuid: str):
	"""Update node metadata and tags."""
	dlfi = get_dlfi()
	data = request.get_json() or {}
	
	cursor = dlfi.conn.execute("SELECT cached_path FROM nodes WHERE uuid = ?", (uuid,))
	row = cursor.fetchone()
	if not row:
		return jsonify({"error": "Node not found"}), 404
	
	try:
		with dlfi.conn:
			# Update metadata
			if "metadata" in data:
				dlfi.conn.execute(
					"UPDATE nodes SET metadata = ?, last_modified = ? WHERE uuid = ?",
					(json.dumps(data["metadata"]), time.time(), uuid)
				)
			
			# Update tags
			if "tags" in data:
				dlfi.conn.execute("DELETE FROM tags WHERE node_uuid = ?", (uuid,))
				for tag in data["tags"]:
					dlfi.conn.execute(
						"INSERT OR IGNORE INTO tags (node_uuid, tag) VALUES (?, ?)",
						(uuid, tag.lower())
					)
		
		return jsonify({"success": True})
	except Exception as e:
		return jsonify({"error": str(e)}), 500


@api_bp.route("/nodes/<uuid>", methods=["DELETE"])
@require_vault
def delete_node(uuid: str):
	"""Delete a node and its children."""
	dlfi = get_dlfi()
	
	cursor = dlfi.conn.execute("SELECT cached_path FROM nodes WHERE uuid = ?", (uuid,))
	row = cursor.fetchone()
	if not row:
		return jsonify({"error": "Node not found"}), 404
	
	try:
		with dlfi.conn:
			# CASCADE will handle children, node_files, tags, edges
			dlfi.conn.execute("DELETE FROM nodes WHERE uuid = ?", (uuid,))
		
		return jsonify({"success": True})
	except Exception as e:
		return jsonify({"error": str(e)}), 500


# ============ File Operations ============

@api_bp.route("/nodes/<uuid>/files", methods=["POST"])
@require_vault
def upload_file(uuid: str):
	"""Upload a file to a record."""
	dlfi = get_dlfi()
	
	cursor = dlfi.conn.execute("SELECT cached_path, type FROM nodes WHERE uuid = ?", (uuid,))
	row = cursor.fetchone()
	if not row:
		return jsonify({"error": "Node not found"}), 404
	
	path, node_type = row
	if node_type != "RECORD":
		return jsonify({"error": "Can only add files to records"}), 400
	
	if "file" not in request.files:
		return jsonify({"error": "No file provided"}), 400
	
	file = request.files["file"]
	if not file.filename:
		return jsonify({"error": "No filename"}), 400
	
	try:
		dlfi.append_stream(path, file.stream, file.filename)
		return jsonify({"success": True})
	except Exception as e:
		return jsonify({"error": str(e)}), 500


@api_bp.route("/blobs/<file_hash>", methods=["GET"])
@require_vault
def get_blob(file_hash: str):
	"""Download a blob by hash."""
	dlfi = get_dlfi()
	
	# Get blob info
	cursor = dlfi.conn.execute(
		"SELECT ext FROM blobs WHERE hash = ?", (file_hash,)
	)
	row = cursor.fetchone()
	if not row:
		return jsonify({"error": "Blob not found"}), 404
	
	ext = row[0]
	
	try:
		data = dlfi.read_blob(file_hash)
		if data is None:
			return jsonify({"error": "Blob data not found"}), 404
		
		# Determine MIME type
		mime_types = {
			"jpg": "image/jpeg",
			"jpeg": "image/jpeg",
			"png": "image/png",
			"gif": "image/gif",
			"webp": "image/webp",
			"mp4": "video/mp4",
			"webm": "video/webm",
			"mov": "video/quicktime",
			"pdf": "application/pdf",
			"txt": "text/plain",
		}
		mime = mime_types.get(ext, "application/octet-stream")
		
		return Response(
			data,
			mimetype=mime,
			headers={
				"Content-Disposition": f"inline; filename={file_hash}.{ext}" if ext else f"inline; filename={file_hash}",
				"Cache-Control": "max-age=31536000"
			}
		)
	except Exception as e:
		logger.exception("Failed to read blob")
		return jsonify({"error": str(e)}), 500


@api_bp.route("/blobs/<file_hash>/thumbnail", methods=["GET"])
@require_vault
def get_blob_thumbnail(file_hash: str):
	"""Get a thumbnail for an image blob."""
	dlfi = get_dlfi()
	
	cursor = dlfi.conn.execute(
		"SELECT ext FROM blobs WHERE hash = ?", (file_hash,)
	)
	row = cursor.fetchone()
	if not row:
		return jsonify({"error": "Blob not found"}), 404
	
	ext = row[0]
	if ext not in ("jpg", "jpeg", "png", "gif", "webp"):
		return jsonify({"error": "Not an image"}), 400
	
	try:
		data = dlfi.read_blob(file_hash)
		if data is None:
			return jsonify({"error": "Blob data not found"}), 404
		
		# Try to create thumbnail with PIL if available
		try:
			from PIL import Image
			img = Image.open(BytesIO(data))
			img.thumbnail((200, 200))
			output = BytesIO()
			img.save(output, format="JPEG", quality=80)
			output.seek(0)
			return Response(output, mimetype="image/jpeg")
		except ImportError:
			# PIL not available, return original
			mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
			return Response(data, mimetype=mime)
	except Exception as e:
		return jsonify({"error": str(e)}), 500


# ============ Relationships ============

@api_bp.route("/nodes/<uuid>/relationships", methods=["POST"])
@require_vault
def add_relationship(uuid: str):
	"""Add a relationship from this node to another."""
	dlfi = get_dlfi()
	data = request.get_json() or {}
	
	target_path = data.get("target_path")
	relation = data.get("relation")
	
	if not target_path or not relation:
		return jsonify({"error": "target_path and relation required"}), 400
	
	cursor = dlfi.conn.execute("SELECT cached_path FROM nodes WHERE uuid = ?", (uuid,))
	row = cursor.fetchone()
	if not row:
		return jsonify({"error": "Source node not found"}), 404
	
	source_path = row[0]
	
	try:
		dlfi.link(source_path, target_path, relation)
		return jsonify({"success": True})
	except ValueError as e:
		return jsonify({"error": str(e)}), 400


# ============ Tags ============

@api_bp.route("/tags", methods=["GET"])
@require_vault
def list_all_tags():
	"""List all unique tags in the vault."""
	dlfi = get_dlfi()
	
	cursor = dlfi.conn.execute("SELECT DISTINCT tag FROM tags ORDER BY tag")
	tags = [row[0] for row in cursor]
	
	return jsonify({"tags": tags})


# ============ Search ============

@api_bp.route("/search", methods=["GET"])
@require_vault
def search_nodes():
	"""Search nodes by various criteria."""
	dlfi = get_dlfi()
	
	query = dlfi.query()
	
	# Apply filters from query params
	if request.args.get("inside"):
		query.inside(request.args.get("inside"))
	
	if request.args.get("type"):
		query.type(request.args.get("type"))
	
	if request.args.get("tag"):
		query.has_tag(request.args.get("tag"))
	
	results = query.execute()
	
	return jsonify({"results": results})


# ============ Export ============

@api_bp.route("/export", methods=["POST"])
@require_vault
def export_static():
	"""Generate static site export."""
	dlfi = get_dlfi()
	
	try:
		dlfi.generate_static_site()
		return jsonify({"success": True, "message": "Static site generated in vault root"})
	except Exception as e:
		return jsonify({"error": str(e)}), 500

# ============ Query System ============

@api_bp.route("/query", methods=["POST"])
@require_vault
def execute_query():
    """Execute a query and return matching nodes."""
    dlfi = get_dlfi()
    data = request.get_json() or {}
    
    query_str = data.get("query", "")
    offset = int(data.get("offset", 0))
    
    if not query_str.strip():
        # Empty query - return all nodes
        query_str = "type:VAULT | type:RECORD"
    
    try:
        parser = QueryParser(query_str)
        ast = parser.parse()
        
        executor = QueryExecutor(dlfi)
        result = executor.execute(ast, offset=offset)
        
        return jsonify({
            "success": True,
            "nodes": result.nodes,
            "total": result.total_count,
            "limit": result.limit,
            "offset": result.offset,
            "query_time_ms": result.query_time_ms
        })
    except ParseError as e:
        return jsonify({
            "error": f"Query parse error: {e.message}",
            "position": e.position
        }), 400
    except Exception as e:
        logger.exception("Query execution failed")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/autocomplete", methods=["GET"])
@require_vault
def get_autocomplete():
    """Get autocomplete suggestions for a query."""
    dlfi = get_dlfi()
    
    query = request.args.get("q", "")
    cursor_pos = request.args.get("cursor")
    
    if cursor_pos is not None:
        cursor_pos = int(cursor_pos)
    
    provider = AutocompleteProvider(dlfi)
    suggestions = provider.get_suggestions(query, cursor_pos)
    
    return jsonify({"suggestions": suggestions})


@api_bp.route("/query/help", methods=["GET"])
def get_query_help():
    """Return query language documentation."""
    return jsonify({
        "syntax": [
            {
                "category": "Basic Search",
                "items": [
                    {"syntax": "text", "description": "Global search across name, path, tags, and metadata"},
                    {"syntax": '"quoted text"', "description": "Search for exact phrase"},
                    {"syntax": "tag:value", "description": "Find nodes with tag containing value"},
                    {"syntax": "tag=value", "description": "Find nodes with exact tag match"},
                    {"syntax": "-tag:value", "description": "Exclude nodes with tag"},
                ]
            },
            {
                "category": "Metadata",
                "items": [
                    {"syntax": "key:value", "description": "Metadata field contains value"},
                    {"syntax": "key=value", "description": "Metadata field equals value"},
                    {"syntax": "key?", "description": "Field exists"},
                    {"syntax": "-key", "description": "Field does not exist"},
                    {"syntax": "key>N", "description": "Numeric comparison (>, <, >=, <=)"},
                    {"syntax": "key:start..end", "description": "Range search"},
                ]
            },
            {
                "category": "Structure",
                "items": [
                    {"syntax": "inside:path", "description": "Search within path"},
                    {"syntax": "path:pattern*", "description": "Wildcard path match"},
                    {"syntax": "^term", "description": "Include descendants of matches"},
                    {"syntax": "%term", "description": "Include ancestors of matches"},
                ]
            },
            {
                "category": "Relationships",
                "items": [
                    {"syntax": "!path", "description": "Nodes related to path"},
                    {"syntax": "!path:RELATION", "description": "Specific relation type"},
                    {"syntax": "!path:REL>", "description": "Outgoing relations only"},
                    {"syntax": "!path:REL<", "description": "Incoming relations only"},
                    {"syntax": "RELATION_NAME", "description": "Any node with this relation type"},
                ]
            },
            {
                "category": "Files",
                "items": [
                    {"syntax": "ext:mp4", "description": "Filter by extension"},
                    {"syntax": "files>3", "description": "File count comparison"},
                    {"syntax": "size>10mb", "description": "Total size comparison"},
                    {"syntax": "preview:true", "description": "Has visual preview"},
                ]
            },
            {
                "category": "Logic",
                "items": [
                    {"syntax": "a | b", "description": "OR: match either"},
                    {"syntax": "(a b) | c", "description": "Grouping"},
                    {"syntax": "type:VAULT", "description": "Filter by node type"},
                    {"syntax": "limit:50", "description": "Limit results"},
                    {"syntax": "sort:name", "description": "Sort by field (prefix - for desc)"},
                ]
            },
        ]
    })