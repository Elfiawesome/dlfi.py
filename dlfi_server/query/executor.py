"""
DLFI Query Executor

Executes parsed query AST against the database.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass

from .parser import (
	AndGroup, OrGroup, Term, ASTNode,
	TermType, Operator, Modifier
)

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
	"""Result of a query execution."""
	nodes: List[Dict[str, Any]]
	total_count: int
	limit: int
	offset: int
	query_time_ms: float


class QueryExecutor:
	"""Executes parsed queries against the DLFI database."""
	
	DEFAULT_LIMIT = 100
	MAX_LIMIT = 1000
	
	def __init__(self, dlfi_instance):
		self.dlfi = dlfi_instance
		self.conn = dlfi_instance.conn
	
	def execute(self, ast: AndGroup, offset: int = 0) -> QueryResult:
		"""Execute a parsed query and return results."""
		import time
		start_time = time.time()
		
		# Extract global modifiers (limit, sort)
		limit = self.DEFAULT_LIMIT
		sort_key = 'cached_path'
		sort_dir = 'ASC'
		
		for term in ast.terms:
			if isinstance(term, Term):
				if term.type == TermType.LIMIT:
					limit = min(int(term.value or self.DEFAULT_LIMIT), self.MAX_LIMIT)
				elif term.type == TermType.SORT:
					sort_key, sort_dir = self._parse_sort(term.value)
		
		# Build and execute the query
		where_clause, params = self._build_where(ast)
		
		# Count total results
		count_sql = f"""
			SELECT COUNT(DISTINCT n.uuid)
			FROM nodes n
			LEFT JOIN tags t ON n.uuid = t.node_uuid
			LEFT JOIN node_files nf ON n.uuid = nf.node_uuid
			LEFT JOIN blobs b ON nf.file_hash = b.hash
			LEFT JOIN edges e ON n.uuid = e.source_uuid OR n.uuid = e.target_uuid
			{where_clause}
		"""
		
		cursor = self.conn.execute(count_sql, params)
		total_count = cursor.fetchone()[0]
		
		# Fetch results
		select_sql = f"""
			SELECT DISTINCT
				n.uuid, n.type, n.name, n.cached_path, n.metadata,
				n.parent_uuid, n.created_at, n.last_modified
			FROM nodes n
			LEFT JOIN tags t ON n.uuid = t.node_uuid
			LEFT JOIN node_files nf ON n.uuid = nf.node_uuid
			LEFT JOIN blobs b ON nf.file_hash = b.hash
			LEFT JOIN edges e ON n.uuid = e.source_uuid OR n.uuid = e.target_uuid
			{where_clause}
			ORDER BY n.{sort_key} {sort_dir}
			LIMIT ? OFFSET ?
		"""
		
		cursor = self.conn.execute(select_sql, params + [limit, offset])
		
		nodes = []
		for row in cursor:
			uuid, node_type, name, path, metadata, parent, created, modified = row
			
			# Get additional data
			node_data = self._enrich_node(uuid, node_type, name, path, metadata, parent, created, modified)
			nodes.append(node_data)
		
		query_time = (time.time() - start_time) * 1000
		
		return QueryResult(
			nodes=nodes,
			total_count=total_count,
			limit=limit,
			offset=offset,
			query_time_ms=round(query_time, 2)
		)
	
	def _enrich_node(self, uuid: str, node_type: str, name: str, path: str,
					metadata: str, parent: str, created: float, modified: float) -> Dict:
		"""Add tags, file count, etc. to node data."""
		# Get tags
		cursor = self.conn.execute("SELECT tag FROM tags WHERE node_uuid = ?", (uuid,))
		tags = [r[0] for r in cursor]
		
		# Get file count and total size
		cursor = self.conn.execute("""
			SELECT COUNT(*), COALESCE(SUM(b.size_bytes), 0)
			FROM node_files nf
			JOIN blobs b ON nf.file_hash = b.hash
			WHERE nf.node_uuid = ?
		""", (uuid,))
		file_count, total_size = cursor.fetchone()
		
		# Get child count for vaults
		child_count = 0
		if node_type == 'VAULT':
			cursor = self.conn.execute(
				"SELECT COUNT(*) FROM nodes WHERE parent_uuid = ?", (uuid,)
			)
			child_count = cursor.fetchone()[0]
		
		return {
			"uuid": uuid,
			"type": node_type,
			"name": name,
			"path": path,
			"parent": parent,
			"metadata": json.loads(metadata) if metadata else {},
			"tags": tags,
			"file_count": file_count,
			"total_size": total_size,
			"child_count": child_count,
			"created_at": created,
			"last_modified": modified
		}
	
	def _build_where(self, ast: AndGroup) -> Tuple[str, List[Any]]:
		"""Build WHERE clause from AST."""
		conditions = []
		params = []
		
		for node in ast.terms:
			cond, prms = self._build_condition(node)
			if cond:
				conditions.append(cond)
				params.extend(prms)
		
		if not conditions:
			return "", []
		
		return "WHERE " + " AND ".join(conditions), params
	
	def _build_condition(self, node: ASTNode) -> Tuple[str, List[Any]]:
		"""Build a SQL condition from an AST node."""
		if isinstance(node, Term):
			return self._build_term_condition(node)
		
		if isinstance(node, OrGroup):
			parts = []
			params = []
			for term in node.terms:
				cond, prms = self._build_condition(term)
				if cond:
					parts.append(cond)
					params.extend(prms)
			if not parts:
				return "", []
			return "(" + " OR ".join(parts) + ")", params
		
		if isinstance(node, AndGroup):
			parts = []
			params = []
			for term in node.terms:
				cond, prms = self._build_condition(term)
				if cond:
					parts.append(cond)
					params.extend(prms)
			if not parts:
				return "", []
			return "(" + " AND ".join(parts) + ")", params
		
		return "", []
	
	def _build_term_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build SQL condition for a single term."""
		# Skip global modifiers
		if term.type in (TermType.LIMIT, TermType.SORT):
			return "", []
		
		condition = ""
		params = []
		
		if term.type == TermType.GLOBAL_SEARCH:
			condition, params = self._build_global_search(term)
		
		elif term.type == TermType.TAG:
			condition, params = self._build_tag_condition(term)
		
		elif term.type == TermType.METADATA:
			condition, params = self._build_metadata_condition(term)
		
		elif term.type == TermType.META_EXISTS:
			condition, params = self._build_meta_exists_condition(term)
		
		elif term.type == TermType.INSIDE:
			condition, params = self._build_inside_condition(term)
		
		elif term.type == TermType.PATH_PATTERN:
			condition, params = self._build_path_pattern_condition(term)
		
		elif term.type == TermType.RELATION:
			condition, params = self._build_relation_condition(term)
		
		elif term.type == TermType.RELATION_TYPE:
			condition, params = self._build_relation_type_condition(term)
		
		elif term.type == TermType.EXTENSION:
			condition, params = self._build_extension_condition(term)
		
		elif term.type == TermType.FILE_COUNT:
			condition, params = self._build_file_count_condition(term)
		
		elif term.type == TermType.SIZE:
			condition, params = self._build_size_condition(term)
		
		elif term.type == TermType.TYPE:
			condition, params = self._build_type_condition(term)
		
		elif term.type == TermType.PREVIEW:
			condition, params = self._build_preview_condition(term)
		
		# Apply negation
		if term.modifier.negated and condition:
			condition = f"NOT ({condition})"
		
		# Apply deep search (include descendants)
		if term.modifier.deep and condition:
			condition = self._wrap_deep_search(condition, params)
		
		# Apply reverse deep (include ancestors)
		if term.modifier.reverse_deep and condition:
			condition = self._wrap_reverse_deep(condition, params)
		
		return condition, params
	
	def _build_global_search(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build global search across name, path, tags, metadata."""
		value = str(term.value)
		
		if term.operator == Operator.EQUALS:
			# Exact phrase match
			return """(
				n.name = ? OR
				n.cached_path = ? OR
				t.tag = ? OR
				n.metadata LIKE ?
			)""", [value, value, value, f'%"{value}"%']
		else:
			# Contains match
			pattern = f"%{value}%"
			return """(
				n.name LIKE ? OR
				n.cached_path LIKE ? OR
				t.tag LIKE ? OR
				n.metadata LIKE ?
			)""", [pattern, pattern, pattern, pattern]
	
	def _build_tag_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build tag search condition."""
		if term.operator == Operator.EXISTS:
			return "t.tag IS NOT NULL", []
		
		value = str(term.value)
		
		if term.operator == Operator.EQUALS:
			return "t.tag = ?", [value.lower()]
		else:
			return "t.tag LIKE ?", [f"%{value.lower()}%"]
	
	def _build_metadata_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build metadata field search condition."""
		key = term.key
		value = term.value
		
		json_path = f"$.{key}"
		
		if term.operator == Operator.EQUALS:
			return f"json_extract(n.metadata, ?) = ?", [json_path, value]
		
		elif term.operator == Operator.CONTAINS:
			return f"CAST(json_extract(n.metadata, ?) AS TEXT) LIKE ?", [json_path, f"%{value}%"]
		
		elif term.operator == Operator.GT:
			return f"CAST(json_extract(n.metadata, ?) AS REAL) > ?", [json_path, value]
		
		elif term.operator == Operator.LT:
			return f"CAST(json_extract(n.metadata, ?) AS REAL) < ?", [json_path, value]
		
		elif term.operator == Operator.GTE:
			return f"CAST(json_extract(n.metadata, ?) AS REAL) >= ?", [json_path, value]
		
		elif term.operator == Operator.LTE:
			return f"CAST(json_extract(n.metadata, ?) AS REAL) <= ?", [json_path, value]
		
		elif term.operator == Operator.RANGE:
			return f"""(
				CAST(json_extract(n.metadata, ?) AS REAL) >= ? AND
				CAST(json_extract(n.metadata, ?) AS REAL) <= ?
			)""", [json_path, value, json_path, term.value_end]
		
		return "", []
	
	def _build_meta_exists_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build metadata existence check condition."""
		key = term.key
		json_path = f"$.{key}"
		
		if term.operator == Operator.NOT_EXISTS:
			return f"json_extract(n.metadata, ?) IS NULL", [json_path]
		else:
			return f"json_extract(n.metadata, ?) IS NOT NULL", [json_path]
	
	def _build_inside_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build path containment condition."""
		path = str(term.value).strip("/")
		return "n.cached_path LIKE ?", [f"{path}/%"]
	
	def _build_path_pattern_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build path pattern matching condition."""
		pattern = str(term.value)
		
		# Convert wildcards to SQL LIKE patterns
		# * matches single level, ** matches any depth
		sql_pattern = pattern.replace("**", "\x00").replace("*", "%").replace("\x00", "%")
		
		if not sql_pattern.startswith("%"):
			sql_pattern = sql_pattern
		
		return "n.cached_path LIKE ?", [sql_pattern]
	
	def _build_relation_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build relationship query condition."""
		target_path = term.key
		relation = term.value
		direction = term.modifier.direction
		
		# Get target UUID
		cursor = self.conn.execute(
			"SELECT uuid FROM nodes WHERE cached_path = ?", (target_path,)
		)
		row = cursor.fetchone()
		if not row:
			return "1=0", []  # No match
		
		target_uuid = row[0]
		
		conditions = []
		params = []
		
		if direction == '>':
			# Outgoing only - this node points to target
			conditions.append("e.source_uuid = n.uuid AND e.target_uuid = ?")
			params.append(target_uuid)
		elif direction == '<':
			# Incoming only - target points to this node
			conditions.append("e.target_uuid = n.uuid AND e.source_uuid = ?")
			params.append(target_uuid)
		else:
			# Either direction
			conditions.append("(e.target_uuid = ? OR e.source_uuid = ?)")
			params.extend([target_uuid, target_uuid])
		
		if relation:
			conditions.append("e.relation = ?")
			params.append(relation.upper())
		
		return " AND ".join(conditions), params
	
	def _build_relation_type_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build condition for finding nodes with a specific relation type."""
		relation = str(term.value).upper()
		return "e.relation = ?", [relation]
	
	def _build_extension_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build file extension condition."""
		ext = str(term.value).lower().lstrip('.')
		
		if term.operator == Operator.EQUALS:
			return "b.ext = ?", [ext]
		else:
			return "b.ext LIKE ?", [f"%{ext}%"]
	
	def _build_file_count_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build file count condition."""
		count = int(term.value or 0)
		
		subquery = "(SELECT COUNT(*) FROM node_files WHERE node_uuid = n.uuid)"
		
		if term.operator == Operator.GT:
			return f"{subquery} > ?", [count]
		elif term.operator == Operator.LT:
			return f"{subquery} < ?", [count]
		elif term.operator == Operator.GTE:
			return f"{subquery} >= ?", [count]
		elif term.operator == Operator.LTE:
			return f"{subquery} <= ?", [count]
		elif term.operator == Operator.EQUALS:
			return f"{subquery} = ?", [count]
		
		return "", []
	
	def _build_size_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build file size condition."""
		size = int(term.value or 0)
		
		subquery = "(SELECT COALESCE(SUM(b2.size_bytes), 0) FROM node_files nf2 JOIN blobs b2 ON nf2.file_hash = b2.hash WHERE nf2.node_uuid = n.uuid)"
		
		if term.operator == Operator.GT:
			return f"{subquery} > ?", [size]
		elif term.operator == Operator.LT:
			return f"{subquery} < ?", [size]
		elif term.operator == Operator.GTE:
			return f"{subquery} >= ?", [size]
		elif term.operator == Operator.LTE:
			return f"{subquery} <= ?", [size]
		elif term.operator == Operator.RANGE and term.value_end:
			return f"({subquery} >= ? AND {subquery} <= ?)", [size, int(term.value_end)]
		
		return "", []
	
	def _build_type_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build node type condition."""
		node_type = str(term.value).upper()
		if node_type in ('VAULT', 'RECORD'):
			return "n.type = ?", [node_type]
		return "", []
	
	def _build_preview_condition(self, term: Term) -> Tuple[str, List[Any]]:
		"""Build preview availability condition."""
		value = str(term.value).lower()
		
		preview_exts = ('jpg', 'jpeg', 'png', 'gif', 'webp', 'mp4', 'webm', 'mov')
		placeholders = ','.join('?' * len(preview_exts))
		
		if value in ('true', '1', 'yes'):
			return f"b.ext IN ({placeholders})", list(preview_exts)
		else:
			return f"(b.ext IS NULL OR b.ext NOT IN ({placeholders}))", list(preview_exts)
	
	def _wrap_deep_search(self, condition: str, params: List[Any]) -> str:
		"""Wrap condition to include all descendants."""
		# Returns nodes matching condition OR whose ancestors match
		return f"""(
			{condition} OR
			n.uuid IN (
				SELECT c.uuid FROM nodes c
				WHERE EXISTS (
					SELECT 1 FROM nodes p
					WHERE c.cached_path LIKE p.cached_path || '/%'
					AND ({condition.replace('n.', 'p.')})
				)
			)
		)"""
	
	def _wrap_reverse_deep(self, condition: str, params: List[Any]) -> str:
		"""Wrap condition to include all ancestors."""
		# Returns nodes matching condition OR whose descendants match
		return f"""(
			{condition} OR
			n.uuid IN (
				SELECT p.uuid FROM nodes p
				WHERE EXISTS (
					SELECT 1 FROM nodes c
					WHERE c.cached_path LIKE p.cached_path || '/%'
					AND ({condition.replace('n.', 'c.')})
				)
			)
		)"""
	
	def _parse_sort(self, sort_value: str) -> Tuple[str, str]:
		"""Parse sort value into column and direction."""
		valid_columns = {
			'name': 'name',
			'path': 'cached_path',
			'type': 'type',
			'created': 'created_at',
			'created_at': 'created_at',
			'modified': 'last_modified',
			'last_modified': 'last_modified',
		}
		
		sort_dir = 'ASC'
		sort_key = str(sort_value or 'path').lower()
		
		if sort_key.startswith('-'):
			sort_dir = 'DESC'
			sort_key = sort_key[1:]
		elif sort_key.endswith('-'):
			sort_dir = 'DESC'
			sort_key = sort_key[:-1]
		
		column = valid_columns.get(sort_key, 'cached_path')
		return column, sort_dir