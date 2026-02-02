"""
DLFI Query Autocomplete Provider

Provides intelligent autocomplete suggestions for the query language.
"""

import re
import json
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum, auto


class SuggestionType(Enum):
	KEYWORD = auto()        # Query keywords (tag, ext, type, etc.)
	TAG = auto()            # Tag values
	METADATA_KEY = auto()   # Metadata field names
	METADATA_VALUE = auto() # Metadata field values
	PATH = auto()           # Node paths
	RELATION = auto()       # Relation types
	EXTENSION = auto()      # File extensions
	NODE_TYPE = auto()      # VAULT or RECORD
	OPERATOR = auto()       # Operators (:, =, >, <)
	MODIFIER = auto()       # Modifiers (-, ^, %, !)


@dataclass
class Suggestion:
	"""A single autocomplete suggestion."""
	text: str
	display: str
	type: SuggestionType
	description: str = ""
	insert_text: str = ""  # What to actually insert (may differ from text)
	
	def to_dict(self) -> Dict[str, Any]:
		return {
			"text": self.text,
			"display": self.display,
			"type": self.type.name.lower(),
			"description": self.description,
			"insert_text": self.insert_text or self.text
		}


class AutocompleteProvider:
	"""Provides autocomplete suggestions for queries."""
	
	# Query language keywords with descriptions
	KEYWORDS = {
		'tag': ('tag:', 'Search by tag', 'tag:'),
		'inside': ('inside:', 'Search within a path', 'inside:'),
		'path': ('path:', 'Match path pattern', 'path:'),
		'ext': ('ext:', 'Filter by file extension', 'ext:'),
		'files': ('files', 'Filter by file count', 'files'),
		'size': ('size', 'Filter by total size', 'size'),
		'type': ('type:', 'Filter by node type', 'type:'),
		'limit': ('limit:', 'Limit number of results', 'limit:'),
		'sort': ('sort:', 'Sort results', 'sort:'),
		'preview': ('preview:', 'Filter by preview availability', 'preview:'),
	}
	
	# Operators
	OPERATORS = [
		(':', 'contains (partial match)'),
		('=', 'equals (exact match)'),
		('>', 'greater than'),
		('<', 'less than'),
		('>=', 'greater than or equal'),
		('<=', 'less than or equal'),
		('..', 'range (e.g., 2020..2024)'),
	]
	
	# Modifiers
	MODIFIERS = [
		('-', 'Negate/exclude'),
		('^', 'Deep search (include descendants)'),
		('%', 'Reverse deep (include ancestors)'),
		('!', 'Relationship query'),
	]
	
	# Sort options
	SORT_OPTIONS = ['name', 'path', 'created', 'modified', '-name', '-path', '-created', '-modified']
	
	def __init__(self, dlfi_instance):
		self.dlfi = dlfi_instance
		self.conn = dlfi_instance.conn
		self._cache = {}
		self._cache_valid = False
	
	def invalidate_cache(self):
		"""Invalidate the autocomplete cache."""
		self._cache = {}
		self._cache_valid = False
	
	def get_suggestions(self, query: str, cursor_pos: int = None) -> List[Dict]:
		"""
		Get autocomplete suggestions for the current query.
		
		:param query: The current query string
		:param cursor_pos: Position of the cursor (defaults to end)
		:return: List of suggestion dictionaries
		"""
		if cursor_pos is None:
			cursor_pos = len(query)
		
		# Get the text before cursor
		text_before = query[:cursor_pos]
		
		# Determine context
		context = self._analyze_context(text_before)
		
		suggestions = []
		
		if context['type'] == 'start':
			suggestions = self._suggest_start(context['prefix'])
		
		elif context['type'] == 'after_keyword':
			suggestions = self._suggest_for_keyword(context['keyword'], context['prefix'])
		
		elif context['type'] == 'after_operator':
			suggestions = self._suggest_value(context['key'], context['operator'], context['prefix'])
		
		elif context['type'] == 'after_relation':
			suggestions = self._suggest_relation(context['path'], context['prefix'])
		
		elif context['type'] == 'path':
			suggestions = self._suggest_path(context['prefix'])
		
		# Convert to dicts and return
		return [s.to_dict() for s in suggestions[:20]]  # Limit to 20 suggestions
	
	def _analyze_context(self, text: str) -> Dict[str, Any]:
		"""Analyze the text to determine the autocomplete context."""
		text = text.rstrip()
		
		# Empty or just started
		if not text or text.endswith(' ') or text.endswith('|') or text.endswith('('):
			return {'type': 'start', 'prefix': ''}
		
		# Find the current "word" being typed
		# Split by spaces, parens, and pipes
		parts = re.split(r'[\s|()]+', text)
		current = parts[-1] if parts else ''
		
		# Check for relation prefix !
		if current.startswith('!'):
			path_part = current[1:]
			if ':' in path_part:
				path, rel_prefix = path_part.rsplit(':', 1)
				return {'type': 'after_relation', 'path': path, 'prefix': rel_prefix}
			return {'type': 'path', 'prefix': path_part, 'for_relation': True}
		
		# Check for modifiers at start
		prefix_mods = ''
		while current and current[0] in '-^%':
			prefix_mods += current[0]
			current = current[1:]
		
		# Check for keyword:value pattern
		if ':' in current:
			key, value = current.split(':', 1)
			return {'type': 'after_operator', 'key': key.lower(), 'operator': ':', 'prefix': value}
		
		if '=' in current:
			key, value = current.split('=', 1)
			return {'type': 'after_operator', 'key': key.lower(), 'operator': '=', 'prefix': value}
		
		# Check for comparison operators
		for op in ['>=', '<=', '>', '<']:
			if op in current:
				key, value = current.split(op, 1)
				return {'type': 'after_operator', 'key': key.lower(), 'operator': op, 'prefix': value}
		
		# Check if typing a known keyword
		current_lower = current.lower()
		for keyword in self.KEYWORDS:
			if keyword.startswith(current_lower):
				return {'type': 'after_keyword', 'keyword': keyword, 'prefix': current}
		
		# Default to start context
		return {'type': 'start', 'prefix': current}
	
	def _suggest_start(self, prefix: str) -> List[Suggestion]:
		"""Suggest keywords and modifiers at the start of a term."""
		suggestions = []
		prefix_lower = prefix.lower()
		
		# Suggest modifiers if at very start
		if not prefix:
			for mod, desc in self.MODIFIERS:
				suggestions.append(Suggestion(
					text=mod,
					display=mod,
					type=SuggestionType.MODIFIER,
					description=desc
				))
		
		# Suggest keywords
		for keyword, (display, desc, insert) in self.KEYWORDS.items():
			if keyword.startswith(prefix_lower) or not prefix:
				suggestions.append(Suggestion(
					text=keyword,
					display=display,
					type=SuggestionType.KEYWORD,
					description=desc,
					insert_text=insert
				))
		
		# Suggest common metadata keys
		meta_keys = self._get_metadata_keys()
		for key in meta_keys:
			if key.lower().startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=key,
					display=f"{key}:",
					type=SuggestionType.METADATA_KEY,
					description=f"Metadata field",
					insert_text=f"{key}:"
				))
		
		# Suggest tags
		tags = self._get_all_tags()
		for tag in tags:
			if tag.startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=f"tag:{tag}",
					display=f"tag:{tag}",
					type=SuggestionType.TAG,
					description="Tag"
				))
		
		return suggestions
	
	def _suggest_for_keyword(self, keyword: str, prefix: str) -> List[Suggestion]:
		"""Suggest completions for a partially typed keyword."""
		suggestions = []
		prefix_lower = prefix.lower()
		
		for kw, (display, desc, insert) in self.KEYWORDS.items():
			if kw.startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=kw,
					display=display,
					type=SuggestionType.KEYWORD,
					description=desc,
					insert_text=insert
				))
		
		return suggestions
	
	def _suggest_value(self, key: str, operator: str, prefix: str) -> List[Suggestion]:
		"""Suggest values for a key:value expression."""
		suggestions = []
		prefix_lower = prefix.lower()
		
		if key == 'tag':
			tags = self._get_all_tags()
			for tag in tags:
				if tag.startswith(prefix_lower) or not prefix:
					suggestions.append(Suggestion(
						text=tag,
						display=tag,
						type=SuggestionType.TAG,
						description="Tag"
					))
		
		elif key == 'type':
			for t in ['VAULT', 'RECORD']:
				if t.lower().startswith(prefix_lower) or not prefix:
					suggestions.append(Suggestion(
						text=t,
						display=t,
						type=SuggestionType.NODE_TYPE,
						description="Node type"
					))
		
		elif key == 'ext':
			extensions = self._get_all_extensions()
			for ext in extensions:
				if ext.startswith(prefix_lower) or not prefix:
					suggestions.append(Suggestion(
						text=ext,
						display=ext,
						type=SuggestionType.EXTENSION,
						description="File extension"
					))
		
		elif key == 'inside' or key == 'path':
			paths = self._get_all_paths()
			for path in paths:
				if path.lower().startswith(prefix_lower) or not prefix:
					suggestions.append(Suggestion(
						text=path,
						display=path,
						type=SuggestionType.PATH,
						description="Path"
					))
		
		elif key == 'sort':
			for opt in self.SORT_OPTIONS:
				if opt.startswith(prefix_lower) or not prefix:
					suggestions.append(Suggestion(
						text=opt,
						display=opt,
						type=SuggestionType.KEYWORD,
						description="Sort order"
					))
		
		elif key == 'preview':
			for val in ['true', 'false']:
				if val.startswith(prefix_lower) or not prefix:
					suggestions.append(Suggestion(
						text=val,
						display=val,
						type=SuggestionType.KEYWORD,
						description="Has preview"
					))
		
		elif key in ('size', 'files', 'limit'):
			# Numeric suggestions
			if key == 'size':
				for size in ['1mb', '10mb', '100mb', '1gb']:
					if size.startswith(prefix_lower) or not prefix:
						suggestions.append(Suggestion(
							text=size,
							display=size,
							type=SuggestionType.KEYWORD,
							description="Size"
						))
		
		else:
			# It's a metadata key - suggest values
			values = self._get_metadata_values(key)
			for val in values:
				val_str = str(val)
				if val_str.lower().startswith(prefix_lower) or not prefix:
					suggestions.append(Suggestion(
						text=val_str,
						display=val_str,
						type=SuggestionType.METADATA_VALUE,
						description=f"{key} value"
					))
		
		return suggestions
	
	def _suggest_relation(self, path: str, prefix: str) -> List[Suggestion]:
		"""Suggest relation types."""
		relations = self._get_all_relations()
		prefix_upper = prefix.upper()
		
		suggestions = []
		for rel in relations:
			if rel.startswith(prefix_upper) or not prefix:
				suggestions.append(Suggestion(
					text=rel,
					display=rel,
					type=SuggestionType.RELATION,
					description="Relationship type"
				))
		
		return suggestions
	
	def _suggest_path(self, prefix: str) -> List[Suggestion]:
		"""Suggest paths for relation queries."""
		paths = self._get_all_paths()
		prefix_lower = prefix.lower()
		
		suggestions = []
		for path in paths:
			if path.lower().startswith(prefix_lower) or not prefix:
				suggestions.append(Suggestion(
					text=path,
					display=path,
					type=SuggestionType.PATH,
					description="Node path"
				))
		
		return suggestions
	
	# ============ Cache Methods ============
	
	def _get_all_tags(self) -> List[str]:
		"""Get all unique tags."""
		if 'tags' not in self._cache:
			cursor = self.conn.execute("SELECT DISTINCT tag FROM tags ORDER BY tag")
			self._cache['tags'] = [row[0] for row in cursor]
		return self._cache['tags']
	
	def _get_all_extensions(self) -> List[str]:
		"""Get all unique file extensions."""
		if 'extensions' not in self._cache:
			cursor = self.conn.execute("SELECT DISTINCT ext FROM blobs WHERE ext IS NOT NULL ORDER BY ext")
			self._cache['extensions'] = [row[0] for row in cursor]
		return self._cache['extensions']
	
	def _get_all_paths(self) -> List[str]:
		"""Get all node paths."""
		if 'paths' not in self._cache:
			cursor = self.conn.execute("SELECT cached_path FROM nodes ORDER BY cached_path")
			self._cache['paths'] = [row[0] for row in cursor]
		return self._cache['paths']
	
	def _get_all_relations(self) -> List[str]:
		"""Get all unique relation types."""
		if 'relations' not in self._cache:
			cursor = self.conn.execute("SELECT DISTINCT relation FROM edges ORDER BY relation")
			self._cache['relations'] = [row[0] for row in cursor]
		return self._cache['relations']
	
	def _get_metadata_keys(self) -> List[str]:
		"""Get all unique metadata keys."""
		if 'meta_keys' not in self._cache:
			keys = set()
			cursor = self.conn.execute("SELECT metadata FROM nodes WHERE metadata IS NOT NULL")
			for row in cursor:
				try:
					meta = json.loads(row[0])
					if isinstance(meta, dict):
						keys.update(meta.keys())
				except:
					pass
			self._cache['meta_keys'] = sorted(keys)
		return self._cache['meta_keys']
	
	def _get_metadata_values(self, key: str) -> List[Any]:
		"""Get all unique values for a metadata key."""
		cache_key = f'meta_values_{key}'
		if cache_key not in self._cache:
			values = set()
			cursor = self.conn.execute("SELECT metadata FROM nodes WHERE metadata IS NOT NULL")
			for row in cursor:
				try:
					meta = json.loads(row[0])
					if isinstance(meta, dict) and key in meta:
						val = meta[key]
						if isinstance(val, (str, int, float, bool)):
							values.add(val)
				except:
					pass
			self._cache[cache_key] = sorted(values, key=str)[:50]  # Limit to 50 values
		return self._cache[cache_key]