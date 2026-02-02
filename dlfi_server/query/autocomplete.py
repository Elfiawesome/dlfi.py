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
	section: str = ""      # Section grouping
	
	def to_dict(self) -> Dict[str, Any]:
		return {
			"text": self.text,
			"display": self.display,
			"type": self.type.name.lower(),
			"description": self.description,
			"insert_text": self.insert_text or self.text,
			"section": self.section
		}


class AutocompleteProvider:
	"""Provides autocomplete suggestions for queries."""
	
	# Query language keywords with descriptions
	KEYWORDS = [
		('tag:', 'Filter by tag', 'Tags'),
		('inside:', 'Search within path', 'Structure'),
		('path:', 'Match path pattern', 'Structure'),
		('ext:', 'Filter by file extension', 'Files'),
		('files>', 'Filter by file count (>, <, =)', 'Files'),
		('size>', 'Filter by size (e.g., size>10mb)', 'Files'),
		('type:', 'Filter by VAULT or RECORD', 'Filters'),
		('limit:', 'Limit number of results', 'Modifiers'),
		('sort:', 'Sort results (name, path, created, modified)', 'Modifiers'),
		('preview:', 'Has visual preview (true/false)', 'Files'),
	]
	
	# Modifiers
	MODIFIERS = [
		('-', 'Negate/exclude the next term', 'Modifiers'),
		('^', 'Deep search - include descendants', 'Modifiers'),
		('%', 'Reverse deep - include ancestors', 'Modifiers'),
		('!', 'Relationship query (e.g., !path:RELATION)', 'Relationships'),
	]
	
	# Sort options
	SORT_OPTIONS = ['name', 'path', 'created', 'modified', '-name', '-path', '-created', '-modified']
	
	def __init__(self, dlfi_instance):
		self.dlfi = dlfi_instance
		self.conn = dlfi_instance.conn
		self._cache = {}
	
	def invalidate_cache(self):
		"""Invalidate the autocomplete cache."""
		self._cache = {}
	
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
		text_before = query[:cursor_pos].rstrip()
		
		# Determine context
		context = self._analyze_context(text_before)
		
		suggestions = []
		
		if context['type'] == 'empty':
			suggestions = self._suggest_initial()
		
		elif context['type'] == 'start':
			suggestions = self._suggest_start(context['prefix'])
		
		elif context['type'] == 'keyword_partial':
			suggestions = self._suggest_keywords(context['prefix'])
		
		elif context['type'] == 'after_colon':
			suggestions = self._suggest_value(context['key'], ':', context['prefix'])
		
		elif context['type'] == 'after_equals':
			suggestions = self._suggest_value(context['key'], '=', context['prefix'])
		
		elif context['type'] == 'after_operator':
			suggestions = self._suggest_value(context['key'], context['operator'], context['prefix'])
		
		elif context['type'] == 'relation_path':
			suggestions = self._suggest_paths(context['prefix'], for_relation=True)
		
		elif context['type'] == 'relation_type':
			suggestions = self._suggest_relations(context['prefix'])
		
		# Convert to dicts and return
		return [s.to_dict() for s in suggestions[:25]]
	
	def _analyze_context(self, text: str) -> Dict[str, Any]:
		"""Analyze the text to determine the autocomplete context."""
		# Empty query
		if not text:
			return {'type': 'empty'}
		
		# Check if we just typed a space, |, or (
		if text[-1] in ' |(':
			return {'type': 'empty'}
		
		# Find the current token being typed
		# Split by spaces, but keep track of position
		tokens = re.split(r'([\s|()]+)', text)
		current_token = ''
		for t in reversed(tokens):
			if t and not re.match(r'^[\s|()]+$', t):
				current_token = t
				break
		
		if not current_token:
			return {'type': 'empty'}
		
		# Check for relation prefix !
		if current_token.startswith('!'):
			path_part = current_token[1:]
			if ':' in path_part:
				# After !path: - suggest relation types
				path, rel_prefix = path_part.rsplit(':', 1)
				return {'type': 'relation_type', 'path': path, 'prefix': rel_prefix}
			# Still typing path
			return {'type': 'relation_path', 'prefix': path_part}
		
		# Strip leading modifiers for analysis
		clean_token = current_token.lstrip('-^%')
		
		# Check for key:value or key=value
		for op in [':', '=', '>=', '<=', '>', '<']:
			if op in clean_token:
				parts = clean_token.split(op, 1)
				key = parts[0]
				value = parts[1] if len(parts) > 1 else ''
				return {
					'type': 'after_colon' if op == ':' else ('after_equals' if op == '=' else 'after_operator'),
					'key': key.lower(),
					'operator': op,
					'prefix': value
				}
		
		# Check if it looks like a partial keyword
		if clean_token and any(kw[0].startswith(clean_token.lower()) for kw in self.KEYWORDS):
			return {'type': 'keyword_partial', 'prefix': clean_token}
		
		# General start - could be keyword, metadata key, or search term
		return {'type': 'start', 'prefix': clean_token}
	
	def _suggest_initial(self) -> List[Suggestion]:
		"""Suggest keywords and modifiers when starting fresh."""
		suggestions = []
		
		# Add modifiers first
		for mod, desc, section in self.MODIFIERS:
			suggestions.append(Suggestion(
				text=mod,
				display=mod,
				type=SuggestionType.MODIFIER,
				description=desc,
				insert_text=mod,
				section=section
			))
		
		# Add keywords
		for kw, desc, section in self.KEYWORDS:
			suggestions.append(Suggestion(
				text=kw,
				display=kw,
				type=SuggestionType.KEYWORD,
				description=desc,
				insert_text=kw,
				section=section
			))
		
		# Add common metadata keys
		meta_keys = self._get_metadata_keys()[:10]
		for key in meta_keys:
			suggestions.append(Suggestion(
				text=f"{key}:",
				display=f"{key}:",
				type=SuggestionType.METADATA_KEY,
				description="Metadata field",
				insert_text=f"{key}:",
				section="Metadata"
			))
		
		return suggestions
	
	def _suggest_start(self, prefix: str) -> List[Suggestion]:
		"""Suggest keywords and metadata keys matching prefix."""
		suggestions = []
		prefix_lower = prefix.lower()
		
		# Match keywords
		for kw, desc, section in self.KEYWORDS:
			kw_name = kw.rstrip(':>=<')
			if kw_name.startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=kw,
					display=kw,
					type=SuggestionType.KEYWORD,
					description=desc,
					insert_text=kw,
					section=section
				))
		
		# Match modifiers
		for mod, desc, section in self.MODIFIERS:
			if mod.startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=mod,
					display=mod,
					type=SuggestionType.MODIFIER,
					description=desc,
					insert_text=mod,
					section=section
				))
		
		# Match metadata keys
		meta_keys = self._get_metadata_keys()
		for key in meta_keys:
			if key.lower().startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=f"{key}:",
					display=f"{key}:",
					type=SuggestionType.METADATA_KEY,
					description="Metadata field",
					insert_text=f"{key}:",
					section="Metadata"
				))
		
		# Match tags with tag: prefix
		tags = self._get_all_tags()
		for tag in tags:
			if tag.startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=f"tag:{tag}",
					display=f"tag:{tag}",
					type=SuggestionType.TAG,
					description="Tag",
					insert_text=f"tag:{tag}",
					section="Tags"
				))
		
		return suggestions
	
	def _suggest_keywords(self, prefix: str) -> List[Suggestion]:
		"""Suggest keywords matching the prefix."""
		suggestions = []
		prefix_lower = prefix.lower()
		
		for kw, desc, section in self.KEYWORDS:
			kw_name = kw.rstrip(':>=<')
			if kw_name.startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=kw,
					display=kw,
					type=SuggestionType.KEYWORD,
					description=desc,
					insert_text=kw,
					section=section
				))
		
		return suggestions
	
	def _suggest_value(self, key: str, operator: str, prefix: str) -> List[Suggestion]:
		"""Suggest values for a key:value or key=value expression."""
		suggestions = []
		prefix_lower = prefix.lower()
		key_lower = key.lower()
		
		if key_lower == 'tag':
			tags = self._get_all_tags()
			for tag in tags:
				if not prefix or tag.startswith(prefix_lower):
					suggestions.append(Suggestion(
						text=tag,
						display=tag,
						type=SuggestionType.TAG,
						description="Tag",
						insert_text=tag,
						section="Tags"
					))
		
		elif key_lower == 'type':
			for t in ['VAULT', 'RECORD']:
				if not prefix or t.lower().startswith(prefix_lower):
					suggestions.append(Suggestion(
						text=t,
						display=t,
						type=SuggestionType.NODE_TYPE,
						description="Node type",
						insert_text=t,
						section="Types"
					))
		
		elif key_lower == 'ext':
			extensions = self._get_all_extensions()
			for ext in extensions:
				if not prefix or ext.startswith(prefix_lower):
					suggestions.append(Suggestion(
						text=ext,
						display=ext,
						type=SuggestionType.EXTENSION,
						description="File extension",
						insert_text=ext,
						section="Extensions"
					))
		
		elif key_lower in ('inside', 'path'):
			paths = self._get_all_paths()
			for path in paths:
				if not prefix or path.lower().startswith(prefix_lower):
					suggestions.append(Suggestion(
						text=path,
						display=path,
						type=SuggestionType.PATH,
						description="Path",
						insert_text=path,
						section="Paths"
					))
		
		elif key_lower == 'sort':
			for opt in self.SORT_OPTIONS:
				if not prefix or opt.startswith(prefix_lower):
					desc = "Descending" if opt.startswith('-') else "Ascending"
					suggestions.append(Suggestion(
						text=opt,
						display=opt,
						type=SuggestionType.KEYWORD,
						description=desc,
						insert_text=opt,
						section="Sort"
					))
		
		elif key_lower == 'preview':
			for val in ['true', 'false']:
				if not prefix or val.startswith(prefix_lower):
					suggestions.append(Suggestion(
						text=val,
						display=val,
						type=SuggestionType.KEYWORD,
						description="Has preview" if val == 'true' else "No preview",
						insert_text=val,
						section="Values"
					))
		
		elif key_lower == 'size':
			sizes = ['1kb', '10kb', '100kb', '1mb', '10mb', '100mb', '1gb']
			for size in sizes:
				if not prefix or size.startswith(prefix_lower):
					suggestions.append(Suggestion(
						text=size,
						display=size,
						type=SuggestionType.KEYWORD,
						description="Size",
						insert_text=size,
						section="Sizes"
					))
		
		elif key_lower in ('files', 'limit'):
			# Numeric suggestions
			for num in ['1', '5', '10', '25', '50', '100']:
				if not prefix or num.startswith(prefix):
					suggestions.append(Suggestion(
						text=num,
						display=num,
						type=SuggestionType.KEYWORD,
						description="",
						insert_text=num,
						section="Numbers"
					))
		
		else:
			# It's a metadata key - suggest values for this key
			values = self._get_metadata_values(key)
			for val in values:
				val_str = str(val)
				if not prefix or val_str.lower().startswith(prefix_lower):
					suggestions.append(Suggestion(
						text=val_str,
						display=val_str,
						type=SuggestionType.METADATA_VALUE,
						description=f"{key} value",
						insert_text=val_str if ' ' not in val_str else f'"{val_str}"',
						section="Values"
					))
		
		return suggestions
	
	def _suggest_paths(self, prefix: str, for_relation: bool = False) -> List[Suggestion]:
		"""Suggest paths."""
		suggestions = []
		prefix_lower = prefix.lower()
		paths = self._get_all_paths()
		
		for path in paths:
			if not prefix or path.lower().startswith(prefix_lower):
				suggestions.append(Suggestion(
					text=path,
					display=path,
					type=SuggestionType.PATH,
					description="Node path",
					insert_text=path + (':' if for_relation else ''),
					section="Paths"
				))
		
		return suggestions
	
	def _suggest_relations(self, prefix: str) -> List[Suggestion]:
		"""Suggest relation types."""
		suggestions = []
		prefix_upper = prefix.upper()
		relations = self._get_all_relations()
		
		for rel in relations:
			if not prefix or rel.startswith(prefix_upper):
				suggestions.append(Suggestion(
					text=rel,
					display=rel,
					type=SuggestionType.RELATION,
					description="Relationship type",
					insert_text=rel,
					section="Relations"
				))
		
		# Add direction hints
		if prefix:
			for direction, desc in [('>', 'Outgoing only'), ('<', 'Incoming only')]:
				suggestions.append(Suggestion(
					text=prefix + direction,
					display=prefix + direction,
					type=SuggestionType.OPERATOR,
					description=desc,
					insert_text=prefix + direction,
					section="Direction"
				))
		
		return suggestions
	
	# ============ Cache Methods ============
	
	def _get_all_tags(self) -> List[str]:
		"""Get all unique tags."""
		if 'tags' not in self._cache:
			cursor = self.conn.execute("SELECT DISTINCT tag FROM tags ORDER BY tag LIMIT 100")
			self._cache['tags'] = [row[0] for row in cursor]
		return self._cache['tags']
	
	def _get_all_extensions(self) -> List[str]:
		"""Get all unique file extensions."""
		if 'extensions' not in self._cache:
			cursor = self.conn.execute("SELECT DISTINCT ext FROM blobs WHERE ext IS NOT NULL AND ext != '' ORDER BY ext LIMIT 50")
			self._cache['extensions'] = [row[0] for row in cursor]
		return self._cache['extensions']
	
	def _get_all_paths(self) -> List[str]:
		"""Get all node paths."""
		if 'paths' not in self._cache:
			cursor = self.conn.execute("SELECT cached_path FROM nodes ORDER BY cached_path LIMIT 200")
			self._cache['paths'] = [row[0] for row in cursor]
		return self._cache['paths']
	
	def _get_all_relations(self) -> List[str]:
		"""Get all unique relation types."""
		if 'relations' not in self._cache:
			cursor = self.conn.execute("SELECT DISTINCT relation FROM edges ORDER BY relation LIMIT 50")
			self._cache['relations'] = [row[0] for row in cursor]
		return self._cache['relations']
	
	def _get_metadata_keys(self) -> List[str]:
		"""Get all unique metadata keys."""
		if 'meta_keys' not in self._cache:
			keys = set()
			cursor = self.conn.execute("SELECT metadata FROM nodes WHERE metadata IS NOT NULL AND metadata != '{}' LIMIT 500")
			for row in cursor:
				try:
					meta = json.loads(row[0])
					if isinstance(meta, dict):
						keys.update(meta.keys())
				except:
					pass
			self._cache['meta_keys'] = sorted(keys)[:50]
		return self._cache['meta_keys']
	
	def _get_metadata_values(self, key: str) -> List[Any]:
		"""Get all unique values for a metadata key."""
		cache_key = f'meta_values_{key}'
		if cache_key not in self._cache:
			values = set()
			cursor = self.conn.execute("SELECT metadata FROM nodes WHERE metadata IS NOT NULL LIMIT 500")
			for row in cursor:
				try:
					meta = json.loads(row[0])
					if isinstance(meta, dict) and key in meta:
						val = meta[key]
						if isinstance(val, (str, int, float, bool)):
							values.add(val)
				except:
					pass
			self._cache[cache_key] = sorted(values, key=lambda x: str(x))[:50]
		return self._cache[cache_key]