"""
DLFI Query Language Parser

Parses the advanced query syntax into an Abstract Syntax Tree (AST)
that can be executed against the database.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, Union, Any
from enum import Enum, auto


class ParseError(Exception):
	"""Raised when query parsing fails."""
	def __init__(self, message: str, position: int = 0):
		self.message = message
		self.position = position
		super().__init__(f"{message} at position {position}")


class TokenType(Enum):
	# Literals
	TEXT = auto()
	QUOTED = auto()
	NUMBER = auto()
	
	# Operators
	COLON = auto()          # :
	EQUALS = auto()         # =
	GT = auto()             # >
	LT = auto()             # <
	GTE = auto()            # >=
	LTE = auto()            # <=
	RANGE = auto()          # ..
	OR = auto()             # |
	LPAREN = auto()         # (
	RPAREN = auto()         # )
	
	# Prefixes
	NEGATE = auto()         # -
	DEEP = auto()           # ^
	REVERSE_DEEP = auto()   # %
	RELATION = auto()       # !
	
	# Special
	QUESTION = auto()       # ?
	STAR = auto()           # *
	DOUBLESTAR = auto()     # **
	
	EOF = auto()


@dataclass
class Token:
	type: TokenType
	value: str
	position: int


class TermType(Enum):
	"""Types of search terms."""
	GLOBAL_SEARCH = auto()      # Plain text search
	TAG = auto()                # tag:value or tag=value
	METADATA = auto()           # key:value or key=value
	META_EXISTS = auto()        # key? or key (existence check)
	INSIDE = auto()             # inside:path
	PATH_PATTERN = auto()       # path:pattern
	RELATION = auto()           # !path or !path:REL
	RELATION_TYPE = auto()      # RELATION_NAME (uppercase)
	EXTENSION = auto()          # ext:value
	FILE_COUNT = auto()         # files>N
	SIZE = auto()               # size>10mb
	TYPE = auto()               # type:VAULT
	LIMIT = auto()              # limit:N
	SORT = auto()               # sort:key
	PREVIEW = auto()            # preview:true


class Operator(Enum):
	"""Comparison operators for terms."""
	CONTAINS = auto()           # : (partial match)
	EQUALS = auto()             # = (exact match)
	EXISTS = auto()             # ? or bare key
	NOT_EXISTS = auto()         # -key
	GT = auto()                 # >
	LT = auto()                 # <
	GTE = auto()                # >=
	LTE = auto()                # <=
	RANGE = auto()              # val..val


@dataclass
class Modifier:
	"""Modifiers that can be applied to terms."""
	negated: bool = False       # - prefix
	deep: bool = False          # ^ prefix (include descendants)
	reverse_deep: bool = False  # % prefix (include ancestors)
	direction: Optional[str] = None  # > or < for relations


@dataclass
class Term:
	"""A single search term in the query."""
	type: TermType
	key: Optional[str] = None
	operator: Operator = Operator.CONTAINS
	value: Any = None
	value_end: Any = None  # For range queries
	modifier: Modifier = field(default_factory=Modifier)


@dataclass
class OrGroup:
	"""Represents terms joined by OR (|)."""
	terms: List[Union['Term', 'OrGroup', 'AndGroup']]


@dataclass
class AndGroup:
	"""Represents terms joined by AND (implicit)."""
	terms: List[Union['Term', 'OrGroup', 'AndGroup']]


# Type alias for AST nodes
ASTNode = Union[Term, OrGroup, AndGroup]


class Lexer:
	"""Tokenizes a query string."""
	
	KEYWORDS = {
		'tag', 'inside', 'path', 'ext', 'files', 'size', 
		'type', 'limit', 'sort', 'preview'
	}
	
	def __init__(self, query: str):
		self.query = query
		self.pos = 0
		self.tokens: List[Token] = []
	
	def tokenize(self) -> List[Token]:
		"""Convert query string to tokens."""
		self.tokens = []
		self.pos = 0
		
		while self.pos < len(self.query):
			self._skip_whitespace()
			if self.pos >= len(self.query):
				break
			
			char = self.query[self.pos]
			
			if char == '"':
				self._read_quoted()
			elif char == ':':
				self._add_token(TokenType.COLON, ':')
			elif char == '=':
				self._add_token(TokenType.EQUALS, '=')
			elif char == '>':
				if self._peek(1) == '=':
					self._add_token(TokenType.GTE, '>=')
					self.pos += 1
				else:
					self._add_token(TokenType.GT, '>')
			elif char == '<':
				if self._peek(1) == '=':
					self._add_token(TokenType.LTE, '<=')
					self.pos += 1
				else:
					self._add_token(TokenType.LT, '<')
			elif char == '|':
				self._add_token(TokenType.OR, '|')
			elif char == '(':
				self._add_token(TokenType.LPAREN, '(')
			elif char == ')':
				self._add_token(TokenType.RPAREN, ')')
			elif char == '-':
				self._add_token(TokenType.NEGATE, '-')
			elif char == '^':
				self._add_token(TokenType.DEEP, '^')
			elif char == '%':
				self._add_token(TokenType.REVERSE_DEEP, '%')
			elif char == '!':
				self._add_token(TokenType.RELATION, '!')
			elif char == '?':
				self._add_token(TokenType.QUESTION, '?')
			elif char == '*':
				if self._peek(1) == '*':
					self._add_token(TokenType.DOUBLESTAR, '**')
					self.pos += 1
				else:
					self._add_token(TokenType.STAR, '*')
			elif char == '.':
				if self._peek(1) == '.':
					self._add_token(TokenType.RANGE, '..')
					self.pos += 1
				else:
					self._read_text()
					continue  # _read_text advances pos
			else:
				self._read_text()
				continue  # _read_text advances pos
			
			self.pos += 1
		
		self.tokens.append(Token(TokenType.EOF, '', self.pos))
		return self.tokens
	
	def _skip_whitespace(self):
		while self.pos < len(self.query) and self.query[self.pos].isspace():
			self.pos += 1
	
	def _peek(self, offset: int = 0) -> Optional[str]:
		pos = self.pos + offset
		if pos < len(self.query):
			return self.query[pos]
		return None
	
	def _add_token(self, type: TokenType, value: str):
		self.tokens.append(Token(type, value, self.pos))
	
	def _read_quoted(self):
		"""Read a quoted string."""
		start = self.pos
		self.pos += 1  # Skip opening quote
		value = []
		
		while self.pos < len(self.query):
			char = self.query[self.pos]
			if char == '"':
				self.tokens.append(Token(TokenType.QUOTED, ''.join(value), start))
				return
			elif char == '\\' and self.pos + 1 < len(self.query):
				self.pos += 1
				value.append(self.query[self.pos])
			else:
				value.append(char)
			self.pos += 1
		
		# Unclosed quote - treat as text
		self.tokens.append(Token(TokenType.QUOTED, ''.join(value), start))
	
	def _read_text(self):
		"""Read plain text until a special character."""
		start = self.pos
		value = []
		
		special = set(':=><|()!^%?*"')
		
		while self.pos < len(self.query):
			char = self.query[self.pos]
			
			if char.isspace() or char in special:
				# Check for .. range operator
				if char == '.' and self._peek(1) == '.':
					break
				if char not in special or char == '.':
					if char.isspace():
						break
					value.append(char)
					self.pos += 1
					continue
				break
			
			value.append(char)
			self.pos += 1
		
		text = ''.join(value)
		if text:
			# Check if it's a number
			if re.match(r'^-?\d+(\.\d+)?$', text):
				self.tokens.append(Token(TokenType.NUMBER, text, start))
			else:
				self.tokens.append(Token(TokenType.TEXT, text, start))


class QueryParser:
	"""Parses tokenized query into an AST."""
	
	# Reserved keywords that indicate a specific term type
	RESERVED_KEYS = {
		'tag': TermType.TAG,
		'inside': TermType.INSIDE,
		'path': TermType.PATH_PATTERN,
		'ext': TermType.EXTENSION,
		'files': TermType.FILE_COUNT,
		'size': TermType.SIZE,
		'type': TermType.TYPE,
		'limit': TermType.LIMIT,
		'sort': TermType.SORT,
		'preview': TermType.PREVIEW,
	}
	
	def __init__(self, query: str):
		self.query = query
		self.lexer = Lexer(query)
		self.tokens: List[Token] = []
		self.pos = 0
	
	def parse(self) -> AndGroup:
		"""Parse the query and return the AST root."""
		self.tokens = self.lexer.tokenize()
		self.pos = 0
		
		terms = []
		while not self._is_at_end():
			term = self._parse_or_group()
			if term:
				terms.append(term)
		
		return AndGroup(terms=terms)
	
	def _current(self) -> Token:
		if self.pos < len(self.tokens):
			return self.tokens[self.pos]
		return self.tokens[-1]  # EOF
	
	def _peek(self, offset: int = 0) -> Token:
		pos = self.pos + offset
		if pos < len(self.tokens):
			return self.tokens[pos]
		return self.tokens[-1]
	
	def _advance(self) -> Token:
		token = self._current()
		self.pos += 1
		return token
	
	def _is_at_end(self) -> bool:
		return self._current().type == TokenType.EOF
	
	def _match(self, *types: TokenType) -> bool:
		if self._current().type in types:
			self._advance()
			return True
		return False
	
	def _parse_or_group(self) -> Optional[ASTNode]:
		"""Parse terms that may be joined by OR."""
		left = self._parse_term()
		if not left:
			return None
		
		terms = [left]
		
		while self._match(TokenType.OR):
			right = self._parse_term()
			if right:
				terms.append(right)
		
		if len(terms) == 1:
			return terms[0]
		
		return OrGroup(terms=terms)
	
	def _parse_term(self) -> Optional[ASTNode]:
		"""Parse a single term or grouped expression."""
		# Handle grouping
		if self._match(TokenType.LPAREN):
			terms = []
			while not self._is_at_end() and self._current().type != TokenType.RPAREN:
				term = self._parse_or_group()
				if term:
					terms.append(term)
			self._match(TokenType.RPAREN)  # Consume closing paren
			if len(terms) == 1:
				return terms[0]
			return AndGroup(terms=terms)
		
		# Handle modifiers
		modifier = Modifier()
		
		while True:
			if self._match(TokenType.NEGATE):
				modifier.negated = True
			elif self._match(TokenType.DEEP):
				modifier.deep = True
			elif self._match(TokenType.REVERSE_DEEP):
				modifier.reverse_deep = True
			elif self._match(TokenType.RELATION):
				return self._parse_relation(modifier)
			else:
				break
		
		return self._parse_key_value(modifier)
	
	def _parse_relation(self, modifier: Modifier) -> Optional[Term]:
		"""Parse a relation query like !path or !path:REL."""
		if self._current().type not in (TokenType.TEXT, TokenType.QUOTED):
			return None
		
		path = self._advance().value
		relation = None
		direction = None
		
		if self._match(TokenType.COLON):
			if self._current().type == TokenType.TEXT:
				relation = self._advance().value
				
				# Check for direction indicator
				if self._match(TokenType.GT):
					direction = '>'
				elif self._match(TokenType.LT):
					direction = '<'
		
		modifier.direction = direction
		
		return Term(
			type=TermType.RELATION,
			key=path,
			value=relation,
			modifier=modifier
		)
	
	def _parse_key_value(self, modifier: Modifier) -> Optional[Term]:
		"""Parse a key:value, key=value, or plain text term."""
		token = self._current()
		
		if token.type == TokenType.EOF:
			return None
		
		if token.type == TokenType.RPAREN:
			return None
		
		# Quoted text - global search
		if token.type == TokenType.QUOTED:
			self._advance()
			return Term(
				type=TermType.GLOBAL_SEARCH,
				value=token.value,
				operator=Operator.EQUALS,  # Exact phrase match
				modifier=modifier
			)
		
		# Check for key:value or key=value pattern
		if token.type == TokenType.TEXT:
			key = self._advance().value
			
			# Check if this is an uppercase RELATION_NAME
			if key.isupper() and len(key) > 2:
				return Term(
					type=TermType.RELATION_TYPE,
					value=key,
					modifier=modifier
				)
			
			# Check for existence query (key?)
			if self._match(TokenType.QUESTION):
				term_type = TermType.META_EXISTS
				if key.lower() == 'tag':
					term_type = TermType.TAG
				return Term(
					type=term_type,
					key=key if key.lower() != 'tag' else None,
					operator=Operator.EXISTS,
					modifier=modifier
				)
			
			# Check for operators
			operator = None
			if self._match(TokenType.COLON):
				operator = Operator.CONTAINS
			elif self._match(TokenType.EQUALS):
				operator = Operator.EQUALS
			elif self._match(TokenType.GT):
				if self._match(TokenType.EQUALS):
					operator = Operator.GTE
				else:
					operator = Operator.GT
			elif self._match(TokenType.LT):
				if self._match(TokenType.EQUALS):
					operator = Operator.LTE
				else:
					operator = Operator.LT
			
			if operator:
				return self._parse_value(key, operator, modifier)
			
			# Bare key - could be metadata existence or global search
			if modifier.negated:
				# -key means field doesn't exist
				return Term(
					type=TermType.META_EXISTS,
					key=key,
					operator=Operator.NOT_EXISTS,
					modifier=Modifier()  # Clear negated since it's in operator
				)
			
			# Plain text - global search
			return Term(
				type=TermType.GLOBAL_SEARCH,
				value=key,
				operator=Operator.CONTAINS,
				modifier=modifier
			)
		
		# Number without key - global search
		if token.type == TokenType.NUMBER:
			self._advance()
			return Term(
				type=TermType.GLOBAL_SEARCH,
				value=token.value,
				operator=Operator.CONTAINS,
				modifier=modifier
			)
		
		return None
	
	def _parse_value(self, key: str, operator: Operator, modifier: Modifier) -> Term:
		"""Parse the value part of a key:value expression."""
		key_lower = key.lower()
		
		# Check if key contains dots (nested path like meta.artist.name)
		# Only treat as reserved keyword if it's a simple key
		if '.' not in key_lower:
			term_type = self.RESERVED_KEYS.get(key_lower, TermType.METADATA)
		else:
			term_type = TermType.METADATA
		
		# Parse the value
		value = self._read_value()
		value_end = None
		
		# Check for range operator
		if self._match(TokenType.RANGE):
			value_end = self._read_value()
			if value_end == '*':
				value_end = None  # Open-ended range
			operator = Operator.RANGE
		
		# Handle size units
		if term_type == TermType.SIZE and isinstance(value, str):
			value = self._parse_size(value)
			if value_end:
				value_end = self._parse_size(value_end)
		
		# For metadata, keep the original key (with dots for nested paths)
		return Term(
			type=term_type,
			key=key if term_type == TermType.METADATA else None,
			operator=operator,
			value=value,
			value_end=value_end,
			modifier=modifier
		)
	
	def _read_value(self) -> Any:
		"""Read a value (text, number, or quoted)."""
		token = self._current()
		
		if token.type == TokenType.QUOTED:
			self._advance()
			return token.value
		
		if token.type == TokenType.NUMBER:
			self._advance()
			# Try to convert to int or float
			try:
				if '.' in token.value:
					return float(token.value)
				return int(token.value)
			except ValueError:
				return token.value
		
		if token.type == TokenType.TEXT:
			self._advance()
			return token.value
		
		if token.type == TokenType.STAR:
			self._advance()
			return '*'
		
		return None
	
	def _parse_size(self, value: str) -> int:
		"""Parse size value with units (e.g., 10mb, 1gb)."""
		if isinstance(value, (int, float)):
			return int(value)
		
		value = str(value).lower().strip()
		
		units = {
			'b': 1,
			'kb': 1024,
			'mb': 1024 * 1024,
			'gb': 1024 * 1024 * 1024,
			'tb': 1024 * 1024 * 1024 * 1024,
		}
		
		for unit, multiplier in units.items():
			if value.endswith(unit):
				try:
					num = float(value[:-len(unit)])
					return int(num * multiplier)
				except ValueError:
					pass
		
		try:
			return int(value)
		except ValueError:
			return 0