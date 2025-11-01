import re
from dataclasses import dataclass
from typing import List, Iterator, Optional
from enum import Enum

class TokenType(Enum):
    # Core SQL Keywords
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    ORDER = "ORDER"
    BY = "BY"
    ASC = "ASC"
    DESC = "DESC"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    
    # JOIN keywords
    JOIN = "JOIN"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    OUTER = "OUTER"
    CROSS = "CROSS"
    NATURAL = "NATURAL"
    ON = "ON"
    USING = "USING"
    
    # Conditional keywords
    IN = "IN"
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    BETWEEN = "BETWEEN"
    IS = "IS"
    NULL = "NULL"
    TRUE = "TRUE"
    FALSE = "FALSE"
    
    # Aggregate and window functions
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"
    DISTINCT = "DISTINCT"
    ALL = "ALL"
    
    # Window functions
    OVER = "OVER"
    PARTITION = "PARTITION"
    ROW_NUMBER = "ROW_NUMBER"
    RANK = "RANK"
    DENSE_RANK = "DENSE_RANK"
    LAG = "LAG"
    LEAD = "LEAD"
    FIRST_VALUE = "FIRST_VALUE"
    LAST_VALUE = "LAST_VALUE"
    
    # Grouping and having
    GROUP = "GROUP"
    HAVING = "HAVING"
    ROLLUP = "ROLLUP"
    CUBE = "CUBE"
    GROUPING = "GROUPING"
    
    # Limit and offset
    LIMIT = "LIMIT"
    OFFSET = "OFFSET"
    TOP = "TOP"
    FETCH = "FETCH"
    FIRST = "FIRST"
    NEXT = "NEXT"
    ROWS = "ROWS"
    ONLY = "ONLY"
    
    # Subquery keywords
    EXISTS = "EXISTS"
    ANY = "ANY"
    SOME = "SOME"
    
    # Set operations
    UNION = "UNION"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"
    MINUS = "MINUS"
    
    # Case statement
    CASE = "CASE"
    WHEN = "WHEN"
    THEN = "THEN"
    ELSE = "ELSE"
    END = "END"
    
    # Data types
    INTEGER = "INTEGER"
    INT = "INT"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    FLOAT = "FLOAT"
    REAL = "REAL"
    DOUBLE = "DOUBLE"
    PRECISION = "PRECISION"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    TEXT = "TEXT"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"
    BOOL = "BOOL"
    
    # DML operations
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    INTO = "INTO"
    VALUES = "VALUES"
    SET = "SET"
    
    # DDL operations
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    TABLE = "TABLE"
    VIEW = "VIEW"
    INDEX = "INDEX"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    
    # Constraints
    PRIMARY = "PRIMARY"
    KEY = "KEY"
    FOREIGN = "FOREIGN"
    REFERENCES = "REFERENCES"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"
    DEFAULT = "DEFAULT"
    AUTO_INCREMENT = "AUTO_INCREMENT"
    
    # String functions
    CONCAT = "CONCAT"
    SUBSTRING = "SUBSTRING"
    LENGTH = "LENGTH"
    UPPER = "UPPER"
    LOWER = "LOWER"
    TRIM = "TRIM"
    LTRIM = "LTRIM"
    RTRIM = "RTRIM"
    REPLACE = "REPLACE"
    
    # Date functions
    NOW = "NOW"
    CURRENT_DATE = "CURRENT_DATE"
    CURRENT_TIME = "CURRENT_TIME"
    CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"
    EXTRACT = "EXTRACT"
    DATE_ADD = "DATE_ADD"
    DATE_SUB = "DATE_SUB"
    DATEDIFF = "DATEDIFF"
    
    # Math functions
    ABS = "ABS"
    CEIL = "CEIL"
    FLOOR = "FLOOR"
    ROUND = "ROUND"
    SQRT = "SQRT"
    POWER = "POWER"
    MOD = "MOD"
    
    # Conditional functions
    COALESCE = "COALESCE"
    NULLIF = "NULLIF"
    ISNULL = "ISNULL"
    
    # Operators
    EQUALS = "="
    NOT_EQUALS = "!="
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_EQUAL = "<="
    GREATER_EQUAL = ">="
    PLUS = "+"
    MINUS = "-"
    MULTIPLY = "*"
    DIVIDE = "/"
    MODULO = "%"
    POWER_OP = "^"
    CONCAT_OP = "||"
    
    # Literals and identifiers
    IDENTIFIER = "IDENTIFIER"
    NUMBER = "NUMBER"
    STRING = "STRING"
    QUOTED_IDENTIFIER = "QUOTED_IDENTIFIER"
    
    # Punctuation
    COMMA = ","
    SEMICOLON = ";"
    LPAREN = "("
    RPAREN = ")"
    ASTERISK = "*"
    DOT = "."
    LBRACKET = "["
    RBRACKET = "]"
    
    # Special tokens
    AS = "AS"
    ALIAS = "ALIAS"
    COMMENT = "COMMENT"
    WHITESPACE = "WHITESPACE"
    EOF = "EOF"

@dataclass
class Token:
    type: TokenType
    value: str
    lineno: int
    column: int
    raw_value: Optional[str] = None

class UniversalSQLLexer:
    """
    Universal SQL Lexer that supports comprehensive SQL syntax including:
    - All standard SQL keywords and functions
    - Multiple SQL dialects (MySQL, PostgreSQL, SQL Server, Oracle, SQLite)
    - Complex expressions and operators
    - Comments and quoted identifiers
    """
    
    def __init__(self):
        self.keywords = self._build_keyword_map()
        self.operators = self._build_operator_map()
        self.punctuation = self._build_punctuation_map()
        
    def _build_keyword_map(self):
        """Build comprehensive keyword mapping"""
        keywords = {}
        
        # Add all TokenType enum values that are keywords
        for token_type in TokenType:
            if token_type.value.isalpha() or '_' in token_type.value:
                keywords[token_type.value.upper()] = token_type
                
        # Add common aliases and variations
        aliases = {
            'INTEGER': TokenType.INT,
            'BOOLEAN': TokenType.BOOL,
            'CHARACTER': TokenType.CHAR,
            'VARYING': TokenType.VARCHAR,
            'DOUBLE PRECISION': TokenType.DOUBLE,
        }
        
        keywords.update(aliases)
        return keywords
    
    def _build_operator_map(self):
        """Build operator mapping"""
        return {
            '=': TokenType.EQUALS,
            '!=': TokenType.NOT_EQUALS,
            '<>': TokenType.NOT_EQUALS,
            '<': TokenType.LESS_THAN,
            '>': TokenType.GREATER_THAN,
            '<=': TokenType.LESS_EQUAL,
            '>=': TokenType.GREATER_EQUAL,
            '+': TokenType.PLUS,
            '-': TokenType.MINUS,
            '*': TokenType.MULTIPLY,
            '/': TokenType.DIVIDE,
            '%': TokenType.MODULO,
            '^': TokenType.POWER_OP,
            '||': TokenType.CONCAT_OP,
        }
    
    def _build_punctuation_map(self):
        """Build punctuation mapping"""
        return {
            ',': TokenType.COMMA,
            ';': TokenType.SEMICOLON,
            '(': TokenType.LPAREN,
            ')': TokenType.RPAREN,
            '*': TokenType.ASTERISK,
            '.': TokenType.DOT,
            '[': TokenType.LBRACKET,
            ']': TokenType.RBRACKET,
        }
    
    def tokenize(self, text: str) -> List[Token]:
        """Tokenize SQL text with comprehensive support"""
        tokens = []
        lines = text.split('\n')
        
        for line_num, line in enumerate(lines, 1):
            col = 0
            i = 0
            
            while i < len(line):
                # Skip whitespace
                if line[i].isspace():
                    i += 1
                    col += 1
                    continue
                
                # Handle comments
                if i < len(line) - 1:
                    # SQL comments --
                    if line[i:i+2] == '--':
                        # Skip rest of line
                        break
                    
                    # C-style comments /* */
                    if line[i:i+2] == '/*':
                        comment_start = i
                        i += 2
                        while i < len(line) - 1:
                            if line[i:i+2] == '*/':
                                i += 2
                                break
                            i += 1
                        col = i
                        continue
                
                # Multi-character operators
                if i < len(line) - 1:
                    two_char = line[i:i+2]
                    if two_char in self.operators:
                        tokens.append(Token(self.operators[two_char], two_char, line_num, col))
                        i += 2
                        col += 2
                        continue
                
                # Single character operators
                if line[i] in self.operators:
                    # Special handling for * (could be multiply or asterisk)
                    if line[i] == '*':
                        tokens.append(Token(TokenType.ASTERISK, line[i], line_num, col))
                    else:
                        tokens.append(Token(self.operators[line[i]], line[i], line_num, col))
                    i += 1
                    col += 1
                    continue
                
                # Punctuation
                if line[i] in self.punctuation:
                    tokens.append(Token(self.punctuation[line[i]], line[i], line_num, col))
                    i += 1
                    col += 1
                    continue
                
                # Quoted identifiers (backticks, square brackets, double quotes)
                if line[i] in ('`', '"') or (line[i] == '['):
                    quote_char = line[i]
                    end_char = ']' if quote_char == '[' else quote_char
                    start = i
                    i += 1
                    
                    while i < len(line) and line[i] != end_char:
                        i += 1
                    
                    if i < len(line):
                        i += 1  # Include closing quote
                    
                    value = line[start+1:i-1]  # Extract content without quotes
                    tokens.append(Token(TokenType.QUOTED_IDENTIFIER, value, line_num, col, line[start:i]))
                    col += (i - start)
                    continue
                
                # String literals
                if line[i] == "'":
                    start = i
                    i += 1
                    
                    while i < len(line):
                        if line[i] == "'":
                            # Check for escaped quote
                            if i + 1 < len(line) and line[i + 1] == "'":
                                i += 2  # Skip escaped quote
                            else:
                                i += 1  # Include closing quote
                                break
                        else:
                            i += 1
                    
                    value = line[start:i]
                    tokens.append(Token(TokenType.STRING, value, line_num, col))
                    col += len(value)
                    continue
                
                # Numbers (including decimals and scientific notation)
                if line[i].isdigit() or (line[i] == '.' and i + 1 < len(line) and line[i + 1].isdigit()):
                    start = i
                    
                    # Handle integer part
                    while i < len(line) and line[i].isdigit():
                        i += 1
                    
                    # Handle decimal part
                    if i < len(line) and line[i] == '.':
                        i += 1
                        while i < len(line) and line[i].isdigit():
                            i += 1
                    
                    # Handle scientific notation
                    if i < len(line) and line[i].lower() == 'e':
                        i += 1
                        if i < len(line) and line[i] in '+-':
                            i += 1
                        while i < len(line) and line[i].isdigit():
                            i += 1
                    
                    value = line[start:i]
                    tokens.append(Token(TokenType.NUMBER, value, line_num, col))
                    col += len(value)
                    continue
                
                # Identifiers and keywords
                if line[i].isalpha() or line[i] == '_':
                    start = i
                    
                    while i < len(line) and (line[i].isalnum() or line[i] in '_$#'):
                        i += 1
                    
                    value = line[start:i]
                    
                    # Check if it's a keyword
                    token_type = self.keywords.get(value.upper(), TokenType.IDENTIFIER)
                    tokens.append(Token(token_type, value, line_num, col))
                    col += len(value)
                    continue
                
                # Handle special characters that might be part of identifiers in some dialects
                if line[i] in '@$#':
                    start = i
                    i += 1
                    
                    # Continue reading alphanumeric characters
                    while i < len(line) and (line[i].isalnum() or line[i] == '_'):
                        i += 1
                    
                    value = line[start:i]
                    tokens.append(Token(TokenType.IDENTIFIER, value, line_num, col))
                    col += len(value)
                    continue
                
                # Unknown character - try to handle gracefully
                tokens.append(Token(TokenType.IDENTIFIER, line[i], line_num, col))
                i += 1
                col += 1
        
        return tokens
    
    def is_keyword(self, word: str) -> bool:
        """Check if a word is a SQL keyword"""
        return word.upper() in self.keywords
    
    def get_token_type(self, word: str) -> TokenType:
        """Get token type for a word"""
        return self.keywords.get(word.upper(), TokenType.IDENTIFIER)