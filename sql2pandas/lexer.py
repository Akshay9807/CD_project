import re
from dataclasses import dataclass
from typing import List, Iterator
from enum import Enum

class TokenType(Enum):
    # Keywords
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
    ON = "ON"
    
    # Conditional keywords
    IN = "IN"
    LIKE = "LIKE"
    BETWEEN = "BETWEEN"
    IS = "IS"
    NULL = "NULL"
    
    # Aggregate and functions
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"
    DISTINCT = "DISTINCT"
    
    # Grouping and having
    GROUP = "GROUP"
    HAVING = "HAVING"
    
    # Limit and offset
    LIMIT = "LIMIT"
    OFFSET = "OFFSET"
    TOP = "TOP"
    
    # Subquery keywords
    EXISTS = "EXISTS"
    ANY = "ANY"
    
    # Case statement
    CASE = "CASE"
    WHEN = "WHEN"
    THEN = "THEN"
    ELSE = "ELSE"
    END = "END"
    
    # Mathematical functions
    ROUND = "ROUND"
    FLOOR = "FLOOR"
    CEIL = "CEIL"
    ABS = "ABS"
    
    # String functions
    UPPER = "UPPER"
    LOWER = "LOWER"
    CONCAT = "CONCAT"
    LENGTH = "LENGTH"
    SUBSTRING = "SUBSTRING"
    
    # Set operations
    UNION = "UNION"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"
    ALL = "ALL"
    
    # Insert, Update, Delete (for future expansion)
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    INTO = "INTO"
    VALUES = "VALUES"
    SET = "SET"
    
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
    
    # Literals
    IDENTIFIER = "IDENTIFIER"
    NUMBER = "NUMBER"
    STRING = "STRING"
    
    # Punctuation
    COMMA = ","
    SEMICOLON = ";"
    LPAREN = "("
    RPAREN = ")"
    ASTERISK = "*"
    DOT = "."
    
    # Special
    WHITESPACE = "WHITESPACE"
    EOF = "EOF"

@dataclass
class Token:
    type: TokenType
    value: str
    lineno: int
    column: int

class SQLLexer:
    def __init__(self):
        self.keywords = {
            'SELECT': TokenType.SELECT,
            'FROM': TokenType.FROM,
            'WHERE': TokenType.WHERE,
            'ORDER': TokenType.ORDER,
            'BY': TokenType.BY,
            'ASC': TokenType.ASC,
            'DESC': TokenType.DESC,
            'AND': TokenType.AND,
            'OR': TokenType.OR,
            'NOT': TokenType.NOT,
            
            # JOIN keywords
            'JOIN': TokenType.JOIN,
            'INNER': TokenType.INNER,
            'LEFT': TokenType.LEFT,
            'RIGHT': TokenType.RIGHT,
            'FULL': TokenType.FULL,
            'OUTER': TokenType.OUTER,
            'CROSS': TokenType.CROSS,
            'ON': TokenType.ON,
            
            # Conditional keywords
            'IN': TokenType.IN,
            'LIKE': TokenType.LIKE,
            'BETWEEN': TokenType.BETWEEN,
            'IS': TokenType.IS,
            'NULL': TokenType.NULL,
            
            # Aggregate functions
            'COUNT': TokenType.COUNT,
            'SUM': TokenType.SUM,
            'AVG': TokenType.AVG,
            'MAX': TokenType.MAX,
            'MIN': TokenType.MIN,
            'DISTINCT': TokenType.DISTINCT,
            
            # Grouping
            'GROUP': TokenType.GROUP,
            'HAVING': TokenType.HAVING,
            
            # Limit
            'LIMIT': TokenType.LIMIT,
            'OFFSET': TokenType.OFFSET,
            'TOP': TokenType.TOP,
            
            # Subquery
            'EXISTS': TokenType.EXISTS,
            'ANY': TokenType.ANY,
            
            # Case
            'CASE': TokenType.CASE,
            'WHEN': TokenType.WHEN,
            'THEN': TokenType.THEN,
            'ELSE': TokenType.ELSE,
            'END': TokenType.END,
            
            # Mathematical functions
            'ROUND': TokenType.ROUND,
            'FLOOR': TokenType.FLOOR,
            'CEIL': TokenType.CEIL,
            'ABS': TokenType.ABS,
            
            # String functions
            'UPPER': TokenType.UPPER,
            'LOWER': TokenType.LOWER,
            'CONCAT': TokenType.CONCAT,
            'LENGTH': TokenType.LENGTH,
            'SUBSTRING': TokenType.SUBSTRING,
            
            # Set operations
            'UNION': TokenType.UNION,
            'INTERSECT': TokenType.INTERSECT,
            'EXCEPT': TokenType.EXCEPT,
            'ALL': TokenType.ALL,
            
            # DML (for future)
            'INSERT': TokenType.INSERT,
            'UPDATE': TokenType.UPDATE,
            'DELETE': TokenType.DELETE,
            'INTO': TokenType.INTO,
            'VALUES': TokenType.VALUES,
            'SET': TokenType.SET,
        }
        
        self.operators = {
            '=': TokenType.EQUALS,
            '!=': TokenType.NOT_EQUALS,
            '<>': TokenType.NOT_EQUALS,
            '<': TokenType.LESS_THAN,
            '>': TokenType.GREATER_THAN,
            '<=': TokenType.LESS_EQUAL,
            '>=': TokenType.GREATER_EQUAL,
            '+': TokenType.PLUS,
            '-': TokenType.MINUS,
            '/': TokenType.DIVIDE,
        }
        
        self.punctuation = {
            ',': TokenType.COMMA,
            ';': TokenType.SEMICOLON,
            '(': TokenType.LPAREN,
            ')': TokenType.RPAREN,
            '*': TokenType.ASTERISK,  # Also serves as MULTIPLY
            '.': TokenType.DOT,
        }
    
    def tokenize(self, text: str) -> List[Token]:
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
                
                # Multi-character operators
                if i < len(line) - 1:
                    two_char = line[i:i+2]
                    if two_char in self.operators:
                        tokens.append(Token(self.operators[two_char], two_char, line_num, col))
                        i += 2
                        col += 2
                        continue
                
                # Single character operators and punctuation
                if line[i] in self.operators:
                    tokens.append(Token(self.operators[line[i]], line[i], line_num, col))
                    i += 1
                    col += 1
                    continue
                
                if line[i] in self.punctuation:
                    tokens.append(Token(self.punctuation[line[i]], line[i], line_num, col))
                    i += 1
                    col += 1
                    continue
                
                # String literals
                if line[i] in ('"', "'"):
                    quote = line[i]
                    start = i
                    i += 1
                    while i < len(line) and line[i] != quote:
                        i += 1
                    if i < len(line):
                        i += 1  # Include closing quote
                    value = line[start:i]
                    tokens.append(Token(TokenType.STRING, value, line_num, col))
                    col += len(value)
                    continue
                
                # Numbers
                if line[i].isdigit():
                    start = i
                    while i < len(line) and (line[i].isdigit() or line[i] == '.'):
                        i += 1
                    value = line[start:i]
                    tokens.append(Token(TokenType.NUMBER, value, line_num, col))
                    col += len(value)
                    continue
                
                # Identifiers and keywords
                if line[i].isalpha() or line[i] == '_':
                    start = i
                    while i < len(line) and (line[i].isalnum() or line[i] == '_'):
                        i += 1
                    value = line[start:i]
                    
                    # Check if it's a keyword
                    token_type = self.keywords.get(value.upper(), TokenType.IDENTIFIER)
                    tokens.append(Token(token_type, value, line_num, col))
                    col += len(value)
                    continue
                
                # Unknown character
                raise ValueError(f"Unexpected character '{line[i]}' at line {line_num}, column {col}")
        
        return tokens