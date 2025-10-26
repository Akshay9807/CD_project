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
    
    # Operators
    EQUALS = "="
    NOT_EQUALS = "!="
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_EQUAL = "<="
    GREATER_EQUAL = ">="
    
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
        }
        
        self.operators = {
            '=': TokenType.EQUALS,
            '!=': TokenType.NOT_EQUALS,
            '<>': TokenType.NOT_EQUALS,
            '<': TokenType.LESS_THAN,
            '>': TokenType.GREATER_THAN,
            '<=': TokenType.LESS_EQUAL,
            '>=': TokenType.GREATER_EQUAL,
        }
        
        self.punctuation = {
            ',': TokenType.COMMA,
            ';': TokenType.SEMICOLON,
            '(': TokenType.LPAREN,
            ')': TokenType.RPAREN,
            '*': TokenType.ASTERISK,
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