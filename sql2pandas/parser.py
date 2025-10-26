from dataclasses import dataclass
from typing import List, Optional, Union
from lexer import Token, TokenType

# AST Node classes
@dataclass
class ASTNode:
    pass

@dataclass
class SelectStatement(ASTNode):
    columns: List['Column']
    table: str
    where_clause: Optional['WhereClause'] = None
    order_clause: Optional['OrderClause'] = None

@dataclass
class Column(ASTNode):
    name: str

@dataclass
class WhereClause(ASTNode):
    condition: 'Condition'

@dataclass
class OrderClause(ASTNode):
    column: str
    direction: str = "ASC"  # ASC or DESC

@dataclass
class Condition(ASTNode):
    left: str
    operator: str
    right: Union[str, int, float]
    logical_op: Optional[str] = None  # AND, OR
    next_condition: Optional['Condition'] = None

class ParseError(Exception):
    pass

class SQLParser:
    def __init__(self):
        self.tokens = []
        self.current = 0
    
    def parse(self, tokens: List[Token]) -> SelectStatement:
        self.tokens = [t for t in tokens if t.type != TokenType.WHITESPACE]
        self.current = 0
        
        if not self.tokens:
            raise ParseError("Empty query")
        
        return self.parse_select_statement()
    
    def current_token(self) -> Optional[Token]:
        if self.current >= len(self.tokens):
            return None
        return self.tokens[self.current]
    
    def peek_token(self) -> Optional[Token]:
        if self.current + 1 >= len(self.tokens):
            return None
        return self.tokens[self.current + 1]
    
    def consume(self, expected_type: TokenType = None) -> Token:
        if self.current >= len(self.tokens):
            raise ParseError("Unexpected end of input")
        
        token = self.tokens[self.current]
        if expected_type and token.type != expected_type:
            raise ParseError(f"Expected {expected_type}, got {token.type}")
        
        self.current += 1
        return token
    
    def parse_select_statement(self) -> SelectStatement:
        # SELECT
        self.consume(TokenType.SELECT)
        
        # Column list
        columns = self.parse_column_list()
        
        # FROM
        self.consume(TokenType.FROM)
        table_token = self.consume(TokenType.IDENTIFIER)
        table = table_token.value
        
        # Optional WHERE clause
        where_clause = None
        if self.current_token() and self.current_token().type == TokenType.WHERE:
            where_clause = self.parse_where_clause()
        
        # Optional ORDER BY clause
        order_clause = None
        if self.current_token() and self.current_token().type == TokenType.ORDER:
            order_clause = self.parse_order_clause()
        
        return SelectStatement(columns, table, where_clause, order_clause)
    
    def parse_column_list(self) -> List[Column]:
        columns = []
        
        # First column
        token = self.consume(TokenType.IDENTIFIER)
        columns.append(Column(token.value))
        
        # Additional columns
        while self.current_token() and self.current_token().type == TokenType.COMMA:
            self.consume(TokenType.COMMA)
            token = self.consume(TokenType.IDENTIFIER)
            columns.append(Column(token.value))
        
        return columns
    
    def parse_where_clause(self) -> WhereClause:
        self.consume(TokenType.WHERE)
        condition = self.parse_condition()
        return WhereClause(condition)
    
    def parse_condition(self) -> Condition:
        # Left operand
        left_token = self.consume(TokenType.IDENTIFIER)
        left = left_token.value
        
        # Operator
        op_token = self.current_token()
        if op_token.type not in [TokenType.EQUALS, TokenType.NOT_EQUALS, 
                                TokenType.LESS_THAN, TokenType.GREATER_THAN,
                                TokenType.LESS_EQUAL, TokenType.GREATER_EQUAL]:
            raise ParseError(f"Expected comparison operator, got {op_token.type}")
        
        operator = op_token.value
        self.consume()
        
        # Right operand
        right_token = self.current_token()
        if right_token.type == TokenType.NUMBER:
            right = float(right_token.value) if '.' in right_token.value else int(right_token.value)
        elif right_token.type == TokenType.STRING:
            right = right_token.value.strip('"\'')
        elif right_token.type == TokenType.IDENTIFIER:
            right = right_token.value
        else:
            raise ParseError(f"Expected value, got {right_token.type}")
        
        self.consume()
        
        condition = Condition(left, operator, right)
        
        # Check for logical operators (AND, OR)
        if self.current_token() and self.current_token().type in [TokenType.AND, TokenType.OR]:
            logical_op = self.consume().value
            next_condition = self.parse_condition()
            condition.logical_op = logical_op
            condition.next_condition = next_condition
        
        return condition
    
    def parse_order_clause(self) -> OrderClause:
        self.consume(TokenType.ORDER)
        self.consume(TokenType.BY)
        
        column_token = self.consume(TokenType.IDENTIFIER)
        column = column_token.value
        
        direction = "ASC"
        if self.current_token() and self.current_token().type in [TokenType.ASC, TokenType.DESC]:
            direction = self.consume().value
        
        return OrderClause(column, direction)