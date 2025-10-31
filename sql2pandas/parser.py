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
    # name holds the expression (identifier, function call, or '*')
    name: str
    alias: Optional[str] = None

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
    right: Union[str, int, float, 'SelectStatement']
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
        
        # Check for SELECT *
        if self.current_token() and self.current_token().type == TokenType.ASTERISK:
            self.consume(TokenType.ASTERISK)
            columns.append(Column("*"))
            return columns
        # Parse one or more comma-separated column expressions
        while True:
            col = self.parse_column_expression()
            columns.append(col)
            # If comma, consume and continue
            if self.current_token() and self.current_token().type == TokenType.COMMA:
                self.consume(TokenType.COMMA)
                continue
            break
        
        return columns

    def parse_column_expression(self) -> Column:
        """Parses a column expression which can be:
        - identifier
        - function call like AVG(age)
        - identifier AS alias
        """
        # Function call: IDENTIFIER LPAREN args RPAREN
        token = self.current_token()
        if token.type == TokenType.IDENTIFIER and self.peek_token() and self.peek_token().type == TokenType.LPAREN:
            func_name = self.consume(TokenType.IDENTIFIER).value
            self.consume(TokenType.LPAREN)
            # For now, only simple single-identifier args or *
            args = []
            while self.current_token() and self.current_token().type != TokenType.RPAREN:
                if self.current_token().type == TokenType.COMMA:
                    self.consume(TokenType.COMMA)
                    continue
                if self.current_token().type == TokenType.ASTERISK:
                    args.append(self.consume(TokenType.ASTERISK).value)
                else:
                    arg = self.consume(TokenType.IDENTIFIER).value
                    args.append(arg)
            self.consume(TokenType.RPAREN)
            expr = f"{func_name}({', '.join(args)})"
        else:
            # Simple identifier
            expr = self.consume(TokenType.IDENTIFIER).value

        # Optional alias: AS alias or just alias
        alias = None
        if self.current_token() and self.current_token().type == TokenType.IDENTIFIER and self.current_token().value.upper() == 'AS':
            # consume AS then alias
            self.consume(TokenType.IDENTIFIER)
            alias = self.consume(TokenType.IDENTIFIER).value
        elif self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
            # treat bare identifier after expression as alias (common SQL)
            # but only if previous token was a function or identifier and next token is comma/FROM
            # peek next
            next_t = self.peek_token()
            if next_t and (next_t.type == TokenType.COMMA or next_t.type == TokenType.FROM or next_t.type == TokenType.RPAREN):
                alias = self.consume(TokenType.IDENTIFIER).value

        return Column(expr, alias)
    
    def parse_where_clause(self) -> WhereClause:
        self.consume(TokenType.WHERE)
        condition = self.parse_condition()
        return WhereClause(condition)
    
    def consume_any(self, types: List[TokenType]) -> Token:
        token = self.current_token()
        if token.type not in types:
            raise ParseError(f"Expected one of {types}, got {token.type}")
        self.current += 1
        return token

    def parse_condition(self) -> Condition:
        # Parse either a parenthesized condition, a simple comparison,
        # or a comparison whose right-hand side is a subquery.

        # Parenthesized condition
        if self.current_token() and self.current_token().type == TokenType.LPAREN:
            self.consume(TokenType.LPAREN)
            # If this is a subquery (SELECT ...), let parse_select_statement handle it
            if self.current_token() and self.current_token().type == TokenType.SELECT:
                subquery = self.parse_select_statement()
                # After parsing subquery we expect a closing parenthesis
                self.consume(TokenType.RPAREN)
                # Return a special Condition with left=None to indicate a subquery expression
                # but to stay compatible with existing structure, place the SelectStatement in right
                # and leave left/operator empty strings
                condition = Condition(left='', operator='', right=subquery)
            else:
                # Regular parenthesized boolean expression
                condition = self.parse_condition()
                self.consume(TokenType.RPAREN)
        else:
            # Parse basic comparison: IDENTIFIER OP VALUE (or subquery on RHS)
            left_token = self.consume(TokenType.IDENTIFIER)
            left = left_token.value

            op_token = self.consume_any([TokenType.EQUALS, TokenType.NOT_EQUALS,
                                        TokenType.LESS_THAN, TokenType.GREATER_THAN,
                                        TokenType.LESS_EQUAL, TokenType.GREATER_EQUAL])
            operator = op_token.value

            # If RHS is a parenthesized expression, it can be a subquery or nested expression
            if self.current_token() and self.current_token().type == TokenType.LPAREN:
                self.consume(TokenType.LPAREN)
                if self.current_token() and self.current_token().type == TokenType.SELECT:
                    # Subquery as RHS
                    subquery = self.parse_select_statement()
                    self.consume(TokenType.RPAREN)
                    right = subquery
                else:
                    # Parenthesized expression (treat as nested condition)
                    nested_cond = self.parse_condition()
                    self.consume(TokenType.RPAREN)
                    right = nested_cond
            else:
                right_token = self.consume_any([TokenType.STRING, TokenType.NUMBER, TokenType.IDENTIFIER])
                if right_token.type == TokenType.NUMBER:
                    right = float(right_token.value) if '.' in right_token.value else int(right_token.value)
                elif right_token.type == TokenType.STRING:
                    right = right_token.value.strip('"\'')
                else:  # IDENTIFIER
                    right = right_token.value

            condition = Condition(left, operator, right)

        # Handle chained AND/OR operations
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