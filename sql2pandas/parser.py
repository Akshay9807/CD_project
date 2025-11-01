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
    from_clause: 'FromClause'
    where_clause: Optional['WhereClause'] = None
    group_by_clause: Optional['GroupByClause'] = None
    having_clause: Optional['HavingClause'] = None
    order_clause: Optional['OrderClause'] = None
    limit_clause: Optional['LimitClause'] = None
    distinct: bool = False

@dataclass
class Column(ASTNode):
    name: str
    alias: Optional[str] = None
    function: Optional[str] = None  # COUNT, SUM, etc.
    table_alias: Optional[str] = None

@dataclass
class FromClause(ASTNode):
    table: str
    alias: Optional[str] = None
    joins: Optional[List['JoinClause']] = None

@dataclass
class JoinClause(ASTNode):
    join_type: str  # INNER, LEFT, RIGHT, FULL
    table: str
    on_condition: 'Condition'
    alias: Optional[str] = None

@dataclass
class WhereClause(ASTNode):
    condition: 'Condition'

@dataclass
class GroupByClause(ASTNode):
    columns: List[str]

@dataclass
class HavingClause(ASTNode):
    condition: 'Condition'

@dataclass
class OrderClause(ASTNode):
    columns: List['OrderColumn']

@dataclass
class OrderColumn(ASTNode):
    column: str
    direction: str = "ASC"

@dataclass
class LimitClause(ASTNode):
    count: int
    offset: Optional[int] = None

@dataclass
class Condition(ASTNode):
    left: Union[str, 'Condition']
    operator: str
    right: Union[str, int, float, List, 'Condition']
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
        # SELECT [DISTINCT]
        self.consume(TokenType.SELECT)
        
        distinct = False
        if self.current_token() and self.current_token().type == TokenType.DISTINCT:
            distinct = True
            self.consume(TokenType.DISTINCT)
        
        # Column list
        columns = self.parse_column_list()
        
        # FROM clause
        from_clause = self.parse_from_clause()
        
        # Optional WHERE clause
        where_clause = None
        if self.current_token() and self.current_token().type == TokenType.WHERE:
            where_clause = self.parse_where_clause()
        
        # Optional GROUP BY clause
        group_by_clause = None
        if self.current_token() and self.current_token().type == TokenType.GROUP:
            group_by_clause = self.parse_group_by_clause()
        
        # Optional HAVING clause
        having_clause = None
        if self.current_token() and self.current_token().type == TokenType.HAVING:
            having_clause = self.parse_having_clause()
        
        # Optional ORDER BY clause
        order_clause = None
        if self.current_token() and self.current_token().type == TokenType.ORDER:
            order_clause = self.parse_order_clause()
        
        # Optional LIMIT clause
        limit_clause = None
        if self.current_token() and self.current_token().type == TokenType.LIMIT:
            limit_clause = self.parse_limit_clause()
        
        return SelectStatement(
            columns=columns,
            from_clause=from_clause,
            where_clause=where_clause,
            group_by_clause=group_by_clause,
            having_clause=having_clause,
            order_clause=order_clause,
            limit_clause=limit_clause,
            distinct=distinct
        )
    
    def parse_column_list(self) -> List[Column]:
        columns = []
        
        # Check for SELECT *
        if self.current_token() and self.current_token().type == TokenType.ASTERISK:
            self.consume(TokenType.ASTERISK)
            columns.append(Column("*"))
            return columns
        
        # First column (could be function or regular column)
        columns.append(self.parse_column())
        
        # Additional columns
        while self.current_token() and self.current_token().type == TokenType.COMMA:
            self.consume(TokenType.COMMA)
            columns.append(self.parse_column())
        
        return columns
    
    def parse_column(self) -> Column:
        """Parse a single column which could be a function, regular column, or expression"""
        # Check for aggregate functions
        if self.current_token() and self.current_token().type in [
            TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN
        ]:
            function_name = self.consume().value
            self.consume(TokenType.LPAREN)
            
            # Handle COUNT(*) or COUNT(column)
            if self.current_token().type == TokenType.ASTERISK:
                column_name = "*"
                self.consume(TokenType.ASTERISK)
            else:
                column_name = self.consume(TokenType.IDENTIFIER).value
            
            self.consume(TokenType.RPAREN)
            
            # Check for alias
            alias = None
            if (self.current_token() and 
                self.current_token().type == TokenType.IDENTIFIER and 
                self.current_token().value.upper() not in ['FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT']):
                alias = self.consume(TokenType.IDENTIFIER).value
            
            return Column(name=column_name, function=function_name, alias=alias)
        
        # Regular column
        token = self.consume(TokenType.IDENTIFIER)
        column_name = token.value
        
        # Check for table.column format
        table_alias = None
        if self.current_token() and self.current_token().value == '.':
            table_alias = column_name
            self.consume()  # consume '.'
            column_name = self.consume(TokenType.IDENTIFIER).value
        
        # Check for alias
        alias = None
        if (self.current_token() and 
            self.current_token().type == TokenType.IDENTIFIER and 
            self.current_token().value.upper() not in ['FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT']):
            alias = self.consume(TokenType.IDENTIFIER).value
        
        return Column(name=column_name, table_alias=table_alias, alias=alias)
    
    def parse_from_clause(self) -> FromClause:
        """Parse FROM clause with optional JOINs"""
        self.consume(TokenType.FROM)
        
        # Main table
        table_token = self.consume(TokenType.IDENTIFIER)
        table_name = table_token.value
        
        # Optional table alias
        table_alias = None
        if (self.current_token() and 
            self.current_token().type == TokenType.IDENTIFIER and
            self.current_token().value.upper() not in ['WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'GROUP', 'ORDER', 'HAVING', 'LIMIT']):
            table_alias = self.consume(TokenType.IDENTIFIER).value
        
        # Parse JOINs
        joins = []
        while self.current_token() and self.current_token().type in [TokenType.JOIN, TokenType.INNER, TokenType.LEFT, TokenType.RIGHT]:
            joins.append(self.parse_join_clause())
        
        return FromClause(table=table_name, alias=table_alias, joins=joins if joins else None)
    
    def parse_join_clause(self) -> JoinClause:
        """Parse JOIN clause"""
        join_type = "INNER"  # default
        
        # Parse join type
        if self.current_token().type in [TokenType.INNER, TokenType.LEFT, TokenType.RIGHT]:
            join_type = self.consume().value
            if self.current_token() and self.current_token().type == TokenType.OUTER:
                self.consume()  # consume OUTER
                join_type += " OUTER"
        
        # Consume JOIN keyword
        self.consume(TokenType.JOIN)
        
        # Table name
        table_name = self.consume(TokenType.IDENTIFIER).value
        
        # Optional alias
        table_alias = None
        if (self.current_token() and 
            self.current_token().type == TokenType.IDENTIFIER and
            self.current_token().value.upper() != 'ON'):
            table_alias = self.consume(TokenType.IDENTIFIER).value
        
        # ON condition
        self.consume(TokenType.ON)
        on_condition = self.parse_condition()
        
        return JoinClause(join_type=join_type, table=table_name, alias=table_alias, on_condition=on_condition)
    
    def parse_group_by_clause(self) -> GroupByClause:
        """Parse GROUP BY clause"""
        self.consume(TokenType.GROUP)
        self.consume(TokenType.BY)
        
        columns = []
        columns.append(self.consume(TokenType.IDENTIFIER).value)
        
        while self.current_token() and self.current_token().type == TokenType.COMMA:
            self.consume(TokenType.COMMA)
            columns.append(self.consume(TokenType.IDENTIFIER).value)
        
        return GroupByClause(columns=columns)
    
    def parse_having_clause(self) -> HavingClause:
        """Parse HAVING clause"""
        self.consume(TokenType.HAVING)
        condition = self.parse_condition()
        return HavingClause(condition=condition)
    
    def parse_limit_clause(self) -> LimitClause:
        """Parse LIMIT clause"""
        self.consume(TokenType.LIMIT)
        count = int(self.consume(TokenType.NUMBER).value)
        
        offset = None
        if self.current_token() and self.current_token().type == TokenType.OFFSET:
            self.consume(TokenType.OFFSET)
            offset = int(self.consume(TokenType.NUMBER).value)
        
        return LimitClause(count=count, offset=offset)
    
    def parse_where_clause(self) -> WhereClause:
        self.consume(TokenType.WHERE)
        condition = self.parse_condition()
        return WhereClause(condition)
    
    def parse_condition(self) -> Condition:
        """Parse condition with support for complex expressions"""
        return self.parse_or_condition()
    
    def parse_or_condition(self) -> Condition:
        """Parse OR conditions (lowest precedence)"""
        condition = self.parse_and_condition()
        
        while self.current_token() and self.current_token().type == TokenType.OR:
            logical_op = self.consume().value
            next_condition = self.parse_and_condition()
            condition = Condition(condition, logical_op, next_condition)
        
        return condition
    
    def parse_and_condition(self) -> Condition:
        """Parse AND conditions (higher precedence than OR)"""
        condition = self.parse_basic_condition()
        
        while self.current_token() and self.current_token().type == TokenType.AND:
            logical_op = self.consume().value
            next_condition = self.parse_basic_condition()
            condition = Condition(condition, logical_op, next_condition)
        
        return condition
    
    def parse_basic_condition(self) -> Condition:
        """Parse basic comparison conditions"""
        # Handle NOT
        is_not = False
        if self.current_token() and self.current_token().type == TokenType.NOT:
            is_not = True
            self.consume(TokenType.NOT)
        
        # Handle parentheses
        if self.current_token() and self.current_token().type == TokenType.LPAREN:
            self.consume(TokenType.LPAREN)
            condition = self.parse_condition()
            self.consume(TokenType.RPAREN)
            return condition
        
        # Left operand (column name, aggregate function, or table alias)
        if self.current_token().type in [TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN]:
            # Handle aggregate functions in conditions (e.g., HAVING COUNT(*) > 1)
            func_name = self.consume().value
            self.consume(TokenType.LPAREN)
            if self.current_token().type == TokenType.ASTERISK:
                left = f"{func_name}(*)"
                self.consume(TokenType.ASTERISK)
            else:
                col_name = self.consume(TokenType.IDENTIFIER).value
                left = f"{func_name}({col_name})"
            self.consume(TokenType.RPAREN)
        else:
            left_token = self.consume(TokenType.IDENTIFIER)
            left = left_token.value
        
        # Handle table.column format
        if self.current_token() and self.current_token().value == '.':
            self.consume()  # consume '.'
            column_token = self.consume(TokenType.IDENTIFIER)
            left = f"{left}.{column_token.value}"
        
        # Handle different operators
        op_token = self.current_token()
        
        if op_token.type == TokenType.IN:
            operator = "NOT IN" if is_not else "IN"
            self.consume(TokenType.IN)
            self.consume(TokenType.LPAREN)
            
            # Parse list of values
            values = []
            values.append(self.parse_value())
            
            while self.current_token() and self.current_token().type == TokenType.COMMA:
                self.consume(TokenType.COMMA)
                values.append(self.parse_value())
            
            self.consume(TokenType.RPAREN)
            return Condition(left, operator, values)
        
        elif op_token.type == TokenType.LIKE:
            operator = "NOT LIKE" if is_not else "LIKE"
            self.consume(TokenType.LIKE)
            right = self.parse_value()
            return Condition(left, operator, right)
        
        elif op_token.type == TokenType.BETWEEN:
            operator = "NOT BETWEEN" if is_not else "BETWEEN"
            self.consume(TokenType.BETWEEN)
            start_value = self.parse_value()
            self.consume(TokenType.AND)
            end_value = self.parse_value()
            return Condition(left, operator, [start_value, end_value])
        
        elif op_token.type == TokenType.IS:
            self.consume(TokenType.IS)
            if self.current_token() and self.current_token().type == TokenType.NOT:
                self.consume(TokenType.NOT)
                operator = "IS NOT NULL"
            else:
                operator = "IS NULL"
            self.consume(TokenType.NULL)
            return Condition(left, operator, None)
        
        elif op_token.type in [TokenType.EQUALS, TokenType.NOT_EQUALS, 
                              TokenType.LESS_THAN, TokenType.GREATER_THAN,
                              TokenType.LESS_EQUAL, TokenType.GREATER_EQUAL]:
            operator = op_token.value
            if is_not and operator == "=":
                operator = "!="
            self.consume()
            right = self.parse_value()
            return Condition(left, operator, right)
        
        else:
            raise ParseError(f"Expected comparison operator, got {op_token.type}")
    
    def parse_value(self):
        """Parse a value (number, string, or identifier)"""
        token = self.current_token()
        
        if token.type == TokenType.NUMBER:
            value = float(token.value) if '.' in token.value else int(token.value)
        elif token.type == TokenType.STRING:
            value = token.value.strip('"\'')
        elif token.type == TokenType.IDENTIFIER:
            value = token.value
            # Handle table.column format
            if self.peek_token() and self.peek_token().value == '.':
                self.consume()  # consume identifier
                self.consume()  # consume '.'
                column_token = self.consume(TokenType.IDENTIFIER)
                return f"{value}.{column_token.value}"
        else:
            raise ParseError(f"Expected value, got {token.type}")
        
        self.consume()
        return value
    
    def parse_order_clause(self) -> OrderClause:
        self.consume(TokenType.ORDER)
        self.consume(TokenType.BY)
        
        columns = []
        
        # First column
        column_name = self.consume(TokenType.IDENTIFIER).value
        direction = "ASC"
        if self.current_token() and self.current_token().type in [TokenType.ASC, TokenType.DESC]:
            direction = self.consume().value
        columns.append(OrderColumn(column=column_name, direction=direction))
        
        # Additional columns
        while self.current_token() and self.current_token().type == TokenType.COMMA:
            self.consume(TokenType.COMMA)
            column_name = self.consume(TokenType.IDENTIFIER).value
            direction = "ASC"
            if self.current_token() and self.current_token().type in [TokenType.ASC, TokenType.DESC]:
                direction = self.consume().value
            columns.append(OrderColumn(column=column_name, direction=direction))
        
        return OrderClause(columns=columns)