from dataclasses import dataclass
from typing import List, Optional, Union, Any
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
    set_operations: Optional[List['SetOperation']] = None

@dataclass
class SetOperation(ASTNode):
    operation_type: str  # UNION, INTERSECT, EXCEPT
    all_flag: bool = False  # For UNION ALL, etc.
    right_query: 'SelectStatement' = None

@dataclass
class Column(ASTNode):
    name: str
    alias: Optional[str] = None
    function: Optional[str] = None  # COUNT, SUM, etc.
    table_alias: Optional[str] = None
    expression: Optional['Expression'] = None  # For complex expressions

@dataclass
class Expression(ASTNode):
    type: str  # 'case', 'function', 'column', 'literal'
    value: Any = None
    arguments: Optional[List['Expression']] = None
    conditions: Optional[List['CaseCondition']] = None

@dataclass
class CaseCondition(ASTNode):
    when_expr: 'Expression'
    then_expr: 'Expression'

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
        
        return self.parse_query_with_set_operations()
    
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
    
    def parse_query_with_set_operations(self) -> SelectStatement:
        """Parse query with potential set operations (UNION, INTERSECT, EXCEPT)"""
        # Parse the first SELECT statement
        left_query = self.parse_select_statement()
        
        # Check for set operations
        set_operations = []
        while (self.current_token() and 
               self.current_token().type in [TokenType.UNION, TokenType.INTERSECT, TokenType.EXCEPT]):
            
            operation_type = self.consume().value.upper()
            
            # Check for ALL keyword
            all_flag = False
            if (self.current_token() and 
                (self.current_token().type == TokenType.ALL or 
                 (self.current_token().type == TokenType.IDENTIFIER and self.current_token().value.upper() == 'ALL'))):
                all_flag = True
                self.consume()
            
            # Parse the right SELECT statement
            right_query = self.parse_select_statement()
            
            set_operations.append(SetOperation(
                operation_type=operation_type,
                all_flag=all_flag,
                right_query=right_query
            ))
        
        # Add set operations to the main query
        if set_operations:
            left_query.set_operations = set_operations
        
        return left_query
    
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
        # Check for mathematical functions (ROUND, FLOOR, etc.)
        if self.current_token() and self.current_token().type in [
            TokenType.ROUND, TokenType.FLOOR, TokenType.CEIL, TokenType.ABS,
            TokenType.UPPER, TokenType.LOWER, TokenType.CONCAT, TokenType.LENGTH
        ]:
            return self.parse_function_column()
        
        # Check for aggregate functions
        elif self.current_token() and self.current_token().type in [
            TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN
        ]:
            return self.parse_aggregate_column()
        
        # Check for CASE expression
        elif self.current_token() and self.current_token().type == TokenType.CASE:
            return self.parse_case_column()
        
        # Regular column
        else:
            return self.parse_regular_column()
    
    def parse_function_column(self) -> Column:
        """Parse mathematical/string function columns like ROUND(AVG(...), 2)"""
        function_name = self.consume().value
        self.consume(TokenType.LPAREN)
        
        # Parse function arguments (could be nested functions or expressions)
        args = []
        if self.current_token().type != TokenType.RPAREN:
            args.append(self.parse_expression())
            
            while self.current_token() and self.current_token().type == TokenType.COMMA:
                self.consume(TokenType.COMMA)
                args.append(self.parse_expression())
        
        self.consume(TokenType.RPAREN)
        
        # Create expression for the function
        expr = Expression(type='function', value=function_name, arguments=args)
        
        # Check for alias
        alias = self.parse_alias()
        
        return Column(name=f"{function_name}(...)", expression=expr, alias=alias)
    
    def parse_aggregate_column(self) -> Column:
        """Parse aggregate function columns"""
        function_name = self.consume().value
        self.consume(TokenType.LPAREN)
        
        # Handle COUNT(*) or COUNT(column) or COUNT(DISTINCT column)
        column_name = "*"
        if self.current_token().type == TokenType.ASTERISK:
            column_name = "*"
            self.consume(TokenType.ASTERISK)
        elif self.current_token().type == TokenType.DISTINCT:
            self.consume(TokenType.DISTINCT)
            column_name = f"DISTINCT {self.consume(TokenType.IDENTIFIER).value}"
        else:
            # Could be a CASE expression or regular column
            if self.current_token().type == TokenType.CASE:
                case_expr = self.parse_case_expression()
                column_name = "CASE_EXPR"
                self.consume(TokenType.RPAREN)
                alias = self.parse_alias()
                expr = Expression(type='function', value=function_name, arguments=[case_expr])
                return Column(name=f"{function_name}(CASE)", expression=expr, alias=alias)
            else:
                # Handle table.column in functions
                col_token = self.consume(TokenType.IDENTIFIER)
                column_name = col_token.value
                if self.current_token() and self.current_token().type == TokenType.DOT:
                    self.consume(TokenType.DOT)
                    actual_col = self.consume(TokenType.IDENTIFIER).value
                    column_name = f"{column_name}.{actual_col}"
        
        self.consume(TokenType.RPAREN)
        alias = self.parse_alias()
        
        return Column(name=column_name, function=function_name, alias=alias)
    
    def parse_case_column(self) -> Column:
        """Parse CASE expression column"""
        case_expr = self.parse_case_expression()
        alias = self.parse_alias()
        
        return Column(name="CASE_EXPR", expression=case_expr, alias=alias)
    
    def parse_regular_column(self) -> Column:
        """Parse regular column"""
        token = self.consume(TokenType.IDENTIFIER)
        column_name = token.value
        
        # Check for table.column format
        table_alias = None
        if self.current_token() and self.current_token().type == TokenType.DOT:
            table_alias = column_name
            self.consume(TokenType.DOT)
            column_name = self.consume(TokenType.IDENTIFIER).value
        
        alias = self.parse_alias()
        return Column(name=column_name, table_alias=table_alias, alias=alias)
    
    def parse_alias(self) -> Optional[str]:
        """Parse column alias (with or without AS keyword)"""
        alias = None
        if self.current_token() and self.current_token().value.upper() == 'AS':
            self.consume()  # consume AS
            if self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
                alias = self.consume(TokenType.IDENTIFIER).value
        elif (self.current_token() and 
              self.current_token().type == TokenType.IDENTIFIER and 
              self.current_token().value.upper() not in ['FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'JOIN', 'INNER', 'LEFT', 'RIGHT']):
            alias = self.consume(TokenType.IDENTIFIER).value
        
        return alias
    
    def parse_expression(self) -> Expression:
        """Parse a general expression (function call, case, column, literal)"""
        if self.current_token().type == TokenType.CASE:
            return self.parse_case_expression()
        elif self.current_token().type in [TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN]:
            # Aggregate function
            func_name = self.consume().value
            self.consume(TokenType.LPAREN)
            
            if self.current_token().type == TokenType.ASTERISK:
                arg = Expression(type='literal', value='*')
                self.consume(TokenType.ASTERISK)
            else:
                arg = self.parse_expression()
            
            self.consume(TokenType.RPAREN)
            return Expression(type='function', value=func_name, arguments=[arg])
        elif self.current_token().type == TokenType.IDENTIFIER:
            # Column reference
            col_name = self.consume().value
            if self.current_token() and self.current_token().type == TokenType.DOT:
                self.consume(TokenType.DOT)
                actual_col = self.consume(TokenType.IDENTIFIER).value
                col_name = f"{col_name}.{actual_col}"
            return Expression(type='column', value=col_name)
        elif self.current_token().type in [TokenType.NUMBER, TokenType.STRING]:
            # Literal value
            value = self.consume().value
            return Expression(type='literal', value=value)
        else:
            raise ParseError(f"Unexpected token in expression: {self.current_token().type}")
    
    def parse_case_expression(self) -> Expression:
        """Parse CASE expression"""
        self.consume(TokenType.CASE)
        
        # Check if it's a simple CASE (CASE column WHEN value) or searched CASE (CASE WHEN condition)
        case_column = None
        if self.current_token().type == TokenType.IDENTIFIER:
            # Simple CASE - save the column being tested
            case_column = self.consume().value
        
        conditions = []
        
        # Parse WHEN clauses
        while self.current_token() and self.current_token().type == TokenType.WHEN:
            self.consume(TokenType.WHEN)
            
            # Parse WHEN condition/value
            when_expr = self.parse_expression()
            
            self.consume(TokenType.THEN)
            
            # Parse THEN value
            then_expr = self.parse_expression()
            
            conditions.append(CaseCondition(when_expr=when_expr, then_expr=then_expr))
        
        # Optional ELSE clause
        else_expr = None
        if self.current_token() and self.current_token().type == TokenType.ELSE:
            self.consume(TokenType.ELSE)
            else_expr = self.parse_expression()
        
        self.consume(TokenType.END)
        
        # Create CASE expression
        case_expr = Expression(type='case', value=case_column, conditions=conditions)
        if else_expr:
            case_expr.arguments = [else_expr]  # Store ELSE expression in arguments
        
        return case_expr
    
    def parse_from_clause(self) -> FromClause:
        """Parse FROM clause with optional JOINs"""
        self.consume(TokenType.FROM)
        
        # Main table
        table_token = self.consume(TokenType.IDENTIFIER)
        table_name = table_token.value
        
        # Optional table alias (with or without AS keyword)
        table_alias = None
        if self.current_token() and self.current_token().value.upper() == 'AS':
            self.consume()  # consume AS
            if self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
                table_alias = self.consume(TokenType.IDENTIFIER).value
        elif (self.current_token() and 
              self.current_token().type == TokenType.IDENTIFIER and
              self.current_token().value.upper() not in ['WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'GROUP', 'ORDER', 'HAVING', 'LIMIT']):
            table_alias = self.consume(TokenType.IDENTIFIER).value
        
        # Parse JOINs
        joins = []
        while (self.current_token() and 
               (self.current_token().type in [TokenType.JOIN, TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.FULL] or
                (self.current_token().type == TokenType.IDENTIFIER and self.current_token().value.upper() in ['JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL']))):
            joins.append(self.parse_join_clause())
        
        return FromClause(table=table_name, alias=table_alias, joins=joins if joins else None)
    
    def parse_join_clause(self) -> JoinClause:
        """Parse JOIN clause with better error handling"""
        join_type = "INNER"  # default
        
        # Parse join type - handle both token types and string values
        current = self.current_token()
        if current:
            if current.type in [TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.FULL, TokenType.CROSS]:
                join_type = self.consume().value.upper()
            elif current.type == TokenType.IDENTIFIER and current.value.upper() in ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS']:
                join_type = self.consume().value.upper()
            
            # Check for OUTER keyword (not applicable to CROSS JOIN)
            if join_type != "CROSS" and self.current_token() and (
                self.current_token().type == TokenType.OUTER or 
                (self.current_token().type == TokenType.IDENTIFIER and self.current_token().value.upper() == 'OUTER')
            ):
                self.consume()  # consume OUTER
                join_type += " OUTER"
        
        # Consume JOIN keyword
        if self.current_token() and self.current_token().type == TokenType.JOIN:
            self.consume(TokenType.JOIN)
        elif self.current_token() and self.current_token().type == TokenType.IDENTIFIER and self.current_token().value.upper() == 'JOIN':
            self.consume()
        else:
            raise ParseError(f"Expected JOIN keyword, got {self.current_token().type if self.current_token() else 'EOF'}")
        
        # Table name
        table_name = self.consume(TokenType.IDENTIFIER).value
        
        # Optional alias (with or without AS)
        table_alias = None
        if self.current_token() and self.current_token().value.upper() == 'AS':
            self.consume()  # consume AS
            if self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
                table_alias = self.consume(TokenType.IDENTIFIER).value
        elif (self.current_token() and 
              self.current_token().type == TokenType.IDENTIFIER and
              self.current_token().value.upper() != 'ON'):
            table_alias = self.consume(TokenType.IDENTIFIER).value
        
        # ON condition (not required for CROSS JOIN)
        on_condition = None
        if join_type == "CROSS":
            # CROSS JOIN doesn't have ON condition
            on_condition = None
        else:
            if not self.current_token() or self.current_token().value.upper() != 'ON':
                raise ParseError(f"Expected ON keyword in JOIN clause, got {self.current_token().value if self.current_token() else 'EOF'}")
            self.consume()  # consume ON
            on_condition = self.parse_condition()
        
        return JoinClause(join_type=join_type, table=table_name, on_condition=on_condition, alias=table_alias)
    
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
        if self.current_token() and self.current_token().type in [TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN]:
            # Handle aggregate functions in conditions (e.g., HAVING COUNT(*) > 1)
            func_name = self.consume().value
            self.consume(TokenType.LPAREN)
            if self.current_token() and self.current_token().type == TokenType.ASTERISK:
                left = f"{func_name}(*)"
                self.consume(TokenType.ASTERISK)
            elif self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
                col_name = self.consume(TokenType.IDENTIFIER).value
                left = f"{func_name}({col_name})"
            else:
                raise ParseError(f"Expected column name or * in {func_name} function")
            self.consume(TokenType.RPAREN)
        else:
            if not self.current_token():
                raise ParseError("Unexpected end of input in condition")
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