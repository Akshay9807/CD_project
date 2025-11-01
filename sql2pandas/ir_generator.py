from typing import Dict, List, Any, Optional, Union
from parser import (SelectStatement, WhereClause, OrderClause, Condition, 
                   FromClause, JoinClause, GroupByClause, HavingClause, 
                   LimitClause, Column, OrderColumn)

class IRGenerator:
    """
    Generates Intermediate Representation (IR) from AST.
    IR is a JSON-like structure that represents the query operations.
    """
    
    def generate(self, ast: SelectStatement) -> Dict[str, Any]:
        """Generate IR from AST"""
        ir = {
            "operation": "select",
            "distinct": ast.distinct,
            "columns": self._process_columns(ast.columns),
            "from": self._process_from_clause(ast.from_clause),
            "filters": None,
            "group_by": None,
            "having": None,
            "ordering": None,
            "limit": None
        }
        
        if ast.where_clause:
            ir["filters"] = self._process_where_clause(ast.where_clause)
        
        if ast.group_by_clause:
            ir["group_by"] = self._process_group_by_clause(ast.group_by_clause)
        
        if ast.having_clause:
            ir["having"] = self._process_having_clause(ast.having_clause)
        
        if ast.order_clause:
            ir["ordering"] = self._process_order_clause(ast.order_clause)
        
        if ast.limit_clause:
            ir["limit"] = self._process_limit_clause(ast.limit_clause)
        
        return ir
    
    def _process_columns(self, columns: List[Column]) -> List[Dict[str, Any]]:
        """Process column list with functions and aliases"""
        processed_columns = []
        
        for col in columns:
            col_info = {
                "name": col.name,
                "alias": col.alias,
                "function": col.function,
                "table_alias": col.table_alias
            }
            processed_columns.append(col_info)
        
        return processed_columns
    
    def _process_from_clause(self, from_clause: FromClause) -> Dict[str, Any]:
        """Process FROM clause with JOINs"""
        from_info = {
            "table": from_clause.table,
            "alias": from_clause.alias,
            "joins": []
        }
        
        if from_clause.joins:
            for join in from_clause.joins:
                join_info = {
                    "type": join.join_type,
                    "table": join.table,
                    "alias": join.alias,
                    "on_condition": self._process_condition_recursive(join.on_condition)
                }
                from_info["joins"].append(join_info)
        
        return from_info
    
    def _process_where_clause(self, where_clause: WhereClause) -> Dict[str, Any]:
        """Process WHERE clause into filter conditions"""
        return self._process_condition(where_clause.condition)
    
    def _process_condition(self, condition: Condition) -> Dict[str, Any]:
        """Process WHERE clause conditions"""
        return {
            "type": "condition_group",
            "condition": self._process_condition_recursive(condition)
        }
    
    def _process_condition_recursive(self, condition: Condition) -> Dict[str, Any]:
        """Process individual conditions recursively"""
        if isinstance(condition.left, Condition):
            # This is a logical operation (AND/OR)
            return {
                "type": "logical",
                "operator": condition.operator,  # AND or OR
                "left": self._process_condition_recursive(condition.left),
                "right": self._process_condition_recursive(condition.right)
            }
        else:
            # This is a comparison operation
            return {
                "type": "comparison",
                "column": condition.left,
                "operator": self._normalize_operator(condition.operator),
                "value": condition.right,
                "value_type": self._get_value_type(condition.right)
            }
    
    def _process_group_by_clause(self, group_by_clause: GroupByClause) -> Dict[str, Any]:
        """Process GROUP BY clause"""
        return {
            "columns": group_by_clause.columns
        }
    
    def _process_having_clause(self, having_clause: HavingClause) -> Dict[str, Any]:
        """Process HAVING clause"""
        return {
            "type": "condition_group",
            "condition": self._process_condition_recursive(having_clause.condition)
        }
    
    def _process_order_clause(self, order_clause: OrderClause) -> Dict[str, Any]:
        """Process ORDER BY clause with multiple columns"""
        columns = []
        for order_col in order_clause.columns:
            columns.append({
                "column": order_col.column,
                "direction": order_col.direction.upper(),
                "ascending": order_col.direction.upper() == "ASC"
            })
        return {
            "columns": columns
        }
    
    def _process_limit_clause(self, limit_clause: LimitClause) -> Dict[str, Any]:
        """Process LIMIT clause"""
        return {
            "count": limit_clause.count,
            "offset": limit_clause.offset
        }
    
    def _normalize_operator(self, operator: str) -> str:
        """Normalize SQL operators to standard form"""
        operator_map = {
            '=': 'eq',
            '!=': 'ne',
            '<>': 'ne',
            '<': 'lt',
            '>': 'gt',
            '<=': 'le',
            '>=': 'ge',
            'IN': 'in',
            'NOT IN': 'not_in',
            'LIKE': 'like',
            'NOT LIKE': 'not_like',
            'BETWEEN': 'between',
            'NOT BETWEEN': 'not_between',
            'IS NULL': 'is_null',
            'IS NOT NULL': 'is_not_null'
        }
        return operator_map.get(operator.upper(), operator.lower())
    
    def _get_value_type(self, value: Any) -> str:
        """Determine the type of a value"""
        if isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return "list"
        elif value is None:
            return "null"
        else:
            return "unknown"