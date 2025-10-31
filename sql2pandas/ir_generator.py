from typing import Dict, List, Any, Optional
from parser import SelectStatement, WhereClause, OrderClause, Condition

class IRGenerator:
    """
    Generates Intermediate Representation (IR) from AST.
    IR is a JSON-like structure that represents the query operations.
    """
    
    def generate(self, ast: SelectStatement) -> Dict[str, Any]:
        """Generate IR from AST"""
        ir = {
            "operation": "select",
            "columns": self._process_columns(ast.columns),
            "table": ast.table,
            "filters": None,
            "ordering": None
        }
        
        if ast.where_clause:
            ir["filters"] = self._process_where_clause(ast.where_clause)
        
        if ast.order_clause:
            ir["ordering"] = self._process_order_clause(ast.order_clause)
        
        return ir
    
    def _process_columns(self, columns) -> List[str]:
        """Process column list"""
        column_names = [col.name for col in columns]
        # Handle SELECT * case
        if len(column_names) == 1 and column_names[0] == "*":
            return ["*"]
        return column_names
    
    def _process_where_clause(self, where_clause: WhereClause) -> Dict[str, Any]:
        """Process WHERE clause into filter conditions"""
        return {
            "type": "condition_group",
            "conditions": self._process_condition(where_clause.condition)
        }
    
    def _process_condition(self, condition: Condition) -> List[Dict[str, Any]]:
        """Process individual conditions recursively"""
        conditions = []
        
        # Current condition
        # If the right-hand side is a SelectStatement (subquery), preserve it
        if isinstance(condition.right, SelectStatement):
            current_cond = {
                "column": condition.left,
                "operator": self._normalize_operator(condition.operator),
                "value": condition.right,  # keep SelectStatement
                "value_type": "subquery"
            }
        else:
            current_cond = {
                "column": condition.left,
                "operator": self._normalize_operator(condition.operator),
                "value": condition.right,
                "value_type": self._get_value_type(condition.right)
            }
        conditions.append(current_cond)
        
        # Handle chained conditions (AND, OR)
        if condition.logical_op and condition.next_condition:
            logical_info = {
                "logical_operator": condition.logical_op,
                "next_conditions": self._process_condition(condition.next_condition)
            }
            conditions.append(logical_info)
        
        return conditions
    
    def _process_order_clause(self, order_clause: OrderClause) -> Dict[str, Any]:
        """Process ORDER BY clause"""
        return {
            "column": order_clause.column,
            "direction": order_clause.direction.upper(),
            "ascending": order_clause.direction.upper() == "ASC"
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
            '>=': 'ge'
        }
        return operator_map.get(operator, operator)
    
    def _get_value_type(self, value: Any) -> str:
        """Determine the type of a value"""
        if isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            return "string"
        else:
            return "unknown"