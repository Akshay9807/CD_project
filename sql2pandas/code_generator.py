from typing import Dict, List, Any

class CodeGenerator:
    """
    Generates Pandas code from Intermediate Representation (IR).
    """
    
    def generate(self, ir: Dict[str, Any]) -> str:
        """Generate Pandas code from IR"""
        if ir["operation"] != "select":
            raise ValueError(f"Unsupported operation: {ir['operation']}")
        
        code_lines = []
        code_lines.append("# Generated Pandas code")
        code_lines.append("result = df.copy()")
        
        # Apply filters (WHERE clause)
        if ir["filters"]:
            filter_code = self._generate_filter_code(ir["filters"])
            if filter_code:
                code_lines.append(f"result = result[{filter_code}]")
        
        # Apply column selection
        if ir["columns"]:
            columns_str = str(ir["columns"])
            code_lines.append(f"result = result[{columns_str}]")
        
        # Apply ordering (ORDER BY clause)
        if ir["ordering"]:
            order_code = self._generate_order_code(ir["ordering"])
            code_lines.append(order_code)
        
        return "\n".join(code_lines)
    
    def _generate_filter_code(self, filters: Dict[str, Any]) -> str:
        """Generate filter conditions for WHERE clause"""
        if filters["type"] != "condition_group":
            raise ValueError("Invalid filter structure")
        
        conditions = filters["conditions"]
        return self._process_conditions(conditions)
    
    def _process_conditions(self, conditions: List[Dict[str, Any]]) -> str:
        """Process list of conditions recursively"""
        if not conditions:
            return ""
        
        result_parts = []
        i = 0
        
        while i < len(conditions):
            condition = conditions[i]
            
            if "column" in condition:
                # Regular condition
                condition_code = self._generate_single_condition(condition)
                result_parts.append(condition_code)
                
                # Check if next item is a logical operator
                if i + 1 < len(conditions) and "logical_operator" in conditions[i + 1]:
                    logical_info = conditions[i + 1]
                    logical_op = " & " if logical_info["logical_operator"] == "AND" else " | "
                    
                    # Process next conditions
                    next_conditions_code = self._process_conditions(logical_info["next_conditions"])
                    if next_conditions_code:
                        result_parts.append(logical_op)
                        result_parts.append(f"({next_conditions_code})")
                    
                    i += 2  # Skip the logical operator entry
                else:
                    i += 1
            else:
                i += 1
        
        return "".join(result_parts)
    
    def _generate_single_condition(self, condition: Dict[str, Any]) -> str:
        """Generate code for a single condition"""
        column = condition["column"]
        operator = condition["operator"]
        value = condition["value"]
        value_type = condition["value_type"]
        
        # Format value based on type
        if value_type == "string":
            formatted_value = f"'{value}'"
        else:
            formatted_value = str(value)
        
        # Map operators to pandas operations
        operator_map = {
            'eq': '==',
            'ne': '!=',
            'lt': '<',
            'gt': '>',
            'le': '<=',
            'ge': '>='
        }
        
        pandas_op = operator_map.get(operator, operator)
        return f"(result['{column}'] {pandas_op} {formatted_value})"
    
    def _generate_order_code(self, ordering: Dict[str, Any]) -> str:
        """Generate code for ORDER BY clause"""
        column = ordering["column"]
        ascending = ordering["ascending"]
        
        return f"result = result.sort_values(by='{column}', ascending={ascending})"