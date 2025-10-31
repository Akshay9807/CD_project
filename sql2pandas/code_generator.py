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
            # _generate_filter_code now returns (preamble_lines, filter_code)
            preamble, filter_code = self._generate_filter_code(ir["filters"])
            for line in preamble:
                code_lines.append(line)
            if filter_code:
                code_lines.append(f"result = result[{filter_code}]")
        
        # Apply column selection
        if ir["columns"] and ir["columns"] != ["*"]:
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
        # return preamble lines and the final boolean expression string
        preamble = []
        expr = self._process_conditions(conditions, preamble)
        return preamble, expr
    
    def _process_conditions(self, conditions: List[Dict[str, Any]], preamble: List[str]) -> str:
        """Process list of conditions recursively"""
        if not conditions:
            return ""
        
        result_parts = []
        i = 0
        
        while i < len(conditions):
            condition = conditions[i]
            # Regular condition
            if "column" in condition:
                condition_code = self._generate_single_condition(condition, preamble)
                result_parts.append(condition_code)

                # Check if next item is a logical operator
                if i + 1 < len(conditions) and "logical_operator" in conditions[i + 1]:
                    logical_info = conditions[i + 1]
                    logical_op = " & " if logical_info["logical_operator"] == "AND" else " | "

                    # Process next conditions
                    next_conditions_code = self._process_conditions(logical_info["next_conditions"], preamble)
                    if next_conditions_code:
                        result_parts.append(logical_op)
                        result_parts.append(f"({next_conditions_code})")

                    i += 2  # Skip the logical operator entry
                else:
                    i += 1
            else:
                i += 1
        
        return "".join(result_parts)
    
    def _generate_single_condition(self, condition: Dict[str, Any], preamble: List[str]) -> str:
        """Generate code for a single condition"""
        column = condition["column"]
        operator = condition["operator"]
        value = condition["value"]
        value_type = condition["value_type"]
        
        # If RHS is a subquery, generate code to evaluate it first
        if value_type == 'subquery' and hasattr(value, 'columns'):
            # Create a unique temp var name
            temp_var = f"_subq_{abs(hash(str(value))) % (10**8)}"
            subquery_code = self._generate_scalar_subquery_code(value)
            # add preamble line that assigns the scalar result
            preamble.append(f"{temp_var} = {subquery_code}")
            rhs = temp_var
            formatted_value = rhs
        else:
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

    def _generate_scalar_subquery_code(self, select_stmt: Any) -> str:
        """Generate a pandas expression for simple scalar subqueries like SELECT AVG(col) FROM table

        Currently supports: AVG(col) -> df['col'].mean(), MAX/MIN -> .max()/min(), COUNT(*) -> len(df)
        """
        # We expect select_stmt.columns to be a list with one Column whose name may be like 'AVG(age)'
        if len(select_stmt.columns) != 1:
            raise ValueError("Only single-column scalar subqueries are supported for now")

        col_expr = select_stmt.columns[0].name  # e.g. "AVG(age)" or "COUNT(*)"
        col_expr_up = col_expr.upper()
        # AVG(...)
        if col_expr_up.startswith("AVG(") and col_expr.endswith(")"):
            inner = col_expr[4:-1].strip()
            return f"df['{inner}'].mean()"
        if col_expr_up.startswith("MAX(") and col_expr.endswith(")"):
            inner = col_expr[4:-1].strip()
            return f"df['{inner}'].max()"
        if col_expr_up.startswith("MIN(") and col_expr.endswith(")"):
            inner = col_expr[4:-1].strip()
            return f"df['{inner}'].min()"
        if col_expr_up.startswith("COUNT("):
            # COUNT(*) or COUNT(col)
            inner = col_expr[col_expr.find('(')+1:col_expr.rfind(')')].strip()
            if inner == '*' or inner == '1':
                return "len(df)"
            else:
                return f"df['{inner}'].count()"

        # Fallback: attempt to return a DataFrame column or expression as-is
        return f"df['{col_expr}']"
    
    def _generate_order_code(self, ordering: Dict[str, Any]) -> str:
        """Generate code for ORDER BY clause"""
        column = ordering["column"]
        ascending = ordering["ascending"]
        
        return f"result = result.sort_values(by='{column}', ascending={ascending})"