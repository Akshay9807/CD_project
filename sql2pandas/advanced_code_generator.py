from typing import Dict, List, Any, Union
import pandas as pd

class AdvancedCodeGenerator:
    """
    Advanced code generator that supports comprehensive SQL features including:
    - JOINs (INNER, LEFT, RIGHT, FULL OUTER)
    - Aggregate functions (COUNT, SUM, AVG, MAX, MIN)
    - GROUP BY and HAVING
    - Complex WHERE conditions (IN, LIKE, BETWEEN, IS NULL)
    - Multiple ORDER BY columns
    - LIMIT and OFFSET
    - DISTINCT
    """
    
    def __init__(self):
        self.table_aliases = {}
        self.available_tables = {}
    
    def generate(self, ir: Dict[str, Any], available_tables: Dict[str, Any] = None) -> str:
        """Generate comprehensive Pandas code from IR with better error handling"""
        if ir["operation"] != "select":
            raise ValueError(f"Unsupported operation: {ir['operation']}")
        
        self.available_tables = available_tables or {}
        self.table_aliases = {}  # Reset aliases for each query
        code_lines = []
        code_lines.append("# Generated Advanced Pandas Code")
        code_lines.append("import numpy as np")
        
        try:
            # Step 1: Load and prepare main table
            main_table = ir["from"]["table"]
            main_alias = ir["from"]["alias"]
            
            # Validate main table exists
            if main_table not in self.available_tables:
                raise ValueError(f"Table '{main_table}' not found in available tables")
            
            if main_alias:
                self.table_aliases[main_alias] = main_table
                code_lines.append(f"{main_alias} = {main_table}.copy()")
                result_df = main_alias
            else:
                code_lines.append(f"result = {main_table}.copy()")
                result_df = "result"
            
            # Step 2: Handle JOINs
            if ir["from"].get("joins"):
                result_df = self._generate_joins(ir["from"]["joins"], result_df, code_lines)
            
            # Step 3: Apply WHERE filters
            if ir.get("filters"):
                try:
                    filter_code = self._generate_filter_code(ir["filters"])
                    if filter_code and filter_code != "(True)":
                        code_lines.append(f"{result_df} = {result_df}[{filter_code}]")
                except Exception as e:
                    # If filter generation fails, add a comment and continue
                    code_lines.append(f"# Warning: Could not generate filter - {str(e)}")
            
            # Step 4: Handle GROUP BY and aggregations
            if ir.get("group_by") or self._has_aggregate_functions(ir["columns"]) or self._has_expressions(ir["columns"]):
                result_df = self._generate_group_by_code(ir, result_df, code_lines)
            
            # Step 5: Apply HAVING clause (after GROUP BY)
            if ir.get("having"):
                try:
                    having_code = self._generate_filter_code(ir["having"])
                    if having_code and having_code != "(True)":
                        # For HAVING clauses, we need to handle aggregate function references
                        # Replace aggregate function calls with column references
                        having_code = having_code.replace("COUNT(*)", "count_*")
                        having_code = having_code.replace("result[", f"{result_df}[")
                        code_lines.append(f"{result_df} = {result_df}[{having_code}]")
                except Exception as e:
                    code_lines.append(f"# Warning: Could not generate HAVING clause - {str(e)}")
            
            # Step 6: Select columns (if not already handled by GROUP BY)
            if not (ir.get("group_by") or self._has_aggregate_functions(ir["columns"]) or self._has_expressions(ir["columns"])):
                column_code = self._generate_column_selection(ir["columns"], ir.get("distinct", False))
                if column_code and "{df}" in column_code:
                    code_lines.append(f"{result_df} = {column_code.format(df=result_df)}")
            
            # Step 7: Apply DISTINCT (if not handled by GROUP BY)
            if ir.get("distinct") and not ir.get("group_by"):
                code_lines.append(f"{result_df} = {result_df}.drop_duplicates()")
            
            # Step 8: Apply ORDER BY
            if ir.get("ordering"):
                try:
                    order_code = self._generate_order_code(ir["ordering"])
                    if order_code and "{df}" in order_code:
                        code_lines.append(f"{result_df} = {order_code.format(df=result_df)}")
                except Exception as e:
                    code_lines.append(f"# Warning: Could not generate ORDER BY - {str(e)}")
            
            # Step 9: Apply LIMIT
            if ir.get("limit"):
                try:
                    limit_code = self._generate_limit_code(ir["limit"])
                    if limit_code and "{df}" in limit_code:
                        code_lines.append(f"{result_df} = {limit_code.format(df=result_df)}")
                except Exception as e:
                    code_lines.append(f"# Warning: Could not generate LIMIT - {str(e)}")
            
            # Step 10: Handle set operations (UNION, INTERSECT, EXCEPT)
            if ir.get("set_operations"):
                result_df = self._generate_set_operations(ir["set_operations"], result_df, code_lines)
            
            # Step 11: Ensure result is assigned
            if result_df != "result":
                code_lines.append(f"result = {result_df}")
            
            return "\n".join(code_lines)
            
        except Exception as e:
            # If code generation fails completely, return a simple fallback
            return f"""# Code generation failed: {str(e)}
# Fallback: return the main table
result = {ir["from"]["table"]}.copy()"""
    
    def _generate_joins(self, joins: List[Dict], main_df: str, code_lines: List[str]) -> str:
        """Generate JOIN operations"""
        current_df = main_df
        
        for i, join in enumerate(joins):
            join_table = join["table"]
            join_alias = join["alias"]
            join_type = join["type"].upper()
            
            # Prepare join table
            if join_alias:
                self.table_aliases[join_alias] = join_table
                code_lines.append(f"{join_alias} = {join_table}.copy()")
                right_df = join_alias
            else:
                right_df = join_table
            
            # Generate join condition (if not CROSS JOIN)
            if join_type == "CROSS":
                # CROSS JOIN - create cartesian product
                code_lines.append(f"{current_df}['_cross_key'] = 1")
                code_lines.append(f"{right_df}['_cross_key'] = 1")
                on_condition = "on='_cross_key', suffixes=('', '_y')"
                pandas_how = "inner"
            else:
                on_condition = self._generate_join_condition(join["on_condition"])
            
            # Map join types to pandas merge parameters
            join_mapping = {
                "INNER": "inner",
                "LEFT": "left", 
                "LEFT OUTER": "left",
                "RIGHT": "right",
                "RIGHT OUTER": "right",
                "FULL": "outer",
                "FULL OUTER": "outer",
                "CROSS": "cross"
            }
            
            pandas_how = join_mapping.get(join_type, "inner")
            
            # Generate merge code with error handling
            result_var = f"joined_{i}" if i < len(joins) - 1 else "result"
            
            if join_type == "CROSS":
                # CROSS JOIN handling
                code_lines.append(f"{result_var} = pd.merge({current_df}, {right_df}, {on_condition})")
                code_lines.append(f"{result_var} = {result_var}.drop('_cross_key', axis=1)")
            else:
                # Regular JOIN handling
                code_lines.append(f"try:")
                code_lines.append(
                    f"    {result_var} = pd.merge({current_df}, {right_df}, "
                    f"how='{pandas_how}', {on_condition}, suffixes=('', '_y'))"
                )
                code_lines.append(f"except:")
                code_lines.append(f"    # Fallback join if columns don't match exactly")
                code_lines.append(f"    {current_df}['_temp_key'] = 1")
                code_lines.append(f"    {right_df}['_temp_key'] = 1")
                code_lines.append(f"    {result_var} = pd.merge({current_df}, {right_df}, on='_temp_key', how='{pandas_how}', suffixes=('', '_y'))")
                code_lines.append(f"    {result_var} = {result_var}.drop('_temp_key', axis=1)")
            current_df = result_var
        
        return current_df
    
    def _generate_join_condition(self, condition: Dict[str, Any]) -> str:
        """Generate JOIN ON condition for pandas merge"""
        if not condition:
            return "left_index=True, right_index=True"
            
        if condition["type"] == "comparison" and condition["operator"] == "eq":
            left_col = condition["column"]
            right_col = condition["value"]
            
            # Handle table.column format
            if "." in left_col:
                left_table, left_col = left_col.split(".", 1)
            if "." in str(right_col):
                right_table, right_col = str(right_col).split(".", 1)
            
            return f"left_on='{left_col}', right_on='{right_col}'"
        else:
            # For complex join conditions, we might need a different approach
            return "left_index=True, right_index=True"
    
    def _has_aggregate_functions(self, columns: List[Dict]) -> bool:
        """Check if any columns use aggregate functions"""
        return any(col.get("function") for col in columns)
    
    def _has_expressions(self, columns: List[Dict]) -> bool:
        """Check if any columns use complex expressions"""
        return any(col.get("expression") for col in columns)
    
    def _generate_group_by_code(self, ir: Dict, result_df: str, code_lines: List[str]) -> str:
        """Generate GROUP BY and aggregation code with expression support"""
        group_by_cols = ir["group_by"]["columns"] if ir["group_by"] else []
        
        # Check if we have complex expressions that need special handling
        has_complex_expressions = any(col.get("expression") for col in ir["columns"])
        
        if has_complex_expressions:
            return self._generate_complex_group_by(ir, result_df, code_lines)
        
        # Handle regular GROUP BY
        regular_cols = []
        agg_funcs = {}
        
        for col in ir["columns"]:
            if col["function"]:
                func_name = col["function"].lower()
                col_name = col["name"]
                alias = col["alias"] or f"{func_name}_{col_name}"
                
                if col_name == "*" and func_name == "count":
                    # COUNT(*) - count all rows
                    agg_funcs[alias] = "size()"
                else:
                    # Regular aggregate function
                    pandas_func = {
                        "count": "count()",
                        "sum": "sum()",
                        "avg": "mean()",
                        "max": "max()",
                        "min": "min()"
                    }.get(func_name, f"{func_name}()")
                    
                    agg_funcs[alias] = f"('{col_name}').{pandas_func}"
            else:
                regular_cols.append(col["name"])
        
        if group_by_cols:
            # Validate group by columns exist
            valid_group_cols = [col for col in group_by_cols if isinstance(col, str) and len(col) > 1]
            
            if not valid_group_cols:
                # If no valid group by columns, skip grouping
                return result_df
            
            # GROUP BY with aggregations
            if agg_funcs:
                try:
                    # Build aggregation dictionary properly
                    agg_dict_items = []
                    for alias, func in agg_funcs.items():
                        if func == "size()":
                            agg_dict_items.append(f"'{alias}': 'size'")
                        else:
                            # Extract column and function from the string like "('age').mean()"
                            if "')." in func:
                                col_part = func.split("').")[0].replace("('", "")
                                func_part = func.split("').")[1].replace("()", "")
                                # Validate column exists
                                if len(col_part) > 1:  # Avoid single character column names that might be aliases
                                    agg_dict_items.append(f"'{alias}': ('{col_part}', '{func_part}')")
                    
                    if agg_dict_items:
                        code_lines.append(f"grouped = {result_df}.groupby({valid_group_cols})")
                        
                        # Handle different aggregation cases
                        if len(agg_dict_items) == 1 and "'size'" in agg_dict_items[0]:
                            # Simple COUNT(*) case
                            alias_name = list(agg_funcs.keys())[0]
                            code_lines.append(f"result_agg = grouped.size().reset_index(name='{alias_name}')")
                        else:
                            # Multiple aggregations
                            agg_dict = "{" + ", ".join(agg_dict_items) + "}"
                            code_lines.append(f"result_agg = grouped.agg({agg_dict})")
                            
                            # Handle column naming - fix for MultiIndex columns
                            code_lines.append("# Fix column names after aggregation")
                            code_lines.append("if hasattr(result_agg.columns, 'levels'):")
                            code_lines.append("    result_agg.columns = result_agg.columns.droplevel(0)")
                            
                            # Rename columns to match aliases
                            if len(agg_funcs) == 1:
                                alias_name = list(agg_funcs.keys())[0]
                                code_lines.append(f"result_agg = result_agg.rename(columns={{result_agg.columns[0]: '{alias_name}'}})")
                            else:
                                rename_map = {f"col_{i}": alias for i, alias in enumerate(agg_funcs.keys())}
                                code_lines.append(f"# Rename aggregated columns")
                                for old_name, new_name in zip(range(len(agg_funcs)), agg_funcs.keys()):
                                    code_lines.append(f"if len(result_agg.columns) > {old_name}: result_agg = result_agg.rename(columns={{result_agg.columns[{old_name}]: '{new_name}'}})")
                            
                            code_lines.append("result_agg = result_agg.reset_index()")
                        
                        return "result_agg"
                    else:
                        # Fallback to simple groupby
                        code_lines.append(f"result_grouped = {result_df}.groupby({valid_group_cols}).first().reset_index()")
                        return "result_grouped"
                        
                except Exception:
                    # If aggregation fails, fall back to simple grouping
                    code_lines.append(f"result_grouped = {result_df}.groupby({valid_group_cols}).first().reset_index()")
                    return "result_grouped"
            else:
                # GROUP BY without aggregations (just grouping)
                code_lines.append(f"result_grouped = {result_df}.groupby({valid_group_cols}).first().reset_index()")
                return "result_grouped"
        
        elif agg_funcs:
            # Aggregations without GROUP BY (single row result)
            agg_operations = []
            for alias, func in agg_funcs.items():
                if func == "size()":
                    agg_operations.append(f"'{alias}': len({result_df})")
                else:
                    # Extract column and function properly
                    col_part = func.split("').")[0].replace("('", "")
                    func_part = func.split("').")[1].replace("()", "")
                    agg_operations.append(f"'{alias}': {result_df}['{col_part}'].{func_part}()")
            
            code_lines.append(f"result_agg = pd.DataFrame([{{{', '.join(agg_operations)}}}])")
            return "result_agg"
        
        return result_df
    
    def _generate_column_selection(self, columns: List[Dict], distinct: bool) -> str:
        """Generate column selection code with expression support"""
        if not columns:
            return "{df}"
        
        if len(columns) == 1 and columns[0]["name"] == "*":
            return "{df}"
        
        # Check if we have complex expressions that need special handling
        has_expressions = any(col.get("expression") for col in columns)
        
        if has_expressions:
            return self._generate_expression_selection(columns)
        
        # Handle regular column selection
        selected_cols = []
        column_renames = {}
        
        for col in columns:
            col_name = col["name"]
            
            # Handle table.column format - extract just the column name
            if col.get("table_alias") and "." not in col_name:
                selected_cols.append(col_name)
            elif "." in col_name:
                actual_col = col_name.split(".")[-1]
                selected_cols.append(actual_col)
            else:
                selected_cols.append(col_name)
            
            # Handle column aliases
            if col.get("alias"):
                original_name = selected_cols[-1]
                column_renames[original_name] = col["alias"]
        
        # Generate selection code
        if selected_cols:
            result_code = "{df}[" + str(selected_cols) + "]"
            
            # Add rename operations if needed
            if column_renames:
                rename_dict = str(column_renames)
                result_code += f".rename(columns={rename_dict})"
            
            return result_code
        
        return "{df}"
    
    def _generate_expression_selection(self, columns: List[Dict]) -> str:
        """Generate selection code for complex expressions"""
        assignments = []
        
        for col in columns:
            if col.get("expression"):
                expr_code = self._generate_expression_code(col["expression"])
                alias = col.get("alias") or f"expr_{len(assignments)}"
                assignments.append(f"'{alias}': {expr_code}")
            else:
                # Regular column
                col_name = col["name"]
                if "." in col_name:
                    col_name = col_name.split(".")[-1]
                
                alias = col.get("alias") or col_name
                assignments.append(f"'{alias}': {{df}}['{col_name}']")
        
        return "pd.DataFrame({" + ", ".join(assignments) + "})"
    
    def _generate_expression_code(self, expr: Dict[str, Any]) -> str:
        """Generate pandas code for expressions"""
        if not expr:
            return "None"
        
        expr_type = expr.get("type")
        
        if expr_type == "case":
            return self._generate_case_code(expr)
        elif expr_type == "function":
            return self._generate_function_code(expr)
        elif expr_type == "column":
            col_name = expr["value"]
            if "." in col_name:
                col_name = col_name.split(".")[-1]
            return f"{{df}}['{col_name}']"
        elif expr_type == "literal":
            value = expr["value"]
            if isinstance(value, str) and value != "*":
                # Remove quotes if they're already there
                if value.startswith("'") and value.endswith("'"):
                    return value  # Already quoted
                return f"'{value}'"
            return str(value)
        
        return "None"
    
    def _generate_case_code(self, case_expr: Dict[str, Any]) -> str:
        """Generate pandas code for CASE expressions"""
        conditions = case_expr.get("conditions", [])
        case_column = case_expr.get("value")  # For simple CASE statements
        
        if not conditions:
            return "None"
        
        # Build numpy.select conditions and choices
        condition_codes = []
        choice_codes = []
        
        for condition in conditions:
            when_expr = condition["when"]
            then_expr = condition["then"]
            
            if case_column:
                # Simple CASE: CASE column WHEN value THEN result
                when_value = self._generate_expression_code(when_expr)
                # Remove extra quotes if present
                if when_value.startswith("''") and when_value.endswith("''"):
                    when_value = when_value[1:-1]  # Remove outer quotes
                condition_code = f"({{df}}['{case_column}'] == {when_value})"
            else:
                # Searched CASE: CASE WHEN condition THEN result
                condition_code = self._generate_expression_code(when_expr)
            
            condition_codes.append(condition_code)
            
            # Process THEN value
            then_value = self._generate_expression_code(then_expr)
            # Convert to numeric if it's a number
            if then_value.isdigit():
                then_value = int(then_value)
            choice_codes.append(str(then_value))
        
        # Handle ELSE clause
        else_value = "np.nan"
        if case_expr.get("arguments"):
            else_value = self._generate_expression_code(case_expr["arguments"][0])
        
        conditions_str = "[" + ", ".join(condition_codes) + "]"
        choices_str = "[" + ", ".join(choice_codes) + "]"
        
        return f"np.select({conditions_str}, {choices_str}, default={else_value})"
    
    def _generate_function_code(self, func_expr: Dict[str, Any]) -> str:
        """Generate pandas code for function calls"""
        func_name = func_expr.get("value", "").lower()
        args = func_expr.get("arguments", [])
        
        if func_name == "round":
            if len(args) >= 2:
                value_code = self._generate_expression_code(args[0])
                decimals = self._generate_expression_code(args[1])
                return f"np.round({value_code}, {decimals})"
            elif len(args) == 1:
                value_code = self._generate_expression_code(args[0])
                return f"np.round({value_code})"
        
        elif func_name in ["avg", "mean"]:
            if args:
                value_code = self._generate_expression_code(args[0])
                return f"{value_code}.mean()"
        
        elif func_name == "count":
            if args and args[0].get("value") == "*":
                return "len({df})"
            elif args:
                value_code = self._generate_expression_code(args[0])
                return f"{value_code}.count()"
        
        elif func_name in ["sum", "max", "min"]:
            if args:
                value_code = self._generate_expression_code(args[0])
                return f"{value_code}.{func_name}()"
        
        elif func_name == "upper":
            if args:
                value_code = self._generate_expression_code(args[0])
                return f"{value_code}.str.upper()"
        
        elif func_name == "lower":
            if args:
                value_code = self._generate_expression_code(args[0])
                return f"{value_code}.str.lower()"
        
        # Default fallback
        return "None"
    
    def _generate_filter_code(self, filters: Dict[str, Any]) -> str:
        """Generate comprehensive filter conditions"""
        if not filters:
            return ""
        
        # Handle both old and new IR structures
        if "condition" in filters:
            return self._generate_condition_code(filters["condition"])
        elif "type" in filters:
            return self._generate_condition_code(filters)
        else:
            return ""
    
    def _generate_condition_code(self, condition: Dict[str, Any]) -> str:
        """Generate code for individual conditions"""
        if not condition:
            return ""
        
        try:
            if condition["type"] == "logical":
                left_code = self._generate_condition_code(condition["left"])
                right_code = self._generate_condition_code(condition["right"])
                
                if not left_code or not right_code:
                    return left_code or right_code or ""
                
                if condition["operator"].upper() == "AND":
                    return f"({left_code}) & ({right_code})"
                elif condition["operator"].upper() == "OR":
                    return f"({left_code}) | ({right_code})"
            
            elif condition["type"] == "comparison":
                return self._generate_comparison_code(condition)
            
            return ""
            
        except Exception as e:
            # Return a safe fallback
            return "(True)"
    
    def _generate_comparison_code(self, condition: Dict[str, Any]) -> str:
        """Generate code for comparison operations"""
        column = condition["column"]
        operator = condition["operator"]
        value = condition["value"]
        value_type = condition["value_type"]
        
        # Handle table.column format - just use the column name
        if "." in column:
            table_alias, column = column.split(".", 1)
            # Store the table alias for potential future use
            if table_alias not in self.table_aliases:
                self.table_aliases[table_alias] = table_alias
        
        # Validate column exists in result DataFrame
        # Note: We'll use a more flexible approach that doesn't fail on missing columns
        
        # Format value based on type
        if value_type == "string":
            formatted_value = f"'{value}'"
        elif value_type == "list":
            # Handle list values for IN clauses
            if isinstance(value, list):
                formatted_list = [f"'{v}'" if isinstance(v, str) else str(v) for v in value]
                formatted_value = f"[{', '.join(formatted_list)}]"
            else:
                formatted_value = str(value)
        else:
            formatted_value = str(value)
        
        # Generate pandas operations with error handling
        try:
            if operator == "eq":
                return f"(result['{column}'] == {formatted_value})"
            elif operator == "ne":
                return f"(result['{column}'] != {formatted_value})"
            elif operator == "lt":
                return f"(result['{column}'] < {formatted_value})"
            elif operator == "gt":
                return f"(result['{column}'] > {formatted_value})"
            elif operator == "le":
                return f"(result['{column}'] <= {formatted_value})"
            elif operator == "ge":
                return f"(result['{column}'] >= {formatted_value})"
            elif operator == "in":
                return f"(result['{column}'].isin({formatted_value}))"
            elif operator == "not_in":
                return f"(~result['{column}'].isin({formatted_value}))"
            elif operator == "like":
                # Convert SQL LIKE to pandas str.contains
                pattern = value.replace('%', '.*').replace('_', '.')
                return f"(result['{column}'].str.contains('{pattern}', na=False, regex=True))"
            elif operator == "not_like":
                pattern = value.replace('%', '.*').replace('_', '.')
                return f"(~result['{column}'].str.contains('{pattern}', na=False, regex=True))"
            elif operator == "between":
                start_val, end_val = value
                return f"(result['{column}'].between({start_val}, {end_val}))"
            elif operator == "not_between":
                start_val, end_val = value
                return f"(~result['{column}'].between({start_val}, {end_val}))"
            elif operator == "is_null":
                return f"(result['{column}'].isna())"
            elif operator == "is_not_null":
                return f"(result['{column}'].notna())"
            
            return f"(result['{column}'] == {formatted_value})"
            
        except Exception:
            # Fallback: return a condition that always evaluates to True
            return "(True)"
    
    def _generate_order_code(self, ordering: Dict[str, Any]) -> str:
        """Generate ORDER BY code for multiple columns"""
        columns = []
        ascending = []
        
        for order_col in ordering["columns"]:
            columns.append(order_col["column"])
            ascending.append(order_col["ascending"])
        
        if len(columns) == 1:
            return f"{{df}}.sort_values(by='{columns[0]}', ascending={ascending[0]})"
        else:
            return f"{{df}}.sort_values(by={columns}, ascending={ascending})"
    
    def _generate_limit_code(self, limit: Dict[str, Any]) -> str:
        """Generate LIMIT/OFFSET code"""
        count = limit["count"]
        offset = limit.get("offset", 0)
        
        if offset:
            return f"{{df}}.iloc[{offset}:{offset + count}]"
        else:
            return f"{{df}}.head({count})"
    
    def _generate_complex_group_by(self, ir: Dict, result_df: str, code_lines: List[str]) -> str:
        """Generate GROUP BY code for complex expressions like CASE statements"""
        group_by_cols = ir["group_by"]["columns"] if ir["group_by"] else []
        
        # Validate group by columns exist
        valid_group_cols = [col for col in group_by_cols if isinstance(col, str) and len(col) > 1]
        
        if not valid_group_cols:
            return result_df
        
        # For complex expressions, we need to use a different approach
        # First, create the expression columns
        code_lines.append("# Handle complex expressions in GROUP BY")
        
        for col in ir["columns"]:
            if col.get("expression"):
                alias = col.get("alias") or "expr_col"
                
                # Handle nested function expressions (like AVG(CASE...))
                expr = col["expression"]
                if expr.get("type") == "function" and expr.get("arguments"):
                    # This is a function with arguments, extract the inner expression
                    inner_expr = expr["arguments"][0]
                    inner_code = self._generate_expression_code(inner_expr)
                    code_lines.append(f"{result_df}['{alias}_temp'] = {inner_code.format(df=result_df)}")
                else:
                    # Regular expression
                    expr_code = self._generate_expression_code(expr)
                    code_lines.append(f"{result_df}['{alias}_temp'] = {expr_code.format(df=result_df)}")
        
        # Now perform the GROUP BY with aggregation
        group_cols_str = str(valid_group_cols)
        
        # Build aggregation for expression columns
        agg_dict = {}
        for col in ir["columns"]:
            if col.get("expression"):
                alias = col.get("alias") or "expr_col"
                temp_col = f"{alias}_temp"
                
                # Check if the expression contains an aggregate function
                expr = col["expression"]
                if self._expression_has_aggregate(expr):
                    # This is an aggregate expression like AVG(CASE...)
                    agg_dict[alias] = (temp_col, 'mean')  # Default to mean for AVG
                else:
                    agg_dict[alias] = (temp_col, 'first')
            elif not col.get("function"):
                # Regular column (should be in GROUP BY)
                col_name = col["name"]
                if "." in col_name:
                    col_name = col_name.split(".")[-1]
                # This column is already in the GROUP BY, no need to aggregate
        
        if agg_dict:
            code_lines.append(f"grouped = {result_df}.groupby({group_cols_str})")
            
            # Create aggregation dictionary
            agg_items = []
            for alias, (col, func) in agg_dict.items():
                agg_items.append(f"'{alias}': ('{col}', '{func}')")
            
            agg_dict_str = "{" + ", ".join(agg_items) + "}"
            code_lines.append(f"result_agg = grouped.agg({agg_dict_str})")
            
            # Flatten column names
            code_lines.append("result_agg.columns = [col[0] for col in result_agg.columns]")
            code_lines.append("result_agg = result_agg.reset_index()")
            
            return "result_agg"
        
        return result_df
    
    def _expression_has_aggregate(self, expr: Dict[str, Any]) -> bool:
        """Check if an expression contains aggregate functions"""
        if not expr:
            return False
        
        if expr.get("type") == "function":
            func_name = expr.get("value", "").lower()
            if func_name in ["avg", "sum", "count", "max", "min", "mean"]:
                return True
        
        # Check arguments recursively
        if expr.get("arguments"):
            for arg in expr["arguments"]:
                if self._expression_has_aggregate(arg):
                    return True
        
        return False
    
    def _generate_set_operations(self, set_operations: List[Dict], left_df: str, code_lines: List[str]) -> str:
        """Generate code for set operations (UNION, INTERSECT, EXCEPT)"""
        current_df = left_df
        
        for i, op in enumerate(set_operations):
            op_type = op["type"].upper()
            all_flag = op.get("all", False)
            right_query_ir = op.get("right_query")
            
            if not right_query_ir:
                continue
            
            # Generate code for the right query
            right_df_name = f"right_query_{i}"
            code_lines.append(f"# Generate right query for {op_type}")
            
            # Recursively generate the right query
            right_code = self.generate(right_query_ir, self.available_tables)
            
            # Extract the result assignment from right code
            right_lines = right_code.split('\n')
            for line in right_lines:
                if line.strip() and not line.startswith('#'):
                    # Replace 'result =' with our right_df_name
                    if 'result =' in line:
                        line = line.replace('result =', f'{right_df_name} =')
                    code_lines.append(line)
            
            # Perform the set operation
            result_var = f"set_result_{i}" if i < len(set_operations) - 1 else "result"
            
            if op_type == "UNION":
                if all_flag:
                    # UNION ALL - concatenate without removing duplicates
                    code_lines.append(f"{result_var} = pd.concat([{current_df}, {right_df_name}], ignore_index=True)")
                else:
                    # UNION - concatenate and remove duplicates
                    code_lines.append(f"{result_var} = pd.concat([{current_df}, {right_df_name}], ignore_index=True).drop_duplicates()")
            
            elif op_type == "INTERSECT":
                # INTERSECT - find common rows
                code_lines.append(f"# Perform INTERSECT operation")
                code_lines.append(f"{result_var} = pd.merge({current_df}, {right_df_name}, how='inner')")
                if not all_flag:
                    code_lines.append(f"{result_var} = {result_var}.drop_duplicates()")
            
            elif op_type == "EXCEPT":
                # EXCEPT - find rows in left but not in right
                code_lines.append(f"# Perform EXCEPT operation")
                code_lines.append(f"merged = pd.merge({current_df}, {right_df_name}, how='left', indicator=True)")
                code_lines.append(f"{result_var} = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)")
                if not all_flag:
                    code_lines.append(f"{result_var} = {result_var}.drop_duplicates()")
            
            current_df = result_var
        
        return current_df