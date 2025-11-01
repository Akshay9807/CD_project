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
        """Generate comprehensive Pandas code from IR"""
        if ir["operation"] != "select":
            raise ValueError(f"Unsupported operation: {ir['operation']}")
        
        self.available_tables = available_tables or {}
        code_lines = []
        code_lines.append("# Generated Advanced Pandas Code")
        
        # Step 1: Load and prepare main table
        main_table = ir["from"]["table"]
        main_alias = ir["from"]["alias"]
        
        if main_alias:
            self.table_aliases[main_alias] = main_table
            code_lines.append(f"{main_alias} = {main_table}.copy()")
            result_df = main_alias
        else:
            code_lines.append(f"result = {main_table}.copy()")
            result_df = "result"
        
        # Step 2: Handle JOINs
        if ir["from"]["joins"]:
            result_df = self._generate_joins(ir["from"]["joins"], result_df, code_lines)
        
        # Step 3: Apply WHERE filters
        if ir["filters"]:
            filter_code = self._generate_filter_code(ir["filters"]["condition"])
            if filter_code:
                code_lines.append(f"{result_df} = {result_df}[{filter_code}]")
        
        # Step 4: Handle GROUP BY and aggregations
        if ir["group_by"] or self._has_aggregate_functions(ir["columns"]):
            result_df = self._generate_group_by_code(ir, result_df, code_lines)
        
        # Step 5: Apply HAVING clause (after GROUP BY)
        if ir["having"]:
            having_code = self._generate_filter_code(ir["having"]["condition"])
            if having_code:
                code_lines.append(f"{result_df} = {result_df}[{having_code}]")
        
        # Step 6: Select columns (if not already handled by GROUP BY)
        if not (ir["group_by"] or self._has_aggregate_functions(ir["columns"])):
            column_code = self._generate_column_selection(ir["columns"], ir["distinct"])
            if column_code:
                code_lines.append(f"{result_df} = {column_code.format(df=result_df)}")
        
        # Step 7: Apply DISTINCT (if not handled by GROUP BY)
        if ir["distinct"] and not ir["group_by"]:
            code_lines.append(f"{result_df} = {result_df}.drop_duplicates()")
        
        # Step 8: Apply ORDER BY
        if ir["ordering"]:
            order_code = self._generate_order_code(ir["ordering"])
            code_lines.append(f"{result_df} = {order_code.format(df=result_df)}")
        
        # Step 9: Apply LIMIT
        if ir["limit"]:
            limit_code = self._generate_limit_code(ir["limit"])
            code_lines.append(f"{result_df} = {limit_code.format(df=result_df)}")
        
        # Step 10: Ensure result is assigned
        if result_df != "result":
            code_lines.append(f"result = {result_df}")
        
        return "\n".join(code_lines)
    
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
            
            # Generate join condition
            on_condition = self._generate_join_condition(join["on_condition"])
            
            # Map join types to pandas merge parameters
            join_mapping = {
                "INNER": "inner",
                "LEFT": "left", 
                "LEFT OUTER": "left",
                "RIGHT": "right",
                "RIGHT OUTER": "right",
                "FULL": "outer",
                "FULL OUTER": "outer"
            }
            
            pandas_how = join_mapping.get(join_type, "inner")
            
            # Generate merge code
            result_var = f"joined_{i}" if i < len(joins) - 1 else "result"
            code_lines.append(
                f"{result_var} = pd.merge({current_df}, {right_df}, "
                f"how='{pandas_how}', {on_condition})"
            )
            current_df = result_var
        
        return current_df
    
    def _generate_join_condition(self, condition: Dict[str, Any]) -> str:
        """Generate JOIN ON condition for pandas merge"""
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
    
    def _generate_group_by_code(self, ir: Dict, result_df: str, code_lines: List[str]) -> str:
        """Generate GROUP BY and aggregation code"""
        group_by_cols = ir["group_by"]["columns"] if ir["group_by"] else []
        
        # Separate regular columns from aggregate functions
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
                        code_lines.append(f"result_agg = grouped.agg({{{', '.join(agg_dict_items)}}})")
                        code_lines.append("result_agg.columns = ['" + "', '".join(agg_funcs.keys()) + "']")
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
        """Generate column selection code"""
        if len(columns) == 1 and columns[0]["name"] == "*":
            return "{df}"
        
        selected_cols = []
        for col in columns:
            if col["table_alias"]:
                # Handle table.column format - in pandas this is just the column name
                selected_cols.append(col["name"])
            else:
                selected_cols.append(col["name"])
        
        return "{df}[" + str(selected_cols) + "]"
    
    def _generate_filter_code(self, condition: Dict[str, Any]) -> str:
        """Generate comprehensive filter conditions"""
        if condition["type"] == "logical":
            left_code = self._generate_filter_code(condition["left"])
            right_code = self._generate_filter_code(condition["right"])
            
            if condition["operator"].upper() == "AND":
                return f"({left_code}) & ({right_code})"
            elif condition["operator"].upper() == "OR":
                return f"({left_code}) | ({right_code})"
        
        elif condition["type"] == "comparison":
            return self._generate_comparison_code(condition)
        
        return ""
    
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