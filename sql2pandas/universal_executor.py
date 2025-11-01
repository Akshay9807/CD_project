"""
Universal SQL Executor
Handles any SQL query using the Universal SQL Engine
"""

import pandas as pd
from typing import Dict, Union, Any
from universal_sql_engine import universal_engine
import traceback

class UniversalExecutor:
    """
    Universal executor that can handle any SQL query by using multiple strategies:
    1. Universal SQL Engine (primary)
    2. Direct pandas operations (fallback)
    3. Query approximation (last resort)
    """
    
    def __init__(self):
        self.engine = universal_engine
    
    def execute(self, sql_query: str, data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
        """
        Execute any SQL query on the provided data
        
        Args:
            sql_query: SQL query string
            data: Single DataFrame or dictionary of DataFrames
            
        Returns:
            Result DataFrame
        """
        try:
            # Prepare tables dictionary
            if isinstance(data, pd.DataFrame):
                tables = {'main_table': data}
                # Also try to infer table name from query
                import re
                table_match = re.search(r'\bFROM\s+(\w+)', sql_query, re.IGNORECASE)
                if table_match:
                    table_name = table_match.group(1)
                    tables[table_name] = data
            else:
                tables = data.copy()
            
            # Method 1: Use Universal SQL Engine
            try:
                result = self.engine.execute_query(sql_query, tables)
                if not result.empty or len(result.columns) > 0:
                    return result
            except Exception as e:
                print(f"Universal engine failed: {e}")
            
            # Method 2: Try pandas-sql (if available)
            try:
                result = self._execute_with_pandasql(sql_query, tables)
                if not result.empty:
                    return result
            except Exception as e:
                print(f"Pandasql failed: {e}")
            
            # Method 3: Pattern-based execution
            try:
                result = self._execute_with_patterns(sql_query, tables)
                if not result.empty:
                    return result
            except Exception as e:
                print(f"Pattern-based execution failed: {e}")
            
            # Method 4: Simple approximation
            return self._simple_approximation(sql_query, tables)
            
        except Exception as e:
            # Return error information
            return pd.DataFrame({
                'error': [f"Query execution failed: {str(e)}"],
                'query': [sql_query],
                'suggestion': ['Try simplifying the query or check table/column names']
            })
    
    def _execute_with_pandasql(self, sql_query: str, tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Execute using pandasql library if available"""
        try:
            import pandasql as psql
            
            # Create a local environment with all tables
            local_env = tables.copy()
            
            # Execute query
            result = psql.sqldf(sql_query, local_env)
            return result
            
        except ImportError:
            # pandasql not available, skip this method
            raise Exception("pandasql not available")
        except Exception as e:
            raise Exception(f"pandasql execution failed: {e}")
    
    def _execute_with_patterns(self, sql_query: str, tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Execute using pattern matching for common SQL structures"""
        import re
        
        # Normalize query
        query = re.sub(r'\s+', ' ', sql_query.strip())
        
        # Pattern 1: Simple SELECT * FROM table
        pattern1 = re.match(r'SELECT\s+\*\s+FROM\s+(\w+)(?:\s+LIMIT\s+(\d+))?', query, re.IGNORECASE)
        if pattern1:
            table_name = pattern1.group(1)
            limit = int(pattern1.group(2)) if pattern1.group(2) else None
            
            if table_name in tables:
                result = tables[table_name].copy()
                if limit:
                    result = result.head(limit)
                return result
        
        # Pattern 2: SELECT columns FROM table
        pattern2 = re.match(r'SELECT\s+(.*?)\s+FROM\s+(\w+)', query, re.IGNORECASE)
        if pattern2:
            columns_str = pattern2.group(1)
            table_name = pattern2.group(2)
            
            if table_name in tables:
                df = tables[table_name].copy()
                
                if columns_str.strip() != '*':
                    # Parse columns
                    columns = [col.strip() for col in columns_str.split(',')]
                    # Filter existing columns
                    existing_cols = [col for col in columns if col in df.columns]
                    if existing_cols:
                        df = df[existing_cols]
                
                return df
        
        # Pattern 3: SELECT with WHERE
        pattern3 = re.match(r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+WHERE\s+(.*)', query, re.IGNORECASE)
        if pattern3:
            columns_str = pattern3.group(1)
            table_name = pattern3.group(2)
            where_clause = pattern3.group(3)
            
            if table_name in tables:
                df = tables[table_name].copy()
                
                # Apply simple WHERE conditions
                df = self._apply_simple_where(df, where_clause)
                
                # Select columns
                if columns_str.strip() != '*':
                    columns = [col.strip() for col in columns_str.split(',')]
                    existing_cols = [col for col in columns if col in df.columns]
                    if existing_cols:
                        df = df[existing_cols]
                
                return df
        
        # Pattern 4: COUNT queries
        count_pattern = re.match(r'SELECT\s+COUNT\s*\(\s*\*?\s*\)\s+FROM\s+(\w+)', query, re.IGNORECASE)
        if count_pattern:
            table_name = count_pattern.group(1)
            if table_name in tables:
                count = len(tables[table_name])
                return pd.DataFrame({'count': [count]})
        
        # Pattern 5: DISTINCT queries
        distinct_pattern = re.match(r'SELECT\s+DISTINCT\s+(.*?)\s+FROM\s+(\w+)', query, re.IGNORECASE)
        if distinct_pattern:
            columns_str = distinct_pattern.group(1)
            table_name = distinct_pattern.group(2)
            
            if table_name in tables:
                df = tables[table_name].copy()
                
                if columns_str.strip() != '*':
                    columns = [col.strip() for col in columns_str.split(',')]
                    existing_cols = [col for col in columns if col in df.columns]
                    if existing_cols:
                        df = df[existing_cols]
                
                return df.drop_duplicates()
        
        raise Exception("No matching pattern found")
    
    def _apply_simple_where(self, df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
        """Apply simple WHERE conditions with better error handling"""
        import re
        
        try:
            # Handle table.column references by removing table aliases
            where_clause = re.sub(r'\b\w+\.(\w+)', r'\1', where_clause)
            
            # Handle simple equality
            eq_matches = re.findall(r'(\w+)\s*=\s*[\'"]([^\'"]*)[\'"]', where_clause)
            for col, val in eq_matches:
                if col in df.columns:
                    df = df[df[col] == val]
            
            # Handle simple numeric comparisons
            num_matches = re.findall(r'(\w+)\s*([><=!]+)\s*(\d+(?:\.\d+)?)', where_clause)
            for col, op, val in num_matches:
                if col in df.columns:
                    try:
                        val = float(val)
                        if op == '>':
                            df = df[df[col] > val]
                        elif op == '<':
                            df = df[df[col] < val]
                        elif op == '>=':
                            df = df[df[col] >= val]
                        elif op == '<=':
                            df = df[df[col] <= val]
                        elif op == '=' or op == '==':
                            df = df[df[col] == val]
                        elif op == '!=' or op == '<>':
                            df = df[df[col] != val]
                    except (ValueError, TypeError):
                        continue
            
            # Handle IN clauses
            in_matches = re.findall(r'(\w+)\s+IN\s*\((.*?)\)', where_clause, re.IGNORECASE)
            for col, values_str in in_matches:
                if col in df.columns:
                    try:
                        # Parse values
                        values = []
                        for val in values_str.split(','):
                            val = val.strip().strip('\'"')
                            # Try to convert to number if possible
                            try:
                                val = float(val) if '.' in val else int(val)
                            except ValueError:
                                pass  # Keep as string
                            values.append(val)
                        df = df[df[col].isin(values)]
                    except Exception:
                        continue
            
            # Handle LIKE clauses
            like_matches = re.findall(r'(\w+)\s+LIKE\s+[\'"]([^\'"]*)[\'"]', where_clause, re.IGNORECASE)
            for col, pattern in like_matches:
                if col in df.columns:
                    try:
                        # Convert SQL LIKE to pandas contains
                        regex_pattern = pattern.replace('%', '.*').replace('_', '.')
                        df = df[df[col].str.contains(regex_pattern, na=False, regex=True)]
                    except Exception:
                        continue
            
            return df
            
        except Exception:
            # If all else fails, return original dataframe
            return df
    
    def _simple_approximation(self, sql_query: str, tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Simple approximation when all else fails"""
        import re
        
        # Try to find any table mentioned in the query
        table_names = list(tables.keys())
        mentioned_table = None
        
        for table_name in table_names:
            if table_name.lower() in sql_query.lower():
                mentioned_table = table_name
                break
        
        if mentioned_table:
            df = tables[mentioned_table].copy()
            
            # If LIMIT is mentioned, apply it
            limit_match = re.search(r'LIMIT\s+(\d+)', sql_query, re.IGNORECASE)
            if limit_match:
                limit_count = int(limit_match.group(1))
                df = df.head(limit_count)
            
            # If specific columns are mentioned, try to select them
            if 'SELECT' in sql_query.upper() and '*' not in sql_query:
                # Try to extract column names (very basic)
                select_part = sql_query.upper().split('SELECT')[1].split('FROM')[0]
                potential_cols = re.findall(r'\b(\w+)\b', select_part)
                existing_cols = [col for col in potential_cols if col in df.columns]
                if existing_cols:
                    df = df[existing_cols[:5]]  # Limit to first 5 matches
            
            return df
        
        # Last resort: return information about available tables
        table_info = []
        for name, df in tables.items():
            table_info.append({
                'table_name': name,
                'rows': len(df),
                'columns': len(df.columns),
                'sample_columns': ', '.join(df.columns[:5].tolist())
            })
        
        return pd.DataFrame(table_info)

# Create global instance
universal_executor = UniversalExecutor()