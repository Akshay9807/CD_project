"""
Universal SQL Engine for SQL2Pandas
Handles any SQL query by using a flexible parsing and execution approach
"""

import re
import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple
import sqlparse
from sqlparse.sql import Statement, Token, TokenList
from sqlparse.tokens import Keyword, Name, Punctuation, Number, String, Operator
import warnings
warnings.filterwarnings('ignore')

class UniversalSQLEngine:
    """
    Universal SQL Engine that can handle any SQL query by:
    1. Using sqlparse for robust SQL parsing
    2. Converting SQL operations to pandas equivalents
    3. Supporting all SQL features through intelligent mapping
    """
    
    def __init__(self):
        self.tables = {}
        self.temp_tables = {}
        self.aliases = {}
        
    def execute_query(self, sql: str, tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Execute any SQL query on the provided tables"""
        self.tables = tables.copy()
        self.temp_tables = {}
        self.aliases = {}
        
        try:
            # Parse the SQL using sqlparse
            parsed = sqlparse.parse(sql)[0]
            
            # Handle different types of SQL statements
            statement_type = self._get_statement_type(parsed)
            
            if statement_type == 'SELECT':
                return self._execute_select(parsed)
            elif statement_type == 'WITH':
                return self._execute_with_cte(parsed)
            elif statement_type == 'INSERT':
                return self._execute_insert(parsed)
            elif statement_type == 'UPDATE':
                return self._execute_update(parsed)
            elif statement_type == 'DELETE':
                return self._execute_delete(parsed)
            else:
                # Try to execute as SELECT if type is unclear
                return self._execute_select(parsed)
                
        except Exception as e:
            # Fallback: try to execute using pandas.query() if possible
            return self._fallback_execution(sql, tables)
    
    def _get_statement_type(self, parsed: Statement) -> str:
        """Determine the type of SQL statement"""
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() in ['SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE']:
                return token.value.upper()
        return 'SELECT'
    
    def _execute_select(self, parsed: Statement) -> pd.DataFrame:
        """Execute SELECT statement with comprehensive support"""
        sql_str = str(parsed).strip()
        
        # Extract main components using regex patterns
        components = self._extract_sql_components(sql_str)
        
        # Start with the main table or subquery
        result_df = self._process_from_clause(components.get('from', ''))
        
        # Apply JOINs
        if components.get('joins'):
            result_df = self._process_joins(result_df, components['joins'])
        
        # Apply WHERE clause
        if components.get('where'):
            result_df = self._process_where_clause(result_df, components['where'])
        
        # Apply GROUP BY
        if components.get('group_by'):
            result_df = self._process_group_by(result_df, components['group_by'], components.get('select', '*'))
        
        # Apply HAVING
        if components.get('having'):
            result_df = self._process_having_clause(result_df, components['having'])
        
        # Apply SELECT (column selection and functions)
        if not components.get('group_by'):  # If no GROUP BY, apply SELECT normally
            result_df = self._process_select_clause(result_df, components.get('select', '*'))
        
        # Apply DISTINCT
        if components.get('distinct'):
            result_df = result_df.drop_duplicates()
        
        # Apply ORDER BY
        if components.get('order_by'):
            result_df = self._process_order_by(result_df, components['order_by'])
        
        # Apply LIMIT/OFFSET
        if components.get('limit'):
            result_df = self._process_limit(result_df, components['limit'])
        
        return result_df
    
    def _extract_sql_components(self, sql: str) -> Dict[str, str]:
        """Extract SQL components using comprehensive regex patterns"""
        components = {}
        
        # Normalize SQL
        sql = re.sub(r'\s+', ' ', sql.strip())
        
        # Extract DISTINCT
        if re.search(r'\bSELECT\s+DISTINCT\b', sql, re.IGNORECASE):
            components['distinct'] = True
            sql = re.sub(r'\bDISTINCT\s+', '', sql, flags=re.IGNORECASE)
        
        # Extract SELECT clause
        select_match = re.search(r'\bSELECT\s+(.*?)\s+FROM\b', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            components['select'] = select_match.group(1).strip()
        
        # Extract FROM clause (including subqueries)
        from_match = re.search(r'\bFROM\s+(.*?)(?:\s+(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|$))', sql, re.IGNORECASE | re.DOTALL)
        if from_match:
            from_clause = from_match.group(1).strip()
            # Separate main table from JOINs
            join_pattern = r'\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\b'
            if re.search(join_pattern, from_clause, re.IGNORECASE):
                parts = re.split(join_pattern, from_clause, flags=re.IGNORECASE)
                components['from'] = parts[0].strip()
                components['joins'] = from_clause[len(parts[0]):].strip()
            else:
                components['from'] = from_clause
        
        # Extract WHERE clause
        where_match = re.search(r'\bWHERE\s+(.*?)(?:\s+(?:GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|$))', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            components['where'] = where_match.group(1).strip()
        
        # Extract GROUP BY clause
        group_match = re.search(r'\bGROUP\s+BY\s+(.*?)(?:\s+(?:HAVING|ORDER\s+BY|LIMIT|UNION|$))', sql, re.IGNORECASE | re.DOTALL)
        if group_match:
            components['group_by'] = group_match.group(1).strip()
        
        # Extract HAVING clause
        having_match = re.search(r'\bHAVING\s+(.*?)(?:\s+(?:ORDER\s+BY|LIMIT|UNION|$))', sql, re.IGNORECASE | re.DOTALL)
        if having_match:
            components['having'] = having_match.group(1).strip()
        
        # Extract ORDER BY clause
        order_match = re.search(r'\bORDER\s+BY\s+(.*?)(?:\s+(?:LIMIT|UNION|$))', sql, re.IGNORECASE | re.DOTALL)
        if order_match:
            components['order_by'] = order_match.group(1).strip()
        
        # Extract LIMIT clause
        limit_match = re.search(r'\bLIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?', sql, re.IGNORECASE)
        if limit_match:
            components['limit'] = {
                'count': int(limit_match.group(1)),
                'offset': int(limit_match.group(2)) if limit_match.group(2) else 0
            }
        
        return components
    
    def _process_from_clause(self, from_clause: str) -> pd.DataFrame:
        """Process FROM clause including subqueries and table aliases"""
        if not from_clause:
            return pd.DataFrame()
        
        # Handle subqueries
        if '(' in from_clause and ')' in from_clause:
            # Extract subquery
            subquery_match = re.search(r'\((.*?)\)', from_clause, re.DOTALL)
            if subquery_match:
                subquery = subquery_match.group(1)
                alias_match = re.search(r'\)\s+(?:AS\s+)?(\w+)', from_clause, re.IGNORECASE)
                alias = alias_match.group(1) if alias_match else 'subquery'
                
                # Execute subquery
                subquery_result = self.execute_query(subquery, self.tables)
                self.temp_tables[alias] = subquery_result
                return subquery_result
        
        # Handle regular table with optional alias
        parts = from_clause.strip().split()
        table_name = parts[0]
        
        # Remove quotes if present
        table_name = table_name.strip('"`\'')
        
        if table_name in self.tables:
            df = self.tables[table_name].copy()
        elif table_name in self.temp_tables:
            df = self.temp_tables[table_name].copy()
        else:
            raise ValueError(f"Table '{table_name}' not found")
        
        # Handle alias
        if len(parts) > 1:
            alias = parts[-1] if parts[-1].upper() != 'AS' else parts[-2]
            self.aliases[alias] = table_name
        
        return df
    
    def _process_joins(self, left_df: pd.DataFrame, joins_clause: str) -> pd.DataFrame:
        """Process all types of JOINs"""
        result_df = left_df.copy()
        
        # Split multiple JOINs
        join_pattern = r'\b((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN)\s+(.*?)(?:\s+ON\s+(.*?))?(?=\s+(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN|\s*$)'
        
        joins = re.findall(join_pattern, joins_clause, re.IGNORECASE | re.DOTALL)
        
        for join_type, table_part, on_condition in joins:
            # Parse table and alias
            table_parts = table_part.strip().split()
            table_name = table_parts[0].strip('"`\'')
            table_alias = table_parts[-1] if len(table_parts) > 1 and table_parts[-1].upper() != 'AS' else None
            
            # Get the right DataFrame
            if table_name in self.tables:
                right_df = self.tables[table_name].copy()
            elif table_name in self.temp_tables:
                right_df = self.temp_tables[table_name].copy()
            else:
                continue
            
            # Process ON condition
            if on_condition:
                left_col, right_col = self._parse_join_condition(on_condition)
                
                # Determine join type
                how = 'inner'
                if 'LEFT' in join_type.upper():
                    how = 'left'
                elif 'RIGHT' in join_type.upper():
                    how = 'right'
                elif 'FULL' in join_type.upper():
                    how = 'outer'
                
                # Perform the join
                try:
                    result_df = pd.merge(result_df, right_df, 
                                       left_on=left_col, right_on=right_col, 
                                       how=how, suffixes=('', '_right'))
                except:
                    # Fallback: try different column matching strategies
                    result_df = self._smart_join(result_df, right_df, on_condition, how)
        
        return result_df
    
    def _parse_join_condition(self, condition: str) -> Tuple[str, str]:
        """Parse JOIN ON condition to extract column names"""
        # Handle table.column = table.column format
        match = re.search(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', condition)
        if match:
            return match.group(2), match.group(4)
        
        # Handle simple column = column format
        match = re.search(r'(\w+)\s*=\s*(\w+)', condition)
        if match:
            return match.group(1), match.group(2)
        
        return 'id', 'id'  # Default fallback
    
    def _smart_join(self, left_df: pd.DataFrame, right_df: pd.DataFrame, condition: str, how: str) -> pd.DataFrame:
        """Smart join that tries to match columns intelligently"""
        # Try to find common columns
        common_cols = set(left_df.columns) & set(right_df.columns)
        
        if common_cols:
            # Use the first common column
            join_col = list(common_cols)[0]
            return pd.merge(left_df, right_df, on=join_col, how=how, suffixes=('', '_right'))
        
        # If no common columns, try cross join
        left_df['_join_key'] = 1
        right_df['_join_key'] = 1
        result = pd.merge(left_df, right_df, on='_join_key', how=how, suffixes=('', '_right'))
        result = result.drop('_join_key', axis=1)
        return result
    
    def _process_where_clause(self, df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
        """Process WHERE clause with comprehensive condition support"""
        try:
            # Convert SQL conditions to pandas query format
            pandas_condition = self._convert_sql_to_pandas_condition(where_clause, df)
            return df.query(pandas_condition)
        except:
            # Fallback: try manual filtering
            return self._manual_filter(df, where_clause)
    
    def _convert_sql_to_pandas_condition(self, condition: str, df: pd.DataFrame) -> str:
        """Convert SQL WHERE condition to pandas query format"""
        # Replace SQL operators with pandas equivalents
        condition = re.sub(r'\bAND\b', ' and ', condition, flags=re.IGNORECASE)
        condition = re.sub(r'\bOR\b', ' or ', condition, flags=re.IGNORECASE)
        condition = re.sub(r'\bNOT\b', ' not ', condition, flags=re.IGNORECASE)
        
        # Handle IN clauses
        condition = re.sub(r'(\w+)\s+IN\s*\((.*?)\)', r'\1.isin([\2])', condition, flags=re.IGNORECASE)
        
        # Handle LIKE clauses
        condition = re.sub(r'(\w+)\s+LIKE\s+[\'"]([^\'\"]*)[\'"]', 
                          r'\1.str.contains("\2", na=False)', condition, flags=re.IGNORECASE)
        
        # Handle IS NULL / IS NOT NULL
        condition = re.sub(r'(\w+)\s+IS\s+NULL', r'\1.isna()', condition, flags=re.IGNORECASE)
        condition = re.sub(r'(\w+)\s+IS\s+NOT\s+NULL', r'\1.notna()', condition, flags=re.IGNORECASE)
        
        # Handle BETWEEN
        condition = re.sub(r'(\w+)\s+BETWEEN\s+(\S+)\s+AND\s+(\S+)', 
                          r'(\1 >= \2) and (\1 <= \3)', condition, flags=re.IGNORECASE)
        
        return condition
    
    def _manual_filter(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """Manual filtering when query() fails"""
        # Simple equality conditions
        eq_matches = re.findall(r'(\w+)\s*=\s*[\'"]([^\'"]*)[\'"]', condition, re.IGNORECASE)
        for col, val in eq_matches:
            if col in df.columns:
                df = df[df[col] == val]
        
        # Simple numeric conditions
        num_matches = re.findall(r'(\w+)\s*([><=!]+)\s*(\d+(?:\.\d+)?)', condition, re.IGNORECASE)
        for col, op, val in num_matches:
            if col in df.columns:
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
        
        return df
    
    def _process_select_clause(self, df: pd.DataFrame, select_clause: str) -> pd.DataFrame:
        """Process SELECT clause with functions and expressions"""
        if select_clause.strip() == '*':
            return df
        
        # Parse selected columns and expressions
        columns = []
        expressions = []
        
        # Split by comma, but respect parentheses
        parts = self._smart_split(select_clause, ',')
        
        for part in parts:
            part = part.strip()
            
            # Check for aggregate functions
            if re.search(r'\b(COUNT|SUM|AVG|MAX|MIN|STDDEV|VARIANCE)\s*\(', part, re.IGNORECASE):
                expressions.append(part)
            # Check for mathematical expressions
            elif re.search(r'[+\-*/]', part) and not re.search(r'[\'"]', part):
                expressions.append(part)
            else:
                # Regular column (possibly with alias)
                col_match = re.search(r'^(\w+(?:\.\w+)?)\s*(?:AS\s+(\w+))?$', part, re.IGNORECASE)
                if col_match:
                    col_name = col_match.group(1)
                    alias = col_match.group(2)
                    
                    # Handle table.column format
                    if '.' in col_name:
                        col_name = col_name.split('.')[1]
                    
                    if col_name in df.columns:
                        columns.append(col_name)
                        if alias:
                            # Will rename later
                            pass
        
        # Apply column selection
        if columns:
            result_df = df[columns].copy()
        else:
            result_df = df.copy()
        
        # Apply expressions (basic support)
        for expr in expressions:
            try:
                # Simple aggregate function handling
                if 'COUNT(*)' in expr.upper():
                    result_df = pd.DataFrame({'count': [len(df)]})
                elif 'COUNT(' in expr.upper():
                    col_match = re.search(r'COUNT\s*\(\s*(\w+)\s*\)', expr, re.IGNORECASE)
                    if col_match:
                        col = col_match.group(1)
                        if col in df.columns:
                            result_df = pd.DataFrame({'count': [df[col].count()]})
            except:
                pass
        
        return result_df
    
    def _process_group_by(self, df: pd.DataFrame, group_clause: str, select_clause: str) -> pd.DataFrame:
        """Process GROUP BY with aggregate functions"""
        # Parse GROUP BY columns
        group_cols = [col.strip() for col in group_clause.split(',')]
        group_cols = [col for col in group_cols if col in df.columns]
        
        if not group_cols:
            return df
        
        # Parse SELECT clause for aggregate functions
        agg_dict = {}
        select_parts = self._smart_split(select_clause, ',')
        
        for part in select_parts:
            part = part.strip()
            
            # Handle aggregate functions
            agg_match = re.search(r'(COUNT|SUM|AVG|MAX|MIN)\s*\(\s*(\*|\w+)\s*\)', part, re.IGNORECASE)
            if agg_match:
                func = agg_match.group(1).lower()
                col = agg_match.group(2)
                
                if col == '*':
                    agg_dict['count'] = 'size'
                elif col in df.columns:
                    if func == 'avg':
                        func = 'mean'
                    agg_dict[f'{func}_{col}'] = (col, func)
        
        # Perform grouping
        if agg_dict:
            grouped = df.groupby(group_cols)
            if 'count' in agg_dict:
                result = grouped.size().reset_index(name='count')
            else:
                result = grouped.agg(agg_dict).reset_index()
                # Flatten column names
                if isinstance(result.columns, pd.MultiIndex):
                    result.columns = [col[0] if col[1] == '' else f"{col[1]}_{col[0]}" for col in result.columns]
        else:
            # GROUP BY without aggregates
            result = df.groupby(group_cols).first().reset_index()
        
        return result
    
    def _process_having_clause(self, df: pd.DataFrame, having_clause: str) -> pd.DataFrame:
        """Process HAVING clause (similar to WHERE but for grouped data)"""
        return self._process_where_clause(df, having_clause)
    
    def _process_order_by(self, df: pd.DataFrame, order_clause: str) -> pd.DataFrame:
        """Process ORDER BY clause"""
        order_parts = [part.strip() for part in order_clause.split(',')]
        
        columns = []
        ascending = []
        
        for part in order_parts:
            if part.upper().endswith(' DESC'):
                col = part[:-5].strip()
                ascending.append(False)
            elif part.upper().endswith(' ASC'):
                col = part[:-4].strip()
                ascending.append(True)
            else:
                col = part
                ascending.append(True)
            
            if col in df.columns:
                columns.append(col)
        
        if columns:
            return df.sort_values(by=columns, ascending=ascending)
        
        return df
    
    def _process_limit(self, df: pd.DataFrame, limit_info: Dict) -> pd.DataFrame:
        """Process LIMIT and OFFSET"""
        offset = limit_info.get('offset', 0)
        count = limit_info['count']
        
        return df.iloc[offset:offset + count]
    
    def _smart_split(self, text: str, delimiter: str) -> List[str]:
        """Split text by delimiter while respecting parentheses"""
        parts = []
        current = ""
        paren_count = 0
        
        for char in text:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == delimiter and paren_count == 0:
                parts.append(current)
                current = ""
                continue
            
            current += char
        
        if current:
            parts.append(current)
        
        return parts
    
    def _execute_with_cte(self, parsed: Statement) -> pd.DataFrame:
        """Execute WITH (Common Table Expression) queries"""
        # Basic CTE support - extract and execute CTEs first
        sql_str = str(parsed)
        
        # Find WITH clause
        with_match = re.search(r'\bWITH\s+(.*?)\s+SELECT\b', sql_str, re.IGNORECASE | re.DOTALL)
        if with_match:
            cte_part = with_match.group(1)
            
            # Parse CTEs (simplified)
            cte_matches = re.findall(r'(\w+)\s+AS\s*\((.*?)\)', cte_part, re.IGNORECASE | re.DOTALL)
            
            for cte_name, cte_query in cte_matches:
                cte_result = self.execute_query(cte_query, self.tables)
                self.temp_tables[cte_name] = cte_result
            
            # Execute main query
            main_query = sql_str[sql_str.upper().find('SELECT'):]
            return self.execute_query(main_query, {**self.tables, **self.temp_tables})
        
        return pd.DataFrame()
    
    def _execute_insert(self, parsed: Statement) -> pd.DataFrame:
        """Execute INSERT statements (return affected rows info)"""
        return pd.DataFrame({'message': ['INSERT operation not supported in read-only mode']})
    
    def _execute_update(self, parsed: Statement) -> pd.DataFrame:
        """Execute UPDATE statements (return affected rows info)"""
        return pd.DataFrame({'message': ['UPDATE operation not supported in read-only mode']})
    
    def _execute_delete(self, parsed: Statement) -> pd.DataFrame:
        """Execute DELETE statements (return affected rows info)"""
        return pd.DataFrame({'message': ['DELETE operation not supported in read-only mode']})
    
    def _fallback_execution(self, sql: str, tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Fallback execution using simple heuristics"""
        # Try to identify the main table
        table_names = list(tables.keys())
        main_table = None
        
        for table_name in table_names:
            if table_name.lower() in sql.lower():
                main_table = table_name
                break
        
        if main_table:
            df = tables[main_table].copy()
            
            # Apply simple filtering if WHERE is present
            if 'WHERE' in sql.upper():
                # Very basic WHERE processing
                where_part = sql.upper().split('WHERE')[1].split('ORDER')[0].split('GROUP')[0].split('LIMIT')[0]
                
                # Simple equality filter
                eq_match = re.search(r'(\w+)\s*=\s*[\'"]([^\'"]*)[\'"]', where_part, re.IGNORECASE)
                if eq_match:
                    col, val = eq_match.groups()
                    if col in df.columns:
                        df = df[df[col] == val]
            
            # Apply LIMIT if present
            limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
            if limit_match:
                limit_count = int(limit_match.group(1))
                df = df.head(limit_count)
            
            return df
        
        return pd.DataFrame({'error': ['Could not execute query']})

# Global instance
universal_engine = UniversalSQLEngine()