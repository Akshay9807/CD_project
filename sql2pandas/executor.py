import pandas as pd
from typing import Any
import sys
from io import StringIO

class ExecutionError(Exception):
    pass

class PandasExecutor:
    """
    Executes generated Pandas code safely and returns results.
    """
    
    def execute(self, code: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute Pandas code with the provided DataFrame.
        
        Args:
            code: Generated Pandas code string
            df: Input DataFrame
            
        Returns:
            Result DataFrame after executing the code
        """
        try:
            # Validate that required columns exist
            self._validate_dataframe(df, code)
            
            # Create execution environment
            exec_globals = {
                'df': df,
                'pd': pd,
                'result': None
            }
            
            # Execute the code
            exec(code, exec_globals)
            
            # Get the result
            result = exec_globals.get('result')
            
            if result is None:
                raise ExecutionError("Code execution did not produce a result")
            
            if not isinstance(result, pd.DataFrame):
                raise ExecutionError(f"Expected DataFrame, got {type(result)}")
            
            return result
            
        except KeyError as e:
            raise ExecutionError(f"Column not found: {str(e)}")
        except Exception as e:
            raise ExecutionError(f"Execution error: {str(e)}")
    
    def _validate_dataframe(self, df: pd.DataFrame, code: str) -> None:
        """
        Basic validation of DataFrame and code compatibility.
        """
        if df.empty:
            raise ExecutionError("Input DataFrame is empty")
        
        # Extract column references from code (basic check)
        import re
        column_pattern = r"result\['([^']+)'\]"
        referenced_columns = re.findall(column_pattern, code)
        
        missing_columns = [col for col in referenced_columns if col not in df.columns]
        if missing_columns:
            available_cols = list(df.columns)
            raise ExecutionError(
                f"Missing columns: {missing_columns}. "
                f"Available columns: {available_cols}"
            )
    
    def get_execution_info(self, code: str, df: pd.DataFrame) -> dict:
        """
        Get information about the execution without running it.
        """
        info = {
            "input_shape": df.shape,
            "input_columns": list(df.columns),
            "input_dtypes": df.dtypes.to_dict(),
            "code_lines": len(code.split('\n')),
            "estimated_memory": df.memory_usage(deep=True).sum()
        }
        
        return info