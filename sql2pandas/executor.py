import pandas as pd
from typing import Any, Dict, Union
import sys
from io import StringIO

class ExecutionError(Exception):
    pass

class PandasExecutor:
    """
    Executes generated Pandas code safely and returns results.
    Supports multiple DataFrames for JOIN operations.
    """
    
    def execute(self, code: str, data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
        """
        Execute Pandas code with the provided DataFrame(s).
        
        Args:
            code: Generated Pandas code string
            data: Input DataFrame or dictionary of DataFrames for multiple tables
            
        Returns:
            Result DataFrame after executing the code
        """
        try:
            # Handle single DataFrame (backward compatibility)
            if isinstance(data, pd.DataFrame):
                df = data
                datasets = {'df': df}
            else:
                # Multiple DataFrames
                datasets = data.copy()
                # Set primary df for backward compatibility
                df = list(datasets.values())[0] if datasets else pd.DataFrame()
            
            # Validate data
            self._validate_data(datasets, code)
            
            # Create execution environment
            exec_globals = {
                'pd': pd,
                'result': None,
                'df': df  # Primary DataFrame for backward compatibility
            }
            
            # Add all datasets to execution environment
            exec_globals.update(datasets)
            
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
    
    def _validate_data(self, datasets: Dict[str, pd.DataFrame], code: str) -> None:
        """
        Basic validation of DataFrames and code compatibility.
        """
        if not datasets:
            raise ExecutionError("No datasets provided")
        
        # Check if any dataset is empty
        for name, df in datasets.items():
            if df.empty:
                raise ExecutionError(f"Dataset '{name}' is empty")
        
        # Extract column references from code (basic check)
        import re
        column_pattern = r"result\['([^']+)'\]"
        referenced_columns = re.findall(column_pattern, code)
        
        # For single table queries, validate against primary DataFrame
        if len(datasets) == 1 and referenced_columns:
            df = list(datasets.values())[0]
            missing_columns = [col for col in referenced_columns if col not in df.columns]
            if missing_columns:
                available_cols = list(df.columns)
                raise ExecutionError(
                    f"Missing columns: {missing_columns}. "
                    f"Available columns: {available_cols}"
                )
    
    def get_execution_info(self, code: str, data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]) -> dict:
        """
        Get information about the execution without running it.
        """
        if isinstance(data, pd.DataFrame):
            datasets = {'primary': data}
        else:
            datasets = data
        
        total_memory = 0
        total_rows = 0
        all_columns = []
        
        for name, df in datasets.items():
            total_memory += df.memory_usage(deep=True).sum()
            total_rows += df.shape[0]
            all_columns.extend(df.columns.tolist())
        
        info = {
            "total_datasets": len(datasets),
            "total_rows": total_rows,
            "unique_columns": len(set(all_columns)),
            "all_columns": list(set(all_columns)),
            "code_lines": len(code.split('\n')),
            "estimated_memory_mb": round(total_memory / (1024 * 1024), 2),
            "datasets_info": {name: {"shape": df.shape, "columns": df.columns.tolist()} 
                            for name, df in datasets.items()}
        }
        
        return info