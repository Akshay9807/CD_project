#!/usr/bin/env python3
"""
Debug code generation for CASE statements
"""

from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
from advanced_code_generator import AdvancedCodeGenerator
import pandas as pd

def debug_code_generation():
    """Debug code generation for CASE statements"""
    
    # Create test data
    enrollments_data = {
        'instructor': ['Dr. Smith', 'Dr. Smith', 'Dr. Brown', 'Dr. Brown'],
        'grade': ['A', 'B', 'A', 'C']
    }
    enrollments = pd.DataFrame(enrollments_data)
    
    datasets = {'enrollments': enrollments}
    
    # Simple CASE query
    sql_query = """
    SELECT instructor, 
           AVG(CASE grade 
               WHEN 'A' THEN 4 
               WHEN 'B' THEN 3 
               WHEN 'C' THEN 2
               END) AS avg_gpa
    FROM enrollments
    GROUP BY instructor
    """
    
    print("Debugging code generation:")
    print(sql_query)
    print("=" * 60)
    
    try:
        # Parse the query
        lexer = SQLLexer()
        parser = SQLParser()
        ir_gen = IRGenerator()
        code_gen = AdvancedCodeGenerator()
        
        tokens = lexer.tokenize(sql_query)
        ast = parser.parse(tokens)
        ir = ir_gen.generate(ast)
        
        print("IR columns:")
        for i, col in enumerate(ir["columns"]):
            print(f"  Column {i}: {col}")
            if col.get("expression"):
                print(f"    Has expression: {col['expression']['type']}")
        
        print(f"\nHas GROUP BY: {bool(ir.get('group_by'))}")
        print(f"Has aggregate functions: {code_gen._has_aggregate_functions(ir['columns'])}")
        print(f"Has expressions: {code_gen._has_expressions(ir['columns'])}")
        
        # Generate code
        print("\nGenerating code...")
        pandas_code = code_gen.generate(ir, datasets)
        
        print("Generated code:")
        for line in pandas_code.split('\n'):
            print(f"  {line}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_code_generation()