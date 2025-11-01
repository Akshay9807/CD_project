#!/usr/bin/env python3
"""
Test script for SQL2Pandas Compiler
"""

import pandas as pd
from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
from code_generator import CodeGenerator
from executor import PandasExecutor

def test_query(sql_query, datasets):
    """Test a single SQL query through the entire pipeline"""
    print(f"\n{'='*60}")
    print(f"Testing: {sql_query}")
    print('='*60)
    
    try:
        # Initialize components
        lexer = SQLLexer()
        parser = SQLParser()
        ir_gen = IRGenerator()
        code_gen = CodeGenerator()
        executor = PandasExecutor()
        
        # Phase 1: Lexical Analysis
        print("\n1. Lexical Analysis:")
        tokens = lexer.tokenize(sql_query)
        print(f"   Generated {len(tokens)} tokens")
        for i, token in enumerate(tokens[:5]):  # Show first 5 tokens
            print(f"   {i+1}. {token.type.value}: '{token.value}'")
        if len(tokens) > 5:
            print(f"   ... and {len(tokens)-5} more tokens")
        
        # Phase 2: Syntax Analysis
        print("\n2. Syntax Analysis:")
        ast = parser.parse(tokens)
        print(f"   Parse tree: {type(ast).__name__}")
        print(f"   Columns: {[col.name for col in ast.columns]}")
        print(f"   Table: {ast.table}")
        
        # Check if table exists
        if ast.table not in datasets:
            print(f"\n❌ Error: Table '{ast.table}' not found in datasets")
            print(f"   Available tables: {list(datasets.keys())}")
            return False
        
        df = datasets[ast.table]
        
        # Phase 3: IR Generation
        print("\n3. Intermediate Representation:")
        ir = ir_gen.generate(ast)
        print(f"   Operation: {ir['operation']}")
        print(f"   Columns: {ir['columns']}")
        if ir['filters']:
            print(f"   Has filters: Yes")
        if ir['ordering']:
            print(f"   Ordering: {ir['ordering']['column']} {ir['ordering']['direction']}")
        
        # Phase 4: Code Generation
        print("\n4. Code Generation:")
        pandas_code = code_gen.generate(ir)
        print("   Generated Pandas code:")
        for line in pandas_code.split('\n'):
            print(f"   {line}")
        
        # Phase 5: Execution
        print("\n5. Execution:")
        result = executor.execute(pandas_code, df)
        print(f"   Result shape: {result.shape}")
        print("   Result preview:")
        print(result.to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return False

def main():
    """Run comprehensive tests"""
    print("SQL2Pandas Compiler Test Suite")
    print("=" * 60)
    
    # Load all test data
    import os
    datasets = {}
    sample_dir = os.path.join(os.path.dirname(__file__), 'sample_data')
    
    if os.path.exists(sample_dir):
        for file in os.listdir(sample_dir):
            if file.endswith('.csv'):
                table_name = file[:-4]  # Remove .csv extension
                file_path = os.path.join(sample_dir, file)
                try:
                    datasets[table_name] = pd.read_csv(file_path)
                    df = datasets[table_name]
                    print(f"Loaded {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
                    print(f"  Columns: {list(df.columns)}")
                except Exception as e:
                    print(f"Error loading {file}: {e}")
    
    if not datasets:
        print("No CSV files found in sample_data directory!")
        return
    
    print(f"\nTotal datasets loaded: {len(datasets)}")
    print("Available tables:", list(datasets.keys()))
    
    # Test queries
    test_queries = [
        "SELECT name, age FROM students WHERE age > 20 ORDER BY age DESC",
        "SELECT name FROM students WHERE grade = 'A'",
        "SELECT * FROM students WHERE age >= 21 AND grade != 'C'",
        "SELECT name, city FROM students WHERE city = 'New York' OR city = 'Chicago'",
        "SELECT name, age, grade FROM students ORDER BY name ASC",
        "SELECT * FROM courses WHERE credits > 3",
        "SELECT course_name, instructor FROM courses ORDER BY course_name",
        "SELECT * FROM enrollments WHERE grade = 'A'"
    ]
    
    passed = 0
    total = len(test_queries)
    
    for query in test_queries:
        if test_query(query, datasets):
            passed += 1
    
    print(f"\n{'='*60}")
    print(f"Test Results: {passed}/{total} queries passed")
    print('='*60)
    
    if passed == total:
        print("🎉 All tests passed! The compiler is working correctly.")
    else:
        print(f"⚠️  {total - passed} test(s) failed. Check the errors above.")

if __name__ == "__main__":
    main()