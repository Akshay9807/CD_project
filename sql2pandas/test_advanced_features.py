#!/usr/bin/env python3
"""
Advanced test script for SQL2Pandas Compiler
Tests comprehensive SQL features including JOINs, aggregates, and complex conditions
"""

import pandas as pd
from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
from advanced_code_generator import AdvancedCodeGenerator
from executor import PandasExecutor

def test_advanced_query(sql_query, datasets, description=""):
    """Test an advanced SQL query through the entire pipeline"""
    print(f"\n{'='*80}")
    print(f"Testing: {description}")
    print(f"Query: {sql_query}")
    print('='*80)
    
    try:
        # Initialize components
        lexer = SQLLexer()
        parser = SQLParser()
        ir_gen = IRGenerator()
        code_gen = AdvancedCodeGenerator()
        executor = PandasExecutor()
        
        # Phase 1: Lexical Analysis
        print("\n1. Lexical Analysis:")
        tokens = lexer.tokenize(sql_query)
        print(f"   Generated {len(tokens)} tokens")
        
        # Phase 2: Syntax Analysis
        print("\n2. Syntax Analysis:")
        ast = parser.parse(tokens)
        print(f"   Parse tree: {type(ast).__name__}")
        print(f"   Main table: {ast.from_clause.table}")
        if ast.from_clause.joins:
            print(f"   JOINs: {len(ast.from_clause.joins)}")
        if ast.distinct:
            print(f"   DISTINCT: Yes")
        
        # Phase 3: IR Generation
        print("\n3. Intermediate Representation:")
        ir = ir_gen.generate(ast)
        print(f"   Operation: {ir['operation']}")
        
        # Phase 4: Code Generation
        print("\n4. Code Generation:")
        pandas_code = code_gen.generate(ir, datasets)
        print("   Generated Pandas code:")
        for line in pandas_code.split('\n'):
            print(f"   {line}")
        
        # Phase 5: Execution
        print("\n5. Execution:")
        result = executor.execute(pandas_code, datasets)
        print(f"   Result shape: {result.shape}")
        print("   Result preview:")
        print(result.to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run comprehensive advanced tests"""
    print("SQL2Pandas Advanced Features Test Suite")
    print("=" * 80)
    
    # Load test data
    import os
    datasets = {}
    sample_dir = os.path.join(os.path.dirname(__file__), 'sample_data')
    
    if os.path.exists(sample_dir):
        for file in os.listdir(sample_dir):
            if file.endswith('.csv'):
                table_name = file[:-4]
                file_path = os.path.join(sample_dir, file)
                try:
                    datasets[table_name] = pd.read_csv(file_path)
                    df = datasets[table_name]
                    print(f"Loaded {table_name}: {df.shape[0]} rows, {df.shape[1]} columns")
                except Exception as e:
                    print(f"Error loading {file}: {e}")
    
    print(f"\nTotal datasets loaded: {len(datasets)}")
    print("Available tables:", list(datasets.keys()))
    
    # Advanced test queries
    test_queries = [
        # Basic WHERE conditions
        ("SELECT name, age FROM students WHERE age > 20", "Basic WHERE with comparison"),
        ("SELECT * FROM students WHERE grade = 'A'", "WHERE with string equality"),
        ("SELECT name FROM students WHERE city IN ('New York', 'Chicago')", "WHERE with IN clause"),
        ("SELECT * FROM students WHERE name LIKE 'A%'", "WHERE with LIKE pattern"),
        ("SELECT * FROM students WHERE age BETWEEN 18 AND 22", "WHERE with BETWEEN"),
        ("SELECT * FROM students WHERE grade IS NOT NULL", "WHERE with IS NOT NULL"),
        
        # Complex WHERE conditions
        ("SELECT * FROM students WHERE age > 20 AND grade = 'A'", "Complex WHERE with AND"),
        ("SELECT * FROM students WHERE city = 'New York' OR city = 'Chicago'", "Complex WHERE with OR"),
        ("SELECT * FROM students WHERE (age > 20 AND grade = 'A') OR city = 'Phoenix'", "Complex WHERE with parentheses"),
        
        # Aggregate functions
        ("SELECT COUNT(*) FROM students", "COUNT(*) aggregate"),
        ("SELECT AVG(age) FROM students", "AVG aggregate function"),
        ("SELECT MAX(age), MIN(age) FROM students", "Multiple aggregates"),
        ("SELECT SUM(credits) FROM courses", "SUM aggregate function"),
        
        # GROUP BY
        ("SELECT grade, COUNT(*) FROM students GROUP BY grade", "GROUP BY with COUNT"),
        ("SELECT city, AVG(age) FROM students GROUP BY city", "GROUP BY with AVG"),
        ("SELECT instructor, COUNT(*) FROM courses GROUP BY instructor", "GROUP BY on courses"),
        
        # HAVING clause
        ("SELECT grade, COUNT(*) FROM students GROUP BY grade HAVING COUNT(*) > 1", "GROUP BY with HAVING"),
        ("SELECT city, AVG(age) FROM students GROUP BY city HAVING AVG(age) > 20", "HAVING with aggregate condition"),
        
        # ORDER BY
        ("SELECT * FROM students ORDER BY age DESC", "ORDER BY single column DESC"),
        ("SELECT * FROM students ORDER BY grade ASC, age DESC", "ORDER BY multiple columns"),
        ("SELECT name, age FROM students ORDER BY age", "ORDER BY with column selection"),
        
        # DISTINCT
        ("SELECT DISTINCT city FROM students", "DISTINCT values"),
        ("SELECT DISTINCT grade FROM students ORDER BY grade", "DISTINCT with ORDER BY"),
        
        # LIMIT
        ("SELECT * FROM students ORDER BY age DESC LIMIT 3", "LIMIT with ORDER BY"),
        ("SELECT name, age FROM students LIMIT 5", "Simple LIMIT"),
        
        # JOINs (if we have related tables)
        ("SELECT s.name, e.grade FROM students s JOIN enrollments e ON s.id = e.student_id", "INNER JOIN"),
        ("SELECT s.name, e.course_id FROM students s LEFT JOIN enrollments e ON s.id = e.student_id", "LEFT JOIN"),
        ("SELECT c.course_name, e.grade FROM courses c JOIN enrollments e ON c.course_id = e.course_id", "JOIN courses and enrollments"),
        
        # Complex queries combining multiple features
        ("SELECT s.name, COUNT(e.course_id) FROM students s LEFT JOIN enrollments e ON s.id = e.student_id GROUP BY s.name", "JOIN with GROUP BY"),
        ("SELECT c.course_name, COUNT(e.student_id) FROM courses c LEFT JOIN enrollments e ON c.course_id = e.course_id GROUP BY c.course_name HAVING COUNT(e.student_id) > 0", "JOIN with GROUP BY and HAVING"),
        ("SELECT DISTINCT s.city FROM students s JOIN enrollments e ON s.id = e.student_id WHERE e.grade = 'A'", "JOIN with WHERE and DISTINCT"),
    ]
    
    passed = 0
    total = len(test_queries)
    
    for query, description in test_queries:
        if test_advanced_query(query, datasets, description):
            passed += 1
    
    print(f"\n{'='*80}")
    print(f"Advanced Test Results: {passed}/{total} queries passed")
    print('='*80)
    
    if passed == total:
        print("🎉 All advanced tests passed! The compiler supports comprehensive SQL features.")
    else:
        print(f"⚠️  {total - passed} test(s) failed. Check the errors above.")

if __name__ == "__main__":
    main()