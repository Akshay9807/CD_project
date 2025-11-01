#!/usr/bin/env python3
"""
Test Traditional Compiler Coverage
Tests how many queries the traditional compiler can handle before falling back
"""

import pandas as pd
from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
from advanced_code_generator import AdvancedCodeGenerator
from executor import PandasExecutor
import os

def test_traditional_compiler(sql_query, datasets, description=""):
    """Test if traditional compiler can handle the query"""
    print(f"\nTesting: {description}")
    print(f"Query: {sql_query}")
    
    try:
        # Initialize components
        lexer = SQLLexer()
        parser = SQLParser()
        ir_gen = IRGenerator()
        code_gen = AdvancedCodeGenerator()
        executor = PandasExecutor()
        
        # Phase 1: Lexical Analysis
        tokens = lexer.tokenize(sql_query)
        
        # Phase 2: Syntax Analysis
        ast = parser.parse(tokens)
        
        # Phase 3: IR Generation
        ir = ir_gen.generate(ast)
        
        # Phase 4: Code Generation
        pandas_code = code_gen.generate(ir, datasets)
        
        # Phase 5: Execution
        result = executor.execute(pandas_code, datasets)
        
        print(f"✅ SUCCESS - Traditional compiler handled query")
        print(f"   Result shape: {result.shape}")
        return True
        
    except Exception as e:
        print(f"❌ FAILED - {str(e)}")
        return False

def main():
    """Test traditional compiler coverage"""
    print("Traditional Compiler Coverage Test")
    print("=" * 60)
    
    # Load test data
    datasets = {}
    sample_dir = "sample_data"
    
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
    else:
        # Create sample data if directory doesn't exist
        print("Creating sample data...")
        students_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
            'age': [22, 19, 21, 20, 23],
            'grade': ['A', 'B', 'A', 'C', 'B'],
            'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
        }
        datasets['students'] = pd.DataFrame(students_data)
        
        courses_data = {
            'course_id': ['CS101', 'MATH101', 'ENG101'],
            'course_name': ['Programming', 'Calculus', 'English'],
            'instructor': ['Dr. Smith', 'Dr. Brown', 'Prof. Davis'],
            'credits': [3, 4, 3]
        }
        datasets['courses'] = pd.DataFrame(courses_data)
        
        enrollments_data = {
            'student_id': [1, 2, 3, 1, 2],
            'course_id': ['CS101', 'CS101', 'MATH101', 'ENG101', 'MATH101'],
            'semester': ['Fall2023', 'Fall2023', 'Fall2023', 'Fall2023', 'Fall2023'],
            'grade': ['A', 'B', 'A', 'A', 'C']
        }
        datasets['enrollments'] = pd.DataFrame(enrollments_data)
    
    print(f"\nTotal datasets loaded: {len(datasets)}")
    for name, df in datasets.items():
        print(f"  {name}: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Test queries that SHOULD work with traditional compiler
    test_queries = [
        # Basic SELECT queries
        ("SELECT * FROM students", "Basic SELECT all"),
        ("SELECT name, age FROM students", "SELECT specific columns"),
        ("SELECT DISTINCT city FROM students", "SELECT DISTINCT"),
        
        # WHERE clauses
        ("SELECT * FROM students WHERE age > 20", "WHERE with comparison"),
        ("SELECT * FROM students WHERE grade = 'A'", "WHERE with equality"),
        ("SELECT * FROM students WHERE city IN ('New York', 'Chicago')", "WHERE with IN"),
        ("SELECT * FROM students WHERE name LIKE 'A%'", "WHERE with LIKE"),
        ("SELECT * FROM students WHERE age BETWEEN 18 AND 22", "WHERE with BETWEEN"),
        ("SELECT * FROM students WHERE age > 20 AND grade = 'A'", "WHERE with AND"),
        ("SELECT * FROM students WHERE city = 'New York' OR city = 'Chicago'", "WHERE with OR"),
        
        # Aggregate functions
        ("SELECT COUNT(*) FROM students", "COUNT all"),
        ("SELECT COUNT(name) FROM students", "COUNT column"),
        ("SELECT AVG(age) FROM students", "AVG function"),
        ("SELECT MAX(age), MIN(age) FROM students", "Multiple aggregates"),
        ("SELECT SUM(credits) FROM courses", "SUM function"),
        
        # GROUP BY
        ("SELECT grade, COUNT(*) FROM students GROUP BY grade", "GROUP BY with COUNT"),
        ("SELECT city, AVG(age) FROM students GROUP BY city", "GROUP BY with AVG"),
        ("SELECT instructor, COUNT(*) FROM courses GROUP BY instructor", "GROUP BY courses"),
        
        # ORDER BY
        ("SELECT * FROM students ORDER BY age", "ORDER BY single column"),
        ("SELECT * FROM students ORDER BY age DESC", "ORDER BY DESC"),
        ("SELECT * FROM students ORDER BY grade, age DESC", "ORDER BY multiple"),
        
        # LIMIT
        ("SELECT * FROM students LIMIT 5", "LIMIT clause"),
        ("SELECT * FROM students ORDER BY age DESC LIMIT 3", "ORDER BY with LIMIT"),
        
        # Column aliases
        ("SELECT name AS student_name, age AS student_age FROM students", "Column aliases"),
        ("SELECT COUNT(*) AS total_students FROM students", "Aggregate with alias"),
        
        # Table aliases
        ("SELECT s.name, s.age FROM students s", "Table alias"),
        ("SELECT s.name, s.age FROM students AS s WHERE s.age > 20", "Table alias with WHERE"),
        
        # JOINs
        ("SELECT s.name, e.grade FROM students s JOIN enrollments e ON s.id = e.student_id", "INNER JOIN"),
        ("SELECT s.name, e.course_id FROM students s LEFT JOIN enrollments e ON s.id = e.student_id", "LEFT JOIN"),
        ("SELECT c.course_name, e.grade FROM courses c JOIN enrollments e ON c.course_id = e.course_id", "JOIN different tables"),
        
        # Complex combinations
        ("SELECT s.name, COUNT(e.course_id) FROM students s LEFT JOIN enrollments e ON s.id = e.student_id GROUP BY s.name", "JOIN with GROUP BY"),
        ("SELECT grade, COUNT(*) FROM students GROUP BY grade HAVING COUNT(*) > 1", "GROUP BY with HAVING"),
        ("SELECT DISTINCT s.city FROM students s JOIN enrollments e ON s.id = e.student_id WHERE e.grade = 'A'", "Complex query"),
    ]
    
    passed = 0
    total = len(test_queries)
    
    for query, description in test_queries:
        if test_traditional_compiler(query, datasets, description):
            passed += 1
    
    print(f"\n{'='*60}")
    print(f"Traditional Compiler Results: {passed}/{total} queries passed")
    print('='*60)
    
    coverage = (passed / total) * 100
    print(f"Coverage: {coverage:.1f}%")
    
    if coverage >= 90:
        print("🎉 Excellent! Traditional compiler handles most queries!")
    elif coverage >= 75:
        print("👍 Good! Traditional compiler handles many queries!")
    elif coverage >= 50:
        print("👌 Fair! Traditional compiler needs improvement!")
    else:
        print("⚠️ Poor! Traditional compiler needs major fixes!")
    
    return coverage

if __name__ == "__main__":
    main()