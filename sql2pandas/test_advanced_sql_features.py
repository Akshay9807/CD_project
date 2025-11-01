#!/usr/bin/env python3
"""
Test Advanced SQL Features: UNION, INTERSECT, ROUND, JOINs
"""

import pandas as pd
from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
from advanced_code_generator import AdvancedCodeGenerator
from executor import PandasExecutor

def test_advanced_sql_features():
    """Test all advanced SQL features"""
    
    # Create comprehensive test data
    students_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [22, 19, 21, 20, 23],
        'grade': ['A', 'B', 'A', 'C', 'B'],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    }
    students = pd.DataFrame(students_data)
    
    courses_data = {
        'course_id': ['CS101', 'MATH101', 'ENG101', 'PHYS101'],
        'course_name': ['Programming', 'Calculus', 'English', 'Physics'],
        'instructor': ['Dr. Smith', 'Dr. Brown', 'Prof. Davis', 'Dr. Wilson'],
        'credits': [3, 4, 3, 4]
    }
    courses = pd.DataFrame(courses_data)
    
    enrollments_data = {
        'student_id': [1, 2, 3, 1, 2, 4, 5],
        'course_id': ['CS101', 'CS101', 'MATH101', 'ENG101', 'MATH101', 'PHYS101', 'CS101'],
        'semester': ['Fall2023'] * 7,
        'grade': ['A', 'B', 'A', 'A', 'C', 'B', 'A']
    }
    enrollments = pd.DataFrame(enrollments_data)
    
    # Additional table for set operations
    alumni_data = {
        'id': [6, 7, 8],
        'name': ['Frank', 'Grace', 'Henry'],
        'age': [25, 24, 26],
        'grade': ['A', 'B', 'A'],
        'city': ['Boston', 'Seattle', 'Denver']
    }
    alumni = pd.DataFrame(alumni_data)
    
    datasets = {
        'students': students,
        'courses': courses,
        'enrollments': enrollments,
        'alumni': alumni
    }
    
    # Test queries for advanced features
    test_queries = [
        # UNION operations
        {
            'name': 'UNION - Combine students and alumni',
            'query': 'SELECT name, age FROM students UNION SELECT name, age FROM alumni'
        },
        {
            'name': 'UNION ALL - Combine with duplicates',
            'query': 'SELECT city FROM students UNION ALL SELECT city FROM alumni'
        },
        
        # INTERSECT operations
        {
            'name': 'INTERSECT - Common cities',
            'query': 'SELECT city FROM students INTERSECT SELECT city FROM alumni'
        },
        
        # EXCEPT operations
        {
            'name': 'EXCEPT - Students not in alumni cities',
            'query': 'SELECT city FROM students EXCEPT SELECT city FROM alumni'
        },
        
        # Enhanced JOINs
        {
            'name': 'CROSS JOIN - All combinations',
            'query': 'SELECT s.name, c.course_name FROM students s CROSS JOIN courses c'
        },
        {
            'name': 'FULL OUTER JOIN',
            'query': 'SELECT s.name, e.course_id FROM students s FULL OUTER JOIN enrollments e ON s.id = e.student_id'
        },
        
        # ROUND function enhancements
        {
            'name': 'ROUND with calculations',
            'query': 'SELECT name, age, ROUND(age / 10.0, 2) AS age_decade FROM students'
        },
        {
            'name': 'ROUND with aggregates',
            'query': 'SELECT city, ROUND(AVG(age), 1) AS avg_age FROM students GROUP BY city'
        },
        
        # Complex combinations
        {
            'name': 'Complex query with CASE, ROUND, and JOIN',
            'query': '''
            SELECT c.instructor, 
                   ROUND(AVG(CASE e.grade 
                             WHEN 'A' THEN 4.0 
                             WHEN 'B' THEN 3.0 
                             WHEN 'C' THEN 2.0 
                             ELSE 1.0 END), 2) AS gpa
            FROM courses c 
            JOIN enrollments e ON c.course_id = e.course_id 
            GROUP BY c.instructor 
            ORDER BY gpa DESC
            '''
        },
        
        # Set operations with ORDER BY
        {
            'name': 'UNION with ORDER BY',
            'query': 'SELECT name, age FROM students UNION SELECT name, age FROM alumni ORDER BY age DESC'
        }
    ]
    
    print("Testing Advanced SQL Features")
    print("=" * 80)
    
    passed = 0
    total = len(test_queries)
    
    for test_case in test_queries:
        print(f"\nTesting: {test_case['name']}")
        print(f"Query: {test_case['query']}")
        print("-" * 60)
        
        try:
            # Initialize components
            lexer = SQLLexer()
            parser = SQLParser()
            ir_gen = IRGenerator()
            code_gen = AdvancedCodeGenerator()
            executor = PandasExecutor()
            
            # Execute the pipeline
            tokens = lexer.tokenize(test_case['query'])
            ast = parser.parse(tokens)
            ir = ir_gen.generate(ast)
            pandas_code = code_gen.generate(ir, datasets)
            result = executor.execute(pandas_code, datasets)
            
            print(f"✅ SUCCESS - Result shape: {result.shape}")
            print("Sample result:")
            print(result.head().to_string(index=False))
            passed += 1
            
        except Exception as e:
            print(f"❌ FAILED - {str(e)}")
            # Print some debug info
            try:
                tokens = lexer.tokenize(test_case['query'])
                print(f"   Tokens generated: {len(tokens)}")
            except:
                print("   Failed at lexical analysis")
    
    print(f"\n{'='*80}")
    print(f"Advanced Features Test Results: {passed}/{total} queries passed")
    print('='*80)
    
    success_rate = (passed / total) * 100
    print(f"Success Rate: {success_rate:.1f}%")
    
    if success_rate >= 90:
        print("🎉 Excellent! Advanced SQL features working great!")
    elif success_rate >= 70:
        print("👍 Good! Most advanced features working!")
    else:
        print("⚠️ Need more work on advanced features!")
    
    return success_rate

if __name__ == "__main__":
    test_advanced_sql_features()