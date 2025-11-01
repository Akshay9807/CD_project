#!/usr/bin/env python3
"""
Test Complex SQL Query with CASE statements and functions
"""

import pandas as pd
from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
from advanced_code_generator import AdvancedCodeGenerator
from executor import PandasExecutor

def test_complex_query():
    """Test the complex GPA calculation query"""
    
    # Create test data
    enrollments_data = {
        'student_id': [1, 2, 3, 1, 2, 3, 4, 5],
        'course_id': ['CS101', 'CS101', 'MATH101', 'ENG101', 'MATH101', 'CS101', 'CS101', 'ENG101'],
        'semester': ['Fall2023'] * 8,
        'grade': ['A', 'B', 'A', 'B', 'C', 'A', 'C', 'A']
    }
    enrollments = pd.DataFrame(enrollments_data)
    
    courses_data = {
        'course_id': ['CS101', 'MATH101', 'ENG101'],
        'course_name': ['Programming', 'Calculus', 'English'],
        'instructor': ['Dr. Smith', 'Dr. Brown', 'Prof. Davis'],
        'credits': [3, 4, 3]
    }
    courses = pd.DataFrame(courses_data)
    
    datasets = {
        'enrollments': enrollments,
        'courses': courses
    }
    
    # The complex query
    sql_query = """
    SELECT c.instructor, 
           ROUND(AVG(CASE grade 
                     WHEN 'A' THEN 4 
                     WHEN 'B' THEN 3 
                     WHEN 'C' THEN 2 
                     WHEN 'D' THEN 1 
                     END), 2) AS avg_gpa
    FROM enrollments e
    JOIN courses c ON e.course_id = c.course_id
    GROUP BY c.instructor
    ORDER BY avg_gpa DESC
    """
    
    print("Testing Complex Query with CASE and ROUND:")
    print(sql_query)
    print("=" * 60)
    
    try:
        # Initialize components
        lexer = SQLLexer()
        parser = SQLParser()
        ir_gen = IRGenerator()
        code_gen = AdvancedCodeGenerator()
        executor = PandasExecutor()
        
        # Phase 1: Lexical Analysis
        print("1. Lexical Analysis...")
        tokens = lexer.tokenize(sql_query)
        print(f"   Generated {len(tokens)} tokens")
        
        # Phase 2: Syntax Analysis
        print("2. Syntax Analysis...")
        ast = parser.parse(tokens)
        print(f"   Parse tree: {type(ast).__name__}")
        
        # Phase 3: IR Generation
        print("3. IR Generation...")
        ir = ir_gen.generate(ast)
        print("   IR generated successfully")
        
        # Phase 4: Code Generation
        print("4. Code Generation...")
        pandas_code = code_gen.generate(ir, datasets)
        print("   Generated Pandas code:")
        for line in pandas_code.split('\n'):
            print(f"   {line}")
        
        # Phase 5: Execution
        print("5. Execution...")
        result = executor.execute(pandas_code, datasets)
        print(f"   Result shape: {result.shape}")
        print("   Result:")
        print(result.to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_complex_query()
    if success:
        print("\n✅ Complex query test passed!")
    else:
        print("\n❌ Complex query test failed!")