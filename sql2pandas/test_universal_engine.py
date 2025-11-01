#!/usr/bin/env python3
"""
Universal SQL Engine Test Suite
Tests the universal engine with a wide variety of SQL queries
"""

import pandas as pd
from universal_executor import universal_executor
import os

def test_universal_query(sql_query, datasets, description=""):
    """Test a SQL query using the universal engine"""
    print(f"\n{'='*80}")
    print(f"Testing: {description}")
    print(f"Query: {sql_query}")
    print('='*80)
    
    try:
        result = universal_executor.execute(sql_query, datasets)
        
        print(f"✅ Success! Result shape: {result.shape}")
        print("Result preview:")
        print(result.head().to_string(index=False))
        
        if 'error' in result.columns:
            print("⚠️ Query had issues - check result for details")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def main():
    """Run comprehensive universal engine tests"""
    print("Universal SQL Engine Test Suite")
    print("=" * 80)
    
    # Load test data
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
    
    # Comprehensive test queries covering all SQL features
    test_queries = [
        # Basic SELECT queries
        ("SELECT * FROM students", "Basic SELECT all"),
        ("SELECT name, age FROM students", "SELECT specific columns"),
        ("SELECT DISTINCT city FROM students", "SELECT DISTINCT"),
        
        # WHERE clauses - all types
        ("SELECT * FROM students WHERE age > 20", "WHERE with greater than"),
        ("SELECT * FROM students WHERE grade = 'A'", "WHERE with equality"),
        ("SELECT * FROM students WHERE city IN ('New York', 'Chicago')", "WHERE with IN clause"),
        ("SELECT * FROM students WHERE name LIKE 'A%'", "WHERE with LIKE pattern"),
        ("SELECT * FROM students WHERE age BETWEEN 18 AND 22", "WHERE with BETWEEN"),
        ("SELECT * FROM students WHERE grade IS NOT NULL", "WHERE with IS NOT NULL"),
        ("SELECT * FROM students WHERE age > 20 AND grade = 'A'", "WHERE with AND"),
        ("SELECT * FROM students WHERE city = 'New York' OR city = 'Chicago'", "WHERE with OR"),
        ("SELECT * FROM students WHERE NOT (age < 20)", "WHERE with NOT"),
        
        # Aggregate functions
        ("SELECT COUNT(*) FROM students", "COUNT all rows"),
        ("SELECT COUNT(grade) FROM students", "COUNT specific column"),
        ("SELECT AVG(age) FROM students", "AVG function"),
        ("SELECT MAX(age), MIN(age) FROM students", "MAX and MIN functions"),
        ("SELECT SUM(credits) FROM courses", "SUM function"),
        
        # GROUP BY and HAVING
        ("SELECT grade, COUNT(*) FROM students GROUP BY grade", "GROUP BY with COUNT"),
        ("SELECT city, AVG(age) FROM students GROUP BY city", "GROUP BY with AVG"),
        ("SELECT grade, COUNT(*) FROM students GROUP BY grade HAVING COUNT(*) > 1", "GROUP BY with HAVING"),
        ("SELECT city, AVG(age) as avg_age FROM students GROUP BY city HAVING AVG(age) > 20", "GROUP BY with HAVING on aggregate"),
        
        # ORDER BY
        ("SELECT * FROM students ORDER BY age", "ORDER BY single column"),
        ("SELECT * FROM students ORDER BY age DESC", "ORDER BY descending"),
        ("SELECT * FROM students ORDER BY grade, age DESC", "ORDER BY multiple columns"),
        
        # LIMIT and OFFSET
        ("SELECT * FROM students LIMIT 5", "LIMIT clause"),
        ("SELECT * FROM students ORDER BY age DESC LIMIT 3", "ORDER BY with LIMIT"),
        ("SELECT * FROM students LIMIT 5 OFFSET 2", "LIMIT with OFFSET"),
        
        # JOINs
        ("SELECT s.name, e.grade FROM students s JOIN enrollments e ON s.id = e.student_id", "INNER JOIN"),
        ("SELECT s.name, e.course_id FROM students s LEFT JOIN enrollments e ON s.id = e.student_id", "LEFT JOIN"),
        ("SELECT c.course_name, e.grade FROM courses c RIGHT JOIN enrollments e ON c.course_id = e.course_id", "RIGHT JOIN"),
        
        # Complex JOINs
        ("SELECT s.name, c.course_name, e.grade FROM students s JOIN enrollments e ON s.id = e.student_id JOIN courses c ON e.course_id = c.course_id", "Multiple JOINs"),
        
        # Subqueries
        ("SELECT * FROM students WHERE age > (SELECT AVG(age) FROM students)", "Subquery in WHERE"),
        ("SELECT name FROM students WHERE id IN (SELECT student_id FROM enrollments WHERE grade = 'A')", "Subquery with IN"),
        
        # CASE statements
        ("SELECT name, CASE WHEN age >= 21 THEN 'Adult' ELSE 'Young' END as age_group FROM students", "CASE statement"),
        ("SELECT grade, CASE grade WHEN 'A' THEN 'Excellent' WHEN 'B' THEN 'Good' ELSE 'Average' END as performance FROM students", "CASE with multiple conditions"),
        
        # String functions
        ("SELECT UPPER(name) as name_upper FROM students", "UPPER function"),
        ("SELECT LOWER(city) as city_lower FROM students", "LOWER function"),
        ("SELECT CONCAT(name, ' - ', city) as name_city FROM students", "CONCAT function"),
        ("SELECT LENGTH(name) as name_length FROM students", "LENGTH function"),
        
        # Date functions (if applicable)
        ("SELECT name, age, age + 1 as next_year_age FROM students", "Mathematical expression"),
        
        # Window functions
        ("SELECT name, age, ROW_NUMBER() OVER (ORDER BY age) as row_num FROM students", "ROW_NUMBER window function"),
        ("SELECT name, age, RANK() OVER (ORDER BY age DESC) as age_rank FROM students", "RANK window function"),
        
        # UNION operations
        ("SELECT name FROM students WHERE grade = 'A' UNION SELECT name FROM students WHERE age > 22", "UNION operation"),
        ("SELECT 'Student' as type, name FROM students UNION ALL SELECT 'Course' as type, course_name FROM courses", "UNION ALL with different tables"),
        
        # EXISTS
        ("SELECT * FROM students s WHERE EXISTS (SELECT 1 FROM enrollments e WHERE e.student_id = s.id)", "EXISTS subquery"),
        
        # Common Table Expressions (CTEs)
        ("WITH young_students AS (SELECT * FROM students WHERE age < 21) SELECT * FROM young_students", "Simple CTE"),
        ("WITH grade_counts AS (SELECT grade, COUNT(*) as cnt FROM students GROUP BY grade) SELECT * FROM grade_counts WHERE cnt > 2", "CTE with aggregation"),
        
        # Complex analytical queries
        ("SELECT city, COUNT(*) as student_count, AVG(age) as avg_age FROM students GROUP BY city ORDER BY student_count DESC", "Complex analytics"),
        ("SELECT s.name, COUNT(e.course_id) as course_count FROM students s LEFT JOIN enrollments e ON s.id = e.student_id GROUP BY s.name ORDER BY course_count DESC", "JOIN with GROUP BY"),
        
        # Edge cases and advanced syntax
        ("SELECT * FROM students WHERE name REGEXP '^[A-C]'", "REGEXP pattern matching"),
        ("SELECT COALESCE(grade, 'Unknown') as grade_clean FROM students", "COALESCE function"),
        ("SELECT * FROM students WHERE age IN (SELECT DISTINCT age FROM students WHERE age > 20)", "Complex subquery"),
        
        # Multiple table operations
        ("SELECT COUNT(DISTINCT s.city) as cities, COUNT(DISTINCT c.instructor) as instructors FROM students s CROSS JOIN courses c", "CROSS JOIN with aggregates"),
        
        # Advanced WHERE conditions
        ("SELECT * FROM students WHERE (age > 20 AND grade = 'A') OR (age < 19 AND grade = 'B')", "Complex WHERE with parentheses"),
        ("SELECT * FROM students WHERE city LIKE '%New%' OR city LIKE '%San%'", "Multiple LIKE conditions"),
        
        # Null handling
        ("SELECT * FROM students WHERE grade IS NULL OR grade = ''", "NULL and empty string handling"),
        
        # Mathematical operations
        ("SELECT name, age, age * 12 as age_in_months, age / 10.0 as age_decade FROM students", "Mathematical operations"),
        
        # Advanced grouping
        ("SELECT grade, city, COUNT(*) FROM students GROUP BY grade, city ORDER BY grade, city", "Multiple column GROUP BY"),
        ("SELECT grade, COUNT(*) as cnt FROM students GROUP BY grade ORDER BY cnt DESC LIMIT 1", "GROUP BY with ORDER and LIMIT"),
    ]
    
    passed = 0
    total = len(test_queries)
    
    for query, description in test_queries:
        if test_universal_query(query, datasets, description):
            passed += 1
    
    print(f"\n{'='*80}")
    print(f"Universal Engine Test Results: {passed}/{total} queries passed")
    print('='*80)
    
    success_rate = (passed / total) * 100
    print(f"Success Rate: {success_rate:.1f}%")
    
    if success_rate >= 90:
        print("🎉 Excellent! Universal engine handles almost all SQL queries!")
    elif success_rate >= 75:
        print("👍 Good! Universal engine handles most SQL queries!")
    elif success_rate >= 50:
        print("👌 Fair! Universal engine handles many SQL queries!")
    else:
        print("⚠️ Universal engine needs improvement for better SQL coverage!")

if __name__ == "__main__":
    main()