#!/usr/bin/env python3
"""
Debug CASE statement parsing
"""

from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
import json

def debug_case_parsing():
    """Debug CASE statement parsing step by step"""
    
    # Simple CASE query first
    sql_query = """
    SELECT instructor, 
           AVG(CASE grade 
               WHEN 'A' THEN 4 
               WHEN 'B' THEN 3 
               END) AS avg_gpa
    FROM enrollments
    GROUP BY instructor
    """
    
    print("Debugging CASE statement parsing:")
    print(sql_query)
    print("=" * 60)
    
    try:
        # Phase 1: Lexical Analysis
        print("1. Lexical Analysis...")
        lexer = SQLLexer()
        tokens = lexer.tokenize(sql_query)
        print(f"   Generated {len(tokens)} tokens")
        
        # Show some key tokens
        for i, token in enumerate(tokens):
            if token.type.value in ['CASE', 'WHEN', 'THEN', 'END', 'AVG']:
                print(f"   Token {i}: {token.type.value} = '{token.value}'")
        
        # Phase 2: Syntax Analysis
        print("\n2. Syntax Analysis...")
        parser = SQLParser()
        ast = parser.parse(tokens)
        print(f"   Parse tree: {type(ast).__name__}")
        
        # Show column information
        for i, col in enumerate(ast.columns):
            print(f"   Column {i}: name='{col.name}', function='{col.function}', alias='{col.alias}'")
            if col.expression:
                print(f"      Expression: type='{col.expression.type}', value='{col.expression.value}'")
        
        # Phase 3: IR Generation
        print("\n3. IR Generation...")
        ir_gen = IRGenerator()
        ir = ir_gen.generate(ast)
        
        print("   Generated IR:")
        print(json.dumps(ir, indent=2, default=str))
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_case_parsing()