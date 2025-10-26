import streamlit as st
import pandas as pd
import json
from io import StringIO
import traceback

from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
from code_generator import CodeGenerator
from executor import PandasExecutor

def main():
    st.title("SQL2Pandas Compiler")
    st.markdown("Convert SQL queries to Pandas code and execute them on CSV data")
    
    # Input section
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("SQL Query")
        sql_query = st.text_area(
            "Enter your SQL query:",
            value="SELECT name, age FROM students WHERE age > 20 ORDER BY age DESC",
            height=100
        )
    
    with col2:
        st.subheader("CSV Data")
        uploaded_file = st.file_uploader("Upload CSV file", type=['csv'])
        
        if uploaded_file is None:
            st.info("Using sample data (students.csv)")
            csv_path = "sample_data/students.csv"
        else:
            # Save uploaded file temporarily
            csv_content = StringIO(str(uploaded_file.read(), "utf-8"))
            csv_path = None
    
    # Process button
    if st.button("Compile & Execute"):
        try:
            # Initialize components
            lexer = SQLLexer()
            parser = SQLParser()
            ir_gen = IRGenerator()
            code_gen = CodeGenerator()
            executor = PandasExecutor()
            
            # Load data
            if uploaded_file is None:
                df = pd.read_csv(csv_path)
            else:
                df = pd.read_csv(csv_content)
            
            st.subheader("Input Data")
            st.dataframe(df)
            
            # Phase 1: Lexical Analysis
            st.subheader("Phase 1: Lexical Analysis (Tokens)")
            tokens = lexer.tokenize(sql_query)
            token_display = [{"Type": t.type, "Value": t.value, "Position": t.lineno} for t in tokens]
            st.json(token_display)
            
            # Phase 2: Syntax Analysis
            st.subheader("Phase 2: Syntax Analysis (Parse Tree)")
            ast = parser.parse(tokens)
            st.text(f"Parse tree structure: {type(ast).__name__}")
            
            # Phase 3: Intermediate Representation
            st.subheader("Phase 3: Intermediate Representation (IR)")
            ir = ir_gen.generate(ast)
            st.json(ir, expanded=True)
            
            # Phase 4: Code Generation
            st.subheader("Phase 4: Code Generation (Pandas Code)")
            pandas_code = code_gen.generate(ir)
            st.code(pandas_code, language='python')
            
            # Phase 5: Execution
            st.subheader("Phase 5: Execution Result")
            result = executor.execute(pandas_code, df)
            st.dataframe(result)
            
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.text("Traceback:")
            st.text(traceback.format_exc())

if __name__ == "__main__":
    main()