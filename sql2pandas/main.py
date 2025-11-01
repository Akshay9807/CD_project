import streamlit as st
import pandas as pd
import json
from io import StringIO
import traceback
import os
import tempfile

from lexer import SQLLexer
from parser import SQLParser
from ir_generator import IRGenerator
from code_generator import CodeGenerator
from advanced_code_generator import AdvancedCodeGenerator
from executor import PandasExecutor
from universal_executor import universal_executor

def load_sample_data():
    """Load sample data files"""
    sample_data = {}
    sample_dir = "sample_data"
    if os.path.exists(sample_dir):
        for file in os.listdir(sample_dir):
            if file.endswith('.csv'):
                table_name = file[:-4]  # Remove .csv extension
                file_path = os.path.join(sample_dir, file)
                try:
                    sample_data[table_name] = pd.read_csv(file_path)
                except Exception as e:
                    st.warning(f"Could not load sample file {file}: {e}")
    return sample_data

def get_table_suggestions(datasets):
    """Generate example queries based on available tables"""
    if not datasets:
        return ["-- Upload CSV files to see example queries"]
    
    examples = []
    table_names = list(datasets.keys())
    
    # Basic SELECT examples
    examples.append(f"-- Basic queries:")
    examples.append(f"SELECT * FROM {table_names[0]}")
    
    if len(table_names) > 1:
        examples.append(f"SELECT * FROM {table_names[1]}")
    
    # Advanced examples based on available tables
    if 'students' in table_names:
        examples.extend([
            "",
            "-- WHERE clause examples:",
            "SELECT name, age FROM students WHERE age > 20",
            "SELECT * FROM students WHERE grade = 'A' ORDER BY age DESC",
            "SELECT name, city FROM students WHERE city IN ('New York', 'Chicago')",
            "SELECT * FROM students WHERE name LIKE 'A%'",
            "SELECT * FROM students WHERE age BETWEEN 18 AND 22",
            "",
            "-- Aggregate functions:",
            "SELECT COUNT(*) FROM students",
            "SELECT AVG(age), MAX(age), MIN(age) FROM students",
            "SELECT grade, COUNT(*) FROM students GROUP BY grade",
            "SELECT city, AVG(age) FROM students GROUP BY city HAVING AVG(age) > 20"
        ])
    
    if 'courses' in table_names:
        examples.extend([
            "",
            "-- Course queries:",
            "SELECT course_name, credits FROM courses WHERE credits >= 4",
            "SELECT instructor, COUNT(*) FROM courses GROUP BY instructor",
            "SELECT * FROM courses ORDER BY credits DESC, course_name ASC"
        ])
    
    # JOIN examples if we have related tables
    if 'students' in table_names and 'enrollments' in table_names:
        examples.extend([
            "",
            "-- JOIN operations:",
            "SELECT s.name, e.grade FROM students s JOIN enrollments e ON s.id = e.student_id",
            "SELECT s.name, e.course_id, e.grade FROM students s LEFT JOIN enrollments e ON s.id = e.student_id"
        ])
    
    if 'enrollments' in table_names and 'courses' in table_names:
        examples.extend([
            "SELECT c.course_name, e.grade FROM courses c JOIN enrollments e ON c.course_id = e.course_id",
            "SELECT c.course_name, COUNT(e.student_id) FROM courses c LEFT JOIN enrollments e ON c.course_id = e.course_id GROUP BY c.course_name"
        ])
    
    # Advanced examples
    examples.extend([
        "",
        "-- Advanced features:",
        "SELECT DISTINCT city FROM students",
        "SELECT * FROM students ORDER BY age DESC LIMIT 5",
        "",
        "-- Universal engine examples (any SQL syntax):",
        "WITH young_students AS (SELECT * FROM students WHERE age < 21) SELECT * FROM young_students",
        "SELECT *, CASE WHEN age >= 21 THEN 'Adult' ELSE 'Young' END as age_group FROM students",
        "SELECT city, COUNT(*) as student_count FROM students GROUP BY city ORDER BY student_count DESC",
        "SELECT * FROM students WHERE name REGEXP '^[A-C]'",
        "SELECT UPPER(name) as name_upper, LOWER(city) as city_lower FROM students"
    ])
    
    return examples

def main():
    st.title("SQL2Pandas Compiler")
    st.markdown("Convert SQL queries to Pandas code and execute them on multiple CSV datasets")
    
    # Initialize session state for datasets
    if 'datasets' not in st.session_state:
        st.session_state.datasets = load_sample_data()
    
    # Sidebar for data management
    with st.sidebar:
        st.header("Data Management")
        
        # File upload section
        st.subheader("📁 Upload CSV Files")
        uploaded_files = st.file_uploader(
            "Choose CSV files", 
            type=['csv'], 
            accept_multiple_files=True,
            help="Upload one or more CSV files to use as tables in your queries"
        )
        
        # Process uploaded files
        if uploaded_files:
            progress_bar = st.progress(0)
            for i, uploaded_file in enumerate(uploaded_files):
                table_name = uploaded_file.name[:-4]  # Remove .csv extension
                # Clean table name (remove special characters)
                table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
                
                try:
                    df = pd.read_csv(uploaded_file)
                    st.session_state.datasets[table_name] = df
                    st.success(f"✅ Loaded: **{table_name}** ({df.shape[0]} rows, {df.shape[1]} cols)")
                except Exception as e:
                    st.error(f"❌ Error loading {uploaded_file.name}: {str(e)}")
                
                progress_bar.progress((i + 1) / len(uploaded_files))
            
            progress_bar.empty()
        
        # Display available tables
        st.subheader("📊 Available Tables")
        if st.session_state.datasets:
            for table_name, df in st.session_state.datasets.items():
                with st.expander(f"📋 {table_name} ({df.shape[0]} rows)"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Rows:** {df.shape[0]}")
                        st.write(f"**Columns:** {df.shape[1]}")
                    with col2:
                        if st.button(f"Remove {table_name}", key=f"remove_{table_name}"):
                            del st.session_state.datasets[table_name]
                            st.rerun()
                    
                    st.write(f"**Column Names:** {', '.join(df.columns)}")
                    st.write("**Data Types:**")
                    for col, dtype in df.dtypes.items():
                        st.write(f"• {col}: {dtype}")
                    
                    st.write("**Sample Data:**")
                    st.dataframe(df.head(3), use_container_width=True)
        else:
            st.info("📤 No tables loaded. Upload CSV files to get started!")
        
        # Management buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Clear All", help="Remove all loaded tables"):
                st.session_state.datasets = {}
                st.rerun()
        with col2:
            if st.button("🔄 Reload Sample", help="Reload sample data"):
                st.session_state.datasets.update(load_sample_data())
                st.rerun()
    
    # Main content area
    st.subheader("💻 SQL Query Editor")
    
    # Query input with better layout
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Handle example query selection
        if 'example_query' in st.session_state:
            selected_query = st.session_state.example_query
            del st.session_state.example_query
        else:
            # Show default query based on available tables
            if st.session_state.datasets:
                table_names = list(st.session_state.datasets.keys())
                selected_query = f"SELECT * FROM {table_names[0]}"
                if 'students' in table_names:
                    selected_query = "SELECT name, age FROM students WHERE age > 20 ORDER BY age DESC"
            else:
                selected_query = "-- Upload CSV files first, then write your SQL query here\nSELECT * FROM your_table WHERE condition = 'value'"
        
        sql_query = st.text_area(
            "Write your SQL query:",
            value=selected_query,
            height=150,
            help="Supported: SELECT, FROM, WHERE, ORDER BY, AND, OR operations",
            placeholder="SELECT column1, column2 FROM table_name WHERE condition ORDER BY column1"
        )
    
    with col2:
        st.write("**💡 Quick Reference**")
        if st.session_state.datasets:
            st.write(f"**Tables ({len(st.session_state.datasets)}):**")
            for name in st.session_state.datasets.keys():
                st.write(f"• `{name}`")
        else:
            st.warning("⚠️ No tables loaded")
        
        st.write("**Supported SQL:**")
        st.write("• SELECT columns")
        st.write("• FROM table")
        st.write("• WHERE conditions")
        st.write("• ORDER BY column")
        st.write("• AND, OR operators")
        
        # Example queries dropdown
        if st.session_state.datasets:
            with st.expander("📝 Example Queries"):
                examples = get_table_suggestions(st.session_state.datasets)
                for example in examples:
                    if example.startswith('--'):
                        st.write(f"**{example}**")
                    elif example.strip():
                        if st.button(example, key=f"ex_{hash(example)}", help="Click to use this query"):
                            st.session_state.example_query = example
                            st.rerun()
    
    # Process button
    if st.button("🚀 Compile & Execute", type="primary"):
        if not st.session_state.datasets:
            st.error("No datasets available. Please upload CSV files first.")
            return
            
        if not sql_query.strip():
            st.error("Please enter a SQL query.")
            return
            
        # Smart execution: Try traditional compiler first, fall back to universal engine
        execution_method = "Traditional Compiler"
        show_pipeline = True
        
        try:
            # Initialize components
            lexer = SQLLexer()
            parser = SQLParser()
            ir_gen = IRGenerator()
            code_gen = AdvancedCodeGenerator()  # Use advanced code generator
            executor = PandasExecutor()
            
            # Phase 1: Lexical Analysis
            st.subheader("Phase 1: Lexical Analysis (Tokens)")
            tokens = lexer.tokenize(sql_query)
            token_display = [{"Type": t.type.value, "Value": t.value, "Position": t.lineno} for t in tokens]
            
            with st.expander("View Tokens", expanded=False):
                st.json(token_display)
            
            # Phase 2: Syntax Analysis
            st.subheader("Phase 2: Syntax Analysis (Parse Tree)")
            ast = parser.parse(tokens)
            
            # Validate main table exists
            main_table = ast.from_clause.table
            if main_table not in st.session_state.datasets:
                available_tables = list(st.session_state.datasets.keys())
                st.error(f"❌ Table '{main_table}' not found. Available tables: {available_tables}")
                return
            
            # Validate all referenced tables exist (including JOINs)
            referenced_tables = [main_table]
            if ast.from_clause.joins:
                for join in ast.from_clause.joins:
                    referenced_tables.append(join.table)
            
            missing_tables = [t for t in referenced_tables if t not in st.session_state.datasets]
            if missing_tables:
                available_tables = list(st.session_state.datasets.keys())
                st.error(f"❌ Tables not found: {missing_tables}. Available tables: {available_tables}")
                return
            
            # Get the primary table and all datasets
            primary_df = st.session_state.datasets[main_table]
            all_datasets = st.session_state.datasets.copy()
            
            with st.expander("View Parse Tree", expanded=False):
                st.write(f"**Parse tree structure:** {type(ast).__name__}")
                st.write(f"**Columns:** {[col.name for col in ast.columns]}")
                st.write(f"**Main Table:** {ast.from_clause.table}")
                if ast.from_clause.joins:
                    st.write(f"**JOINs:** {len(ast.from_clause.joins)} table(s)")
                if ast.distinct:
                    st.write("**DISTINCT:** Yes")
                if ast.where_clause:
                    st.write("**Has WHERE clause:** Yes")
                if ast.group_by_clause:
                    st.write("**Has GROUP BY clause:** Yes")
                if ast.having_clause:
                    st.write("**Has HAVING clause:** Yes")
                if ast.order_clause:
                    st.write("**Has ORDER BY clause:** Yes")
                if ast.limit_clause:
                    st.write("**Has LIMIT clause:** Yes")
            
            # Show input data for all referenced tables
            st.subheader(f"📋 Input Data")
            
            # Show main table
            with st.expander(f"Main Table: {main_table}", expanded=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Rows", primary_df.shape[0])
                with col2:
                    st.metric("Columns", primary_df.shape[1])
                with col3:
                    st.metric("Memory (KB)", round(primary_df.memory_usage(deep=True).sum() / 1024, 1))
                
                st.dataframe(primary_df, use_container_width=True)
            
            # Show JOIN tables if any
            if ast.from_clause.joins:
                for join in ast.from_clause.joins:
                    join_df = st.session_state.datasets[join.table]
                    with st.expander(f"JOIN Table: {join.table} ({join.join_type})"):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Rows", join_df.shape[0])
                        with col2:
                            st.metric("Columns", join_df.shape[1])
                        with col3:
                            st.metric("Memory (KB)", round(join_df.memory_usage(deep=True).sum() / 1024, 1))
                        
                        st.dataframe(join_df.head(), use_container_width=True)
            
            # Phase 3: Intermediate Representation
            st.subheader("Phase 3: Intermediate Representation (IR)")
            ir = ir_gen.generate(ast)
            
            with st.expander("View IR", expanded=False):
                st.json(ir, expanded=True)
            
            # Phase 4: Code Generation
            st.subheader("Phase 4: Code Generation (Pandas Code)")
            pandas_code = code_gen.generate(ir, all_datasets)
            st.code(pandas_code, language='python')
            
            # Phase 5: Execution
            st.subheader("🎯 Phase 5: Execution Result")
            
            # Execute with all available datasets for JOIN support
            result = executor.execute(pandas_code, all_datasets)
            execution_method = "Traditional Compiler"
            
        except Exception as compiler_error:
            # Traditional compiler failed, try universal engine
            st.warning(f"⚠️ Traditional compiler failed: {str(compiler_error)}")
            st.info("🔄 Falling back to Universal SQL Engine...")
            show_pipeline = False
            
            try:
                result = universal_executor.execute(sql_query, st.session_state.datasets)
                execution_method = "Universal SQL Engine"
                
                if 'error' in result.columns:
                    st.error("❌ Universal engine also encountered issues. See result for details.")
                    show_pipeline = False
                else:
                    st.success("✅ Universal engine executed successfully!")
                    
            except Exception as universal_error:
                st.error(f"❌ Both execution methods failed!")
                st.error(f"Compiler error: {str(compiler_error)}")
                st.error(f"Universal engine error: {str(universal_error)}")
                
                with st.expander("🔍 Troubleshooting"):
                    st.write("**Possible issues:**")
                    st.write("• Table or column names don't match your data")
                    st.write("• Complex SQL syntax not yet supported")
                    st.write("• Data type mismatches")
                    st.write("• Missing JOIN conditions")
                    
                    st.write("**Available tables:**")
                    for name, df in st.session_state.datasets.items():
                        st.write(f"• **{name}**: {list(df.columns)}")
                
                return
        
        # Show results (common for both execution methods)
        if 'result' in locals():
            st.success(f"✅ Query executed successfully using {execution_method}!")
            
            # Show result metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                if 'primary_df' in locals():
                    st.metric("Input Rows", primary_df.shape[0])
                else:
                    total_input_rows = sum(df.shape[0] for df in st.session_state.datasets.values())
                    st.metric("Total Input Rows", total_input_rows)
            with col2:
                st.metric("Output Rows", result.shape[0])
            with col3:
                st.metric("Columns", result.shape[1])
            with col4:
                memory_mb = result.memory_usage(deep=True).sum() / (1024 * 1024)
                st.metric("Memory (MB)", f"{memory_mb:.2f}")
            
            # Show result data
            st.dataframe(result, use_container_width=True)
            
            # Download option
            if not result.empty:
                csv = result.to_csv(index=False)
                st.download_button(
                    label="📥 Download Result as CSV",
                    data=csv,
                    file_name=f"query_result.csv",
                    mime="text/csv"
                )
            
            # Show execution stats
            with st.expander("📊 Execution Details"):
                st.write(f"**Execution Method:** {execution_method}")
                
                if execution_method == "Traditional Compiler" and show_pipeline:
                    st.write("**Pipeline:** Lexer → Parser → IR Generator → Code Generator → Executor")
                    st.write("**Features:** Full compilation pipeline with pandas code generation")
                else:
                    st.write("**Pipeline:** Universal SQL Engine with intelligent parsing")
                    st.write("**Features:** Pattern matching, SQL parsing, fallback strategies")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Query Analysis:**")
                    if 'SELECT' in sql_query.upper():
                        st.write("• SELECT operation detected")
                    if 'JOIN' in sql_query.upper():
                        st.write("• JOIN operation detected")
                    if 'GROUP BY' in sql_query.upper():
                        st.write("• GROUP BY operation detected")
                    if 'ORDER BY' in sql_query.upper():
                        st.write("• ORDER BY operation detected")
                    
                with col2:
                    st.write("**Result Summary:**")
                    st.write(f"• Rows: {result.shape[0]:,}")
                    st.write(f"• Columns: {result.shape[1]}")
                    if not result.empty:
                        st.write("• Data types:")
                        for col, dtype in result.dtypes.items():
                            st.write(f"  - {col}: {dtype}")
                
                if 'error' in result.columns:
                    st.error("⚠️ Query execution had issues - check result data for details")

if __name__ == "__main__":
    main()