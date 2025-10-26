# SQL2Pandas Compiler

A educational compiler that converts SQL SELECT queries into equivalent Pandas code and executes them on CSV data.

## Features

- **Lexical Analysis**: Tokenizes SQL queries into meaningful tokens
- **Syntax Analysis**: Parses tokens into an Abstract Syntax Tree (AST)
- **Intermediate Representation**: Converts AST to JSON-like IR structure
- **Code Generation**: Generates executable Pandas code from IR
- **Execution**: Runs generated code on CSV data and displays results
- **Error Handling**: Comprehensive error handling for syntax and runtime errors
- **Interactive GUI**: Streamlit-based web interface

## Supported SQL Features

- `SELECT` with column selection
- `FROM` table specification
- `WHERE` clause with comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`)
- `ORDER BY` with ASC/DESC
- Logical operators (`AND`, `OR`) in WHERE clauses
- String, numeric, and identifier literals

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
streamlit run main.py
```

Then open your browser to the displayed URL (typically http://localhost:8501).

## Example Queries

```sql
SELECT name, age FROM students WHERE age > 20 ORDER BY age DESC
SELECT * FROM students WHERE grade = 'A' AND age >= 21
SELECT name, city FROM students WHERE city = 'New York' OR city = 'Chicago'
```

## Project Structure

```
sql2pandas/
├── main.py              # Streamlit GUI application
├── lexer.py             # Lexical analyzer (tokenizer)
├── parser.py            # Syntax analyzer (parser)
├── ir_generator.py      # Intermediate representation generator
├── code_generator.py    # Pandas code generator
├── executor.py          # Code execution engine
├── sample_data/
│   └── students.csv     # Sample dataset
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Architecture

The compiler follows a traditional multi-phase design:

1. **Lexical Analysis** (`lexer.py`): Converts SQL text into tokens
2. **Syntax Analysis** (`parser.py`): Builds AST from tokens
3. **IR Generation** (`ir_generator.py`): Creates intermediate representation
4. **Code Generation** (`code_generator.py`): Produces Pandas code
5. **Execution** (`executor.py`): Runs code and returns results

Each phase is clearly separated and can be tested independently.