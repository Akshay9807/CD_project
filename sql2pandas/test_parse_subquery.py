from lexer import SQLLexer
from parser import SQLParser

query = "SELECT name FROM students WHERE age = (SELECT AVG(age) FROM students);"

lexer = SQLLexer()
tokens = lexer.tokenize(query)
print('Tokens:')
for t in tokens:
    print(t.type, t.value)

parser = SQLParser()
ast = parser.parse(tokens)
print('\nAST:')
print(ast)

# Inspect where clause
if ast.where_clause:
    print('\nWhere clause condition:')
    print(ast.where_clause.condition)
    right = ast.where_clause.condition.right
    print('\nRight type:', type(right))
    if hasattr(right, 'columns'):
        print('Subquery columns:', [c.name for c in right.columns])
    else:
        print('Right value:', right)
