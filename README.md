# jsql
In memory SQL database in Julia

## Features
- In-memory tables with full table scans (no indexes).
- SQL parsing and compilation into Julia functions.
- Basic SQL subset:
	- `CREATE TABLE table_name (col1, col2, ...)`
	- `INSERT INTO table_name VALUES (...)`
	- `INSERT INTO table_name (col1, col2, ...) VALUES (...)`
	- `SELECT * FROM table_name`
	- `SELECT col1, col2 FROM table_name WHERE ...`
	- `SELECT t1.col, t2.col FROM t1 JOIN t2 ON t1.key = t2.key [WHERE ...]`
	- `LOAD CSV 'path/to/file.csv' INTO table_name [HEADER|NOHEADER]`
- CSV loading via SQL and direct API.

## Query Semantics
- `WHERE` supports comparison operators: `=`, `!=`, `>`, `<`, `>=`, `<=`.
- `WHERE` boolean operators: `AND`, `OR`, with `AND` precedence over `OR`.
- Parentheses are supported in `WHERE` expressions.

## Usage
```julia
using JSQL

db = Database()

execute!(db, "CREATE TABLE users (id, name, age)")
execute!(db, "INSERT INTO users VALUES (1, 'Ada', 37)")

result = execute!(db, "SELECT name FROM users WHERE age >= 30")

# result.columns == [:name]
# result.rows == [["Ada"]]
```

## CSV Loading
```julia
load_csv!(db, "scores", "scores.csv")
execute!(db, "LOAD CSV 'scores.csv' INTO scores")
```

## Tests
Run:

```powershell
julia --project=. -e "using Pkg; Pkg.test()"
```

## CLI
Run the lightweight CLI with:

```powershell
julia --project=. bin/jsql.jl
```

Execute a single statement:

```powershell
julia --project=. bin/jsql.jl --execute "CREATE TABLE users (id, name); INSERT INTO users VALUES (1, 'Ada'); SELECT * FROM users"
```

Run statements from a file:

```powershell
julia --project=. bin/jsql.jl --file queries.sql
```

Pipe statements from stdin:

```powershell
Get-Content queries.sql | julia --project=. bin/jsql.jl
```
