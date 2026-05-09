module JSQL

using CSV

export Database,
       Table,
       QueryResult,
       execute!,
       compile_sql,
       parse_sql,
       load_csv!

mutable struct Table
    name::String
    columns::Vector{Symbol}
    rows::Vector{Dict{Symbol, Any}}
end

mutable struct Database
    tables::Dict{String, Table}
end

Database() = Database(Dict{String, Table}())

struct QueryResult
    columns::Vector{Symbol}
    rows::Vector{Vector{Any}}
end

abstract type SQLStatement end

struct CreateTableStatement <: SQLStatement
    table::String
    columns::Vector{Symbol}
end

struct InsertStatement <: SQLStatement
    table::String
    columns::Union{Nothing, Vector{Symbol}}
    values::Vector{Any}
end

struct SelectStatement <: SQLStatement
    table::String
    columns::Union{Nothing, Vector{Symbol}}
    where_expr::Union{Nothing, Any}
end

struct LoadCsvStatement <: SQLStatement
    table::String
    path::String
    header::Bool
end

abstract type ExprNode end

struct CompareExpr <: ExprNode
    column::Symbol
    op::String
    value::Any
end

struct AndExpr <: ExprNode
    left::ExprNode
    right::ExprNode
end

struct OrExpr <: ExprNode
    left::ExprNode
    right::ExprNode
end

mutable struct TokenStream
    tokens::Vector{String}
    pos::Int
end

TokenStream(tokens::Vector{String}) = TokenStream(tokens, 1)

function _tokenize(sql::String)
    pattern = r"'[^']*'|>=|<=|!=|=|>|<|,|\(|\)|;|\*|-?\d+\.\d+|-?\d+|[A-Za-z_][A-Za-z0-9_]*"
    tokens = String[]
    for match in eachmatch(pattern, sql)
        push!(tokens, match.match)
    end
    return tokens
end

_peek(ts::TokenStream) = ts.pos <= length(ts.tokens) ? ts.tokens[ts.pos] : nothing

function _consume!(ts::TokenStream)
    token = _peek(ts)
    token === nothing && error("Unexpected end of SQL.")
    ts.pos += 1
    return token
end

function _consume_if!(ts::TokenStream, value::String)
    token = _peek(ts)
    if token !== nothing && uppercase(token) == uppercase(value)
        ts.pos += 1
        return true
    end
    return false
end

function _expect!(ts::TokenStream, value::String)
    token = _consume!(ts)
    if uppercase(token) != uppercase(value)
        error("Expected $(value), got $(token)")
    end
    return token
end

function _expect_identifier!(ts::TokenStream)
    token = _consume!(ts)
    if occursin(r"^[A-Za-z_][A-Za-z0-9_]*$", token)
        return token
    end
    error("Expected identifier, got $(token)")
end

function _parse_literal(token::String)
    u = uppercase(token)
    if startswith(token, "'") && endswith(token, "'")
        return token[2:end-1]
    elseif u == "NULL"
        return nothing
    elseif u == "TRUE"
        return true
    elseif u == "FALSE"
        return false
    elseif occursin(r"^-?\d+$", token)
        return parse(Int, token)
    elseif occursin(r"^-?\d+\.\d+$", token)
        return parse(Float64, token)
    end
    error("Expected literal value, got $(token)")
end

function _parse_identifier_list!(ts::TokenStream)
    names = Symbol[]
    push!(names, Symbol(_expect_identifier!(ts)))
    while _consume_if!(ts, ",")
        push!(names, Symbol(_expect_identifier!(ts)))
    end
    return names
end

function _parse_value_list!(ts::TokenStream)
    values = Any[]
    token = _consume!(ts)
    push!(values, _parse_literal(token))
    while _consume_if!(ts, ",")
        push!(values, _parse_literal(_consume!(ts)))
    end
    return values
end

function _parse_comparison!(ts::TokenStream)::ExprNode
    if _consume_if!(ts, "(")
        expr = _parse_or_expr!(ts)
        _expect!(ts, ")")
        return expr
    end

    column = Symbol(_expect_identifier!(ts))
    op = _consume!(ts)
    op in ("=", "!=", ">", "<", ">=", "<=") || error("Unsupported operator: $(op)")
    value = _parse_literal(_consume!(ts))
    return CompareExpr(column, op, value)
end

function _parse_and_expr!(ts::TokenStream)::ExprNode
    expr = _parse_comparison!(ts)
    while true
        token = _peek(ts)
        if token !== nothing && uppercase(token) == "AND"
            _consume!(ts)
            expr = AndExpr(expr, _parse_comparison!(ts))
        else
            break
        end
    end
    return expr
end

function _parse_or_expr!(ts::TokenStream)::ExprNode
    expr = _parse_and_expr!(ts)
    while true
        token = _peek(ts)
        if token !== nothing && uppercase(token) == "OR"
            _consume!(ts)
            expr = OrExpr(expr, _parse_and_expr!(ts))
        else
            break
        end
    end
    return expr
end

function _parse_create_table!(ts::TokenStream)
    _expect!(ts, "CREATE")
    _expect!(ts, "TABLE")
    table = _expect_identifier!(ts)
    _expect!(ts, "(")
    columns = _parse_identifier_list!(ts)
    _expect!(ts, ")")
    return CreateTableStatement(table, columns)
end

function _parse_insert!(ts::TokenStream)
    _expect!(ts, "INSERT")
    _expect!(ts, "INTO")
    table = _expect_identifier!(ts)

    columns = nothing
    if _consume_if!(ts, "(")
        columns = _parse_identifier_list!(ts)
        _expect!(ts, ")")
    end

    _expect!(ts, "VALUES")
    _expect!(ts, "(")
    values = _parse_value_list!(ts)
    _expect!(ts, ")")
    return InsertStatement(table, columns, values)
end

function _parse_select!(ts::TokenStream)
    _expect!(ts, "SELECT")

    columns = nothing
    if _consume_if!(ts, "*")
        columns = nothing
    else
        columns = _parse_identifier_list!(ts)
    end

    _expect!(ts, "FROM")
    table = _expect_identifier!(ts)

    where_expr = nothing
    token = _peek(ts)
    if token !== nothing && uppercase(token) == "WHERE"
        _consume!(ts)
        where_expr = _parse_or_expr!(ts)
    end

    return SelectStatement(table, columns, where_expr)
end

function _parse_load_csv!(ts::TokenStream)
    _expect!(ts, "LOAD")
    _expect!(ts, "CSV")
    path = _parse_literal(_consume!(ts))
    isa(path, String) || error("CSV path must be a string literal")
    _expect!(ts, "INTO")
    table = _expect_identifier!(ts)

    header = true
    token = _peek(ts)
    if token !== nothing && uppercase(token) == "NOHEADER"
        _consume!(ts)
        header = false
    elseif token !== nothing && uppercase(token) == "HEADER"
        _consume!(ts)
        header = true
    end

    return LoadCsvStatement(table, path, header)
end

function parse_sql(sql::String)::SQLStatement
    tokens = _tokenize(sql)
    isempty(tokens) && error("SQL is empty")
    ts = TokenStream(tokens)

    first_token = uppercase(_peek(ts))
    statement = if first_token == "CREATE"
        _parse_create_table!(ts)
    elseif first_token == "INSERT"
        _parse_insert!(ts)
    elseif first_token == "SELECT"
        _parse_select!(ts)
    elseif first_token == "LOAD"
        _parse_load_csv!(ts)
    else
        error("Unsupported statement type: $(first_token)")
    end

    if _consume_if!(ts, ";")
        # Optional trailing semicolon.
    end

    _peek(ts) === nothing || error("Unexpected token: $(_peek(ts))")
    return statement
end

function _eval_compare(left, op::String, right)
    if op == "="
        return left == right
    elseif op == "!="
        return left != right
    elseif op == ">"
        return left > right
    elseif op == "<"
        return left < right
    elseif op == ">="
        return left >= right
    elseif op == "<="
        return left <= right
    end
    error("Unsupported comparison operator: $(op)")
end

function _eval_expr(expr::ExprNode, row::Dict{Symbol, Any})
    if expr isa CompareExpr
        cmp = expr::CompareExpr
        haskey(row, cmp.column) || error("Unknown column in WHERE: $(cmp.column)")
        return _eval_compare(row[cmp.column], cmp.op, cmp.value)
    elseif expr isa AndExpr
        e = expr::AndExpr
        return _eval_expr(e.left, row) && _eval_expr(e.right, row)
    elseif expr isa OrExpr
        e = expr::OrExpr
        return _eval_expr(e.left, row) || _eval_expr(e.right, row)
    end
    error("Unknown expression type")
end

function _require_table(db::Database, table_name::String)
    haskey(db.tables, table_name) || error("Table does not exist: $(table_name)")
    return db.tables[table_name]
end

function load_csv!(db::Database, table_name::String, path::String; header::Bool = true)
    source = CSV.File(path; header = header)

    columns = Symbol.(propertynames(source))
    if !haskey(db.tables, table_name)
        db.tables[table_name] = Table(table_name, columns, Dict{Symbol, Any}[])
    end

    table = db.tables[table_name]
    if table.columns != columns
        error("CSV columns do not match table schema")
    end

    for r in source
        row = Dict{Symbol, Any}()
        for c in columns
            value = getproperty(r, c)
            row[c] = value === missing ? nothing : value
        end
        push!(table.rows, row)
    end

    return table
end

function compile_sql(statement::CreateTableStatement)
    return function (db::Database)
        haskey(db.tables, statement.table) && error("Table already exists: $(statement.table)")
        db.tables[statement.table] = Table(statement.table, statement.columns, Dict{Symbol, Any}[])
        return nothing
    end
end

function compile_sql(statement::InsertStatement)
    return function (db::Database)
        table = _require_table(db, statement.table)

        target_columns = statement.columns === nothing ? table.columns : statement.columns
        length(target_columns) == length(statement.values) || error("Column/value count mismatch")

        for c in target_columns
            c in table.columns || error("Unknown column for insert: $(c)")
        end

        row = Dict{Symbol, Any}()
        for c in table.columns
            row[c] = nothing
        end

        for (col, value) in zip(target_columns, statement.values)
            row[col] = value
        end

        push!(table.rows, row)
        return nothing
    end
end

function compile_sql(statement::SelectStatement)
    return function (db::Database)
        table = _require_table(db, statement.table)
        selected_columns = statement.columns === nothing ? table.columns : statement.columns

        for c in selected_columns
            c in table.columns || error("Unknown column for select: $(c)")
        end

        rows = Vector{Vector{Any}}()
        for row in table.rows
            if statement.where_expr === nothing || _eval_expr(statement.where_expr, row)
                push!(rows, [row[c] for c in selected_columns])
            end
        end

        return QueryResult(selected_columns, rows)
    end
end

function compile_sql(statement::LoadCsvStatement)
    return function (db::Database)
        load_csv!(db, statement.table, statement.path; header = statement.header)
        return nothing
    end
end

function compile_sql(sql::String)
    statement = parse_sql(sql)
    return compile_sql(statement)
end

function execute!(db::Database, sql::String)
    fn = compile_sql(sql)
    return fn(db)
end

end
