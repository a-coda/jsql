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

struct ColumnRef
    table::Union{Nothing, String}
    column::Symbol
end

struct JoinSpec
    right_table::String
    on_expr::Any
end

struct SelectStatement <: SQLStatement
    from_table::String
    columns::Union{Nothing, Vector{ColumnRef}}
    joins::Vector{JoinSpec}
    where_expr::Union{Nothing, Any}
end

struct LoadCsvStatement <: SQLStatement
    table::String
    path::String
    header::Bool
end

abstract type ExprNode end

struct CompareExpr <: ExprNode
    column::ColumnRef
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
    pattern = r"'[^']*'|>=|<=|!=|=|>|<|,|\.|\(|\)|;|\*|-?\d+\.\d+|-?\d+|[A-Za-z_][A-Za-z0-9_]*"
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

function _parse_column_ref!(ts::TokenStream)
    first = _expect_identifier!(ts)
    if _consume_if!(ts, ".")
        second = _expect_identifier!(ts)
        return ColumnRef(first, Symbol(second))
    end
    return ColumnRef(nothing, Symbol(first))
end

function _parse_column_ref_list!(ts::TokenStream)
    refs = ColumnRef[]
    push!(refs, _parse_column_ref!(ts))
    while _consume_if!(ts, ",")
        push!(refs, _parse_column_ref!(ts))
    end
    return refs
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

    column = _parse_column_ref!(ts)
    op = _consume!(ts)
    op in ("=", "!=", ">", "<", ">=", "<=") || error("Unsupported operator: $(op)")

    next_token = _peek(ts)
    next_token === nothing && error("Expected comparison value")
    value = if startswith(next_token, "'") || occursin(r"^-?\d+$", next_token) || occursin(r"^-?\d+\.\d+$", next_token) || uppercase(next_token) in ("NULL", "TRUE", "FALSE")
        _parse_literal(_consume!(ts))
    else
        _parse_column_ref!(ts)
    end

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
        columns = _parse_column_ref_list!(ts)
    end

    _expect!(ts, "FROM")
    from_table = _expect_identifier!(ts)

    joins = JoinSpec[]
    while true
        token = _peek(ts)
        if token === nothing || uppercase(token) != "JOIN"
            break
        end

        _consume!(ts)
        right_table = _expect_identifier!(ts)
        _expect!(ts, "ON")
        on_expr = _parse_comparison!(ts)
        push!(joins, JoinSpec(right_table, on_expr))
    end

    where_expr = nothing
    token = _peek(ts)
    if token !== nothing && uppercase(token) == "WHERE"
        _consume!(ts)
        where_expr = _parse_or_expr!(ts)
    end

    return SelectStatement(from_table, columns, joins, where_expr)
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
        left_value = _resolve_column_ref(row, cmp.column)
        right_value = cmp.value isa ColumnRef ? _resolve_column_ref(row, cmp.value) : cmp.value
        return _eval_compare(left_value, cmp.op, right_value)
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

function _qualified_symbol(table_name::String, column::Symbol)
    return Symbol("$(table_name).$(column)")
end

function _resolve_column_ref(row::Dict{Symbol, Any}, ref::ColumnRef)
    if ref.table !== nothing
        qualified = _qualified_symbol(ref.table, ref.column)
        if haskey(row, qualified)
            return row[qualified]
        elseif haskey(row, ref.column)
            return row[ref.column]
        end
        error("Unknown column: $(ref.table).$(ref.column)")
    end

    if haskey(row, ref.column)
        return row[ref.column]
    end

    suffix = ".$(ref.column)"
    candidates = [k for k in keys(row) if endswith(String(k), suffix)]
    if length(candidates) == 1
        return row[first(candidates)]
    elseif isempty(candidates)
        error("Unknown column: $(ref.column)")
    end
    error("Ambiguous column: $(ref.column)")
end

function _column_ref_symbol(ref::ColumnRef)
    return ref.table === nothing ? ref.column : _qualified_symbol(ref.table, ref.column)
end

function _qualify_row(table_name::String, row::Dict{Symbol, Any})
    qualified = Dict{Symbol, Any}()
    for (column, value) in row
        qualified[_qualified_symbol(table_name, column)] = value
    end
    return qualified
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
        base_table = _require_table(db, statement.from_table)

        if isempty(statement.joins)
            selected_columns = statement.columns === nothing ? [ColumnRef(nothing, c) for c in base_table.columns] : statement.columns

            rows = Vector{Vector{Any}}()
            for row in base_table.rows
                if statement.where_expr === nothing || _eval_expr(statement.where_expr, row)
                    push!(rows, [_resolve_column_ref(row, c) for c in selected_columns])
                end
            end

            return QueryResult([_column_ref_symbol(c) for c in selected_columns], rows)
        end

        joined_rows = [_qualify_row(statement.from_table, row) for row in base_table.rows]

        for join_spec in statement.joins
            right_table = _require_table(db, join_spec.right_table)
            next_rows = Dict{Symbol, Any}[]

            for left_row in joined_rows
                for right_raw_row in right_table.rows
                    candidate = copy(left_row)
                    merge!(candidate, _qualify_row(join_spec.right_table, right_raw_row))
                    if _eval_expr(join_spec.on_expr, candidate)
                        push!(next_rows, candidate)
                    end
                end
            end

            joined_rows = next_rows
        end

        selected_columns = if statement.columns === nothing
            cols = ColumnRef[ColumnRef(statement.from_table, c) for c in base_table.columns]
            for join_spec in statement.joins
                t = _require_table(db, join_spec.right_table)
                append!(cols, [ColumnRef(join_spec.right_table, c) for c in t.columns])
            end
            cols
        else
            statement.columns
        end

        rows = Vector{Vector{Any}}()
        for row in joined_rows
            if statement.where_expr === nothing || _eval_expr(statement.where_expr, row)
                push!(rows, [_resolve_column_ref(row, c) for c in selected_columns])
            end
        end

        return QueryResult([_column_ref_symbol(c) for c in selected_columns], rows)
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
