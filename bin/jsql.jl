#!/usr/bin/env julia

using JSQL

function usage(io::IO = stdout)
    println(io, "Usage: jsql [--file <path>] [--execute <sql>]")
    println(io, "")
    println(io, "Options:")
    println(io, "  -f, --file <path>     Execute SQL statements from file")
    println(io, "  -e, --execute <sql>   Execute one SQL statement")
    println(io, "  -h, --help            Show this help")
    println(io, "")
    println(io, "When no options are provided:")
    println(io, "  - If stdin is piped, statements are read from stdin")
    println(io, "  - Otherwise, an interactive prompt starts")
end

function parse_args(args::Vector{String})
    file = nothing
    execute_sql = nothing
    i = 1

    while i <= length(args)
        arg = args[i]
        if arg == "-h" || arg == "--help"
            usage()
            return nothing
        elseif arg == "-f" || arg == "--file"
            i += 1
            i <= length(args) || error("Missing value for $(arg)")
            file = args[i]
        elseif arg == "-e" || arg == "--execute"
            i += 1
            i <= length(args) || error("Missing value for $(arg)")
            execute_sql = args[i]
        else
            error("Unknown argument: $(arg)")
        end
        i += 1
    end

    return (file = file, execute_sql = execute_sql)
end

function split_sql_statements(input::String)
    statements = String[]
    buffer = IOBuffer()
    in_string = false

    for ch in input
        if ch == '\''
            in_string = !in_string
            write(buffer, ch)
            continue
        end

        if ch == ';' && !in_string
            stmt = strip(String(take!(buffer)))
            isempty(stmt) || push!(statements, stmt)
            continue
        end

        write(buffer, ch)
    end

    tail = strip(String(take!(buffer)))
    isempty(tail) || push!(statements, tail)

    return statements
end

function print_result(result::QueryResult)
    headers = String.(result.columns)
    rows = [map(x -> x === nothing ? "NULL" : string(x), row) for row in result.rows]

    widths = [length(h) for h in headers]
    for row in rows
        for i in eachindex(row)
            widths[i] = max(widths[i], length(row[i]))
        end
    end

    line = join([rpad(headers[i], widths[i]) for i in eachindex(headers)], " | ")
    println(line)
    println(join([repeat("-", widths[i]) for i in eachindex(widths)], "-+-"))

    for row in rows
        println(join([rpad(row[i], widths[i]) for i in eachindex(row)], " | "))
    end

    println("($(length(rows)) rows)")
end

function execute_statement!(db::Database, sql::String)
    result = execute!(db, sql)
    if result isa QueryResult
        print_result(result)
    else
        println("OK")
    end
end

function run_batch!(db::Database, sql_text::String)
    for statement in split_sql_statements(sql_text)
        execute_statement!(db, statement)
    end
end

function run_repl!()
    db = Database()
    println("jsql interactive mode")
    println("Type .exit or .quit to stop")

    while true
        print("jsql> ")
        line = readline(stdin; keep = false)
        line = strip(line)

        if isempty(line)
            continue
        elseif line == ".exit" || line == ".quit"
            break
        end

        try
            execute_statement!(db, line)
        catch err
            println(stderr, "Error: ", err)
        end
    end
end

function main(args::Vector{String})
    options = try
        parse_args(args)
    catch err
        println(stderr, "Error: ", err)
        usage(stderr)
        return 1
    end

    options === nothing && return 0

    db = Database()

    try
        if options.execute_sql !== nothing
            run_batch!(db, options.execute_sql)
            return 0
        elseif options.file !== nothing
            sql_text = read(options.file, String)
            run_batch!(db, sql_text)
            return 0
        elseif !Base.isatty(stdin)
            sql_text = read(stdin, String)
            run_batch!(db, sql_text)
            return 0
        else
            run_repl!()
            return 0
        end
    catch err
        println(stderr, "Error: ", err)
        return 1
    end
end

exit(main(ARGS))
