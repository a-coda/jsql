using Test

@testset "CLI" begin
    cli = normpath(joinpath(@__DIR__, "..", "bin", "jsql.jl"))
    project_root = normpath(joinpath(@__DIR__, ".."))

    function run_cli(args::Vector{String}; stdin_data::Union{Nothing, String} = nothing)
        cmd = `$(Base.julia_cmd()) --project=$project_root $cli $args`
        out = IOBuffer()
        err = IOBuffer()
        proc_cmd = pipeline(ignorestatus(cmd); stdout = out, stderr = err)

        proc = if stdin_data === nothing
            run(proc_cmd)
        else
            run(pipeline(IOBuffer(stdin_data), proc_cmd))
        end

        return (code = proc.exitcode, stdout = String(take!(out)), stderr = String(take!(err)))
    end

    @testset "Help and arg errors" begin
        help = run_cli(["--help"])
        @test help.code == 0
        @test occursin("Usage: jsql", help.stdout)

        unknown = run_cli(["--nope"])
        @test unknown.code != 0
        @test occursin("Unknown argument", unknown.stderr)

        missing = run_cli(["--file"])
        @test missing.code != 0
        @test occursin("Missing value", missing.stderr)
    end

    @testset "Execute mode output" begin
        run1 = run_cli(["--execute", "CREATE TABLE users (id, name); INSERT INTO users VALUES (1, 'Ada'); SELECT * FROM users"])
        @test run1.code == 0
        @test occursin("OK", run1.stdout)
        @test occursin("id | name", run1.stdout)
        @test occursin("(1 rows)", run1.stdout)

        run2 = run_cli(["--execute", "CREATE TABLE users (id, nickname); INSERT INTO users VALUES (1, NULL); SELECT * FROM users"])
        @test run2.code == 0
        @test occursin("NULL", run2.stdout)
    end

    @testset "File and stdin modes" begin
        mktemp() do path, io
            write(io, "CREATE TABLE logs (id, txt);\n")
            write(io, "INSERT INTO logs VALUES (1, 'hello');\n")
            write(io, "SELECT * FROM logs;\n")
            close(io)

            file_run = run_cli(["--file", path])
            @test file_run.code == 0
            @test occursin("id | txt", file_run.stdout)
            @test occursin("hello", file_run.stdout)
        end

        stdin_sql = "CREATE TABLE t (x); INSERT INTO t VALUES (7); SELECT * FROM t;"
        if Sys.iswindows()
            @test_skip "stdin-mode subprocess piping is unreliable on Windows in this harness"
        else
            stdin_run = run_cli(String[]; stdin_data = stdin_sql)
            @test stdin_run.code == 0
            @test occursin("x", stdin_run.stdout)
            @test occursin("7", stdin_run.stdout)
        end
    end

    @testset "Invalid SQL returns non-zero" begin
        bad = run_cli(["--execute", "THIS IS NOT SQL"])
        @test bad.code != 0
        @test occursin("Error:", bad.stderr)
    end
end
