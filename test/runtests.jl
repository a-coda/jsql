using Test
using JSQL

@testset "JSQL" begin
    db = Database()

    execute!(db, "CREATE TABLE users (id, name, age)")
    execute!(db, "INSERT INTO users VALUES (1, 'Ada', 37)")
    execute!(db, "INSERT INTO users (id, name, age) VALUES (2, 'Grace', 30)")

    all_rows = execute!(db, "SELECT * FROM users")
    @test all_rows.columns == [:id, :name, :age]
    @test length(all_rows.rows) == 2

    filtered = execute!(db, "SELECT name FROM users WHERE age >= 35")
    @test filtered.columns == [:name]
    @test filtered.rows == [["Ada"]]
end

@testset "Boolean WHERE precedence" begin
    db = Database()
    execute!(db, "CREATE TABLE events (id, category, score)")
    execute!(db, "INSERT INTO events VALUES (1, 'A', 10)")
    execute!(db, "INSERT INTO events VALUES (2, 'B', 20)")
    execute!(db, "INSERT INTO events VALUES (3, 'A', 30)")

    # AND should bind tighter than OR.
    result = execute!(db, "SELECT id FROM events WHERE category = 'A' OR category = 'B' AND score > 25")
    @test result.rows == [[1], [3]]
end

@testset "CSV loading API and SQL" begin
    db = Database()

    mktemp() do path, io
        write(io, "id,name,score\n1,Ada,99\n2,Grace,88\n")
        close(io)

        load_csv!(db, "scores", path)
        from_api = execute!(db, "SELECT name FROM scores WHERE score >= 90")
        @test from_api.rows == [["Ada"]]
    end

    mktemp() do path, io
        write(io, "id,name\n1,Lin\n")
        close(io)

        execute!(db, "LOAD CSV '$path' INTO imported")
        from_sql = execute!(db, "SELECT * FROM imported")
        @test from_sql.columns == [:id, :name]
        @test from_sql.rows == [[1, "Lin"]]
    end
end
