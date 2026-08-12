module SQLtoArrow

    using Arrow, DuckDB, DBInterface, .Config, LibPQ
    export get_query, sqlite_to_arrow

    """
        get_query(filename::AbstractString) -> String

        Returns the SQL query from an SQL file as a String:
    """
    function get_query(filename::AbstractString)::String

        if !isfile(filename)
            throw(ArgumentError("file '$filename' not found"))
        end

        query = open(filename) do file
            read(file, String)
        end

        return query

    end

    """
        sqlite_to_arrow(input::String, output::String) -> String
    """
    function sqlite_to_arrow(input::String, output::String)::Nothing

        duck = DBInterface.connect(DuckDB.DB, ":memory:")

        try

            DBInterface.execute(duck, "LOAD sqlite")
            cursor = DBInterface.execute(duck, input)
            Arrow.write("$output.arrow", cursor)

        catch e

            @error "Error processing query: $e"
            rethrow(e)

        finally

            DBInterface.close!(duck)

        end

        return

            nothing

    end

    """
        postgresql_to_arrow(conn::LibPQ.Connection, input::String, output::String) -> String
    """
    function postgresql_to_arrow(conn:: LibPQ.Connection, input::String, output::String)::Nothing

        try
            result = LibPQ.execute(conn, input)
            Arrow.write("$output.arrow", result)

        catch e

            @error "Error processing query: $e"
            rethrow(e)

        finally

            return nothing 

        end

    end

    function get_my_arrow_table(ArrowFile::AbstractString)::MyArrowTable

        return Config.MyArrowTable(
                   splitext(basename(ArrowFile))[1],
                   Arrow.Table(ArrowFile)
               )
    
    end

end
