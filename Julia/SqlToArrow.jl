module SQLtoArrow

    using Arrow, DuckDB, DBInterface 

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

    function sqlite_to_arrow(query::String, output::String)::Nothing

        duck = DBInterface.connect(DuckDB.DB, ":memory:")

        try
            DBInterface.execute(duck, "LOAD sqlite")
            cursor = DBInterface.execute(duck, query)
            Arrow.write(output, cursor)
        
        finally 
            DBInterface.close!(duck)

        end

        return 
            nothing

    end

    function main(SQLiteFilename::AbstractString, OutputFile::String)

        query = get_query(SQLiteFilename)
        sqlite_to_arrow(query, "$(OutputFile).arrow")

    end

end

if Base.@isdefined(PROGRAM_FILE) &&

    abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    b = ARGS[2]
    main(a, b)

end
