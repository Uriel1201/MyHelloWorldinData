module SQLtoArrow

    using Arrow, SQLite, DuckDB, ..MetaQuery

    function sqlite_to_arrow(query::String, output::String="output.arrow")::Nothing

        duck = DBInterface.connect(DuckDB.DB, ":memory:")
        DBInterface.execute(duck, "LOAD sqlite")
        cursor = DBInterface.execute(duck, query)
        Arrow.write(output, cursor)
        DBInterface.close!(duck)

        return nothing

    end

    function main(SQLiteFilename::AbstractString)

        query = MetaQuery.get_query(SQLiteFilename)
        outputfile = MetaQuery.table_name(query)
        sqlite_to_arrow(query, "$(outputfile).arrow")

    end

end

if Base.@isdefined(PROGRAM_FILE) &&

    abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    main(a)

end
