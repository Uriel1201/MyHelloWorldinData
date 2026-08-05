module MyQueries

    using Arrow, DuckDB, ..SQLtoArrow, ..Config

    function get_duck_query(duck::DuckDB.DB, ArrowTable::Config.MyArrowTable, DuckQuery::AbstractString)::DuckDB.QueryResult

        if !(ArrowTable.name in Config.REGISTERED_TABLES)
            throw(ArgumentError("Table name not available: $TableName"))
        end
    
        query = get_query(DuckQuery)
        DuckDB.register_data_frame(duck, ArrowTable.table, ArrowTable.name)
        return cursor = DBInterface.execute(duck, query)

    end

    function print_duck_query(cursor::DuckDB.QueryResult)::Nothing 

        for (i, row) in enumerate(cursor)

            println(row)
            i >= 15 && break

        end

    end

    function main(ArrowFile::AbstractString, DuckQuery::AbstractString)

        print_duck_query(ArrowFile, DuckQuery)

    end

end

if Base.@isdefined(PROGRAM_FILE) &&

    abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    b = ARGS[2]
    main(a, b)

end
