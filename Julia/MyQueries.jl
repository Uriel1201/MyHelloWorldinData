module MyQueries

    using Arrow, DuckDB, ..SQLtoArrow
    const REGISTERED_TABLES = Set(["USERS_01"])

    function duck_query(duck::DuckDB.DB, ArrowFile::AbstractString, DuckQuery::AbstractString)::Nothing 

        table_name = first(splitext(basename(ArrowFile)))
        if !(table_name in REGISTERED_TABLES)
            throw(ArgumentError("Table name not available: $table_name"))
        end

        query = SQLtoArrow.get_query(DuckQuery)
        table = Arrow.Table(ArrowFile)
        DuckDB.register_data_frame(duck, table, table_name)

        cursor = DBInterface.execute(duck, query)

        for (i, row) in enumerate(cursor)

            println(row)
            i >= 15 && break

        end

    end

    function main(ArrowFile::AbstractString, DuckQuery::AbstractString)

        duck = DBInterface.connect(DuckDB.DB, ":memory:")
        duck_query(duck, ArrowFile, DuckQuery)
        DBInterface.close!(duck)

    end

end

if Base.@isdefined(PROGRAM_FILE) &&

    abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    b = ARGS[2]
    main(a, b)

end
