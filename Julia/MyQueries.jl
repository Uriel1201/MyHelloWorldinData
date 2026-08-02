module MyQueries

    using Arrow, DuckDB, ..SQLtoArrow
    const REGISTERED_TABLES = Set(["USERS_01"])

    struct MyArrowTable
        name::String
        table::Arrow.Table
    end

    function duck_query(duck::DuckDB.DB, ArrowTable::MyArrowTable, DuckQuery::AbstractString)::DuckDB.QueryResult

        query = SQLtoArrow.get_query(DuckQuery)
        DuckDB.register_data_frame(duck, ArrowTable.table, ArrowTable.name)
        return cursor = DBInterface.execute(duck, query)

    end

    function main(ArrowFile::AbstractString, DuckQuery::AbstractString, TableName::String)

        duck = DBInterface.connect(DuckDB.DB, ":memory:")

        arrow_table = MyArrowTable(
                          TableName,
                          Arrow.Table(ArrowFile)              
        )

        if !(TableName in REGISTERED_TABLES)
            throw(ArgumentError("Table name not available: $TableName"))
        end

        query = duck_query(duck, arrow_table, DuckQuery)
        for (i, row) in enumerate(query)

            println(row)
            i >= 15 && break

        end

    end

end

if Base.@isdefined(PROGRAM_FILE) &&

    abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    b = ARGS[2]
    c = ARGS[3]
    main(a, b, c)

end
