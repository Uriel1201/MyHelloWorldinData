module MyQueries

    using Arrow, DuckDB, ..SQLtoArrow
    const REGISTERED_TABLES = Set(["USERS_01"])

    struct MyArrowTable
        name::String
        table::Arrow.Table
    end

    function get_duck_query(duck::DuckDB.DB, ArrowTable::MyArrowTable, DuckQuery::AbstractString)::DuckDB.QueryResult

        query = get_query(DuckQuery)
        DuckDB.register_data_frame(duck, ArrowTable.table, ArrowTable.name)
        return cursor = DBInterface.execute(duck, query)

    end

    function get_my_arrow_table(ArrowFile::AbstractString)::MyArrowTable

        return MyArrowTable(
                   splitext(basename(ArrowFile))[1],
                   Arrow.Table(ArrowFile)
               )
    
    end

    function print_duck_query(ArrowFile::AbstractString, DuckQuery::AbstractString)::Nothing 

        duck = DBInterface.connect(DuckDB.DB, ":memory:")
        my_table = get_my_arrow_table(ArrowFile)
        if !(my_table.name in REGISTERED_TABLES)
            throw(ArgumentError("Table name not available: $TableName"))
        end

        query = get_duck_query(duck, my_table, DuckQuery)
        DBInterface.close!(duck)
        for (i, row) in enumerate(query)

            println(row)
            i >= 15 && break

        end

    end

    function main(ArrowFile::AbstractString, DuckQuery::AbstractString, TableName::String)

        print_duck_query(ArrowFile, DuckQuery, TableName)

    end

end

if Base.@isdefined(PROGRAM_FILE) &&

    abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    b = ARGS[2]
    main(a, b)

end
