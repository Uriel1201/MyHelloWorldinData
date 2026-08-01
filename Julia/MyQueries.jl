module MyQueries 

    using Arrow, DuckDB, ..SQLtoArrow
    
    function duck_query(duck::DuckDB.DB, ArrowFile::AbstractString, DuckQuery::AbstractString)::DuckDB.QueryResult
   
        list = ["USERS_01"]
        table_name = first(splitext(ArrowFile))
        if !(table_name in list)
            throw(ArgumentError("Table not registered: $TableName"))
        end
        
        query = SQLtoArrow.get_query(DuckQuery)
        table = Arrow.Table(ArrowFile)
        DuckDB.register_data_frame(duck, table, table_name)
      
        return DBInterface.execute(duck, query)

    end

    function main(ArrowFile::AbstractString, DuckQuery::AbstractString)

        duck = DBInterface.connect(DuckDB.DB, ":memory:")
        query = duck_query(duck, ArrowFile, DuckQuery)

        for (i, row) in enumerate(query)

            println(row)
            i >= 10 && break

        end

    end

end

if Base.@isdefined(PROGRAM_FILE) &&

    abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    b = ARGS[2]
    main(a, b)

end
