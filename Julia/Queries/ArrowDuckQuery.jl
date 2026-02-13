module ArrowDuckQuery

    using ..MetaQuery, Arrow, DuckDB

    export get_duck_query

    #****************************************************************
    #*  get_duck_query:
    #** params:
    #****************************************************************
    function get_duck_query(duck::DuckDB.DB, table::Arrow.Table, DuckQueryFilename::AbstractString)::DuckDB.QueryResult
        
        query = get_query(DuckQueryFilename)
        name = table_name(query)
        DuckDB.register_data_frame(duck, table, name)

        return DBInterface.execute(duck, query)

    end
    #****************************************************************

end
