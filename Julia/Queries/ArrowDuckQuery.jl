module ArrowDuckQuery

    using ..MetaQuery, Arrow, DuckDB

    export get_duck_query

    #****************************************************************
    #*  get_duck_query:
    #** params:
    #****************************************************************
    function get_DuckQuery(duck::DuckDB.DB, table::Arrow.Table, DuckQueryFilename::AbstractString)::DuckDB.QueryResult
        
        query = get_query(DuckQueryFilename)
        table_name = get_table_name(query)
        DuckDB.register_data_frame(duck, ArrowTable, table_name)

        return DBInterface.execute(duck, query)

    end
    #****************************************************************

end
