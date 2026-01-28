module ArrowDuckQuery

    using .MetaQuery, Arrow, DuckDB, PrettyTables

    export get_ArrowDuckQuery

    #****************************************************************
    #*  get_ArrowDuckQuery:
    #** params:
    #****************************************************************
    function get_ArrowDuckQuery(duck::DuckDB.DB, ArrowFilename::AbstractString, DuckQuery:AbstractString)::DuckDB.QueryResult

        arrow_table = Arrow.Table(ArrowFilename)
        query = get_query(DuckQuery)
        table_name = get_table_name(query)
        DuckDB.register_data_frame(duck, arrow_table, table_name)

        return DBInterface.execute(duck, query)

    end
    #****************************************************************

end
