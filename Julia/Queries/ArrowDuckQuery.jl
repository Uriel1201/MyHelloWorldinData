module ArrowDuckQuery

    using ..MetaQuery, Arrow, DuckDB

    export get_arrow_query

    """
        get_arrow_query(duck::DuckDB.DB, table::Arrow.Table, DuckQueryFilename::AbstractString) -> DuckDB.QueryResult

    Returns the SQL query from an SQL file as a String:
    """
    function get_arrow_query(duck::DuckDB.DB, table::Arrow.Table, DuckQueryFilename::AbstractString)::DuckDB.QueryResult
        
        query = get_query(DuckQueryFilename)
        name = table_name(query)
        DuckDB.register_data_frame(duck, table, name)

        return DBInterface.execute(duck, query)

    end
    #****************************************************************

end
