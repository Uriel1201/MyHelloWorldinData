module ArrowDuckQuery

    using ..MetaQuery, Arrow, DuckDB

    export get_DuckQuery

    #****************************************************************
    #*  get_DuckQuery:
    #** params:
    #****************************************************************
    function get_DuckQuery(duck::DuckDB.DB, ArrowTable::Arrow.Table, DuckQueryFilename::AbstractString)::DuckDB.QueryResult

        if !isfile(DuckQueryFilename)
            throw(ArgumentError("'$DuckQueryFilename' does not exist"))
        end

        query = get_query(DuckQueryFilename)
        table_name = get_table_name(query)
        DuckDB.register_data_frame(duck, ArrowTable, table_name)

        return DBInterface.execute(duck, query)

    end
    #****************************************************************

end
