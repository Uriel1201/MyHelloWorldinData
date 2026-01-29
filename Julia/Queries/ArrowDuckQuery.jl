module ArrowDuckQuery

    using ..MetaQuery, Arrow, DuckDB

    export get_ArrowDuckQuery

    #****************************************************************
    #*  get_ArrowDuckQuery:
    #** params:
    #****************************************************************
    function get_ArrowDuckQuery(duck::DuckDB.DB, ArrowFilename::AbstractString, DuckQueryFilename::AbstractString)::DuckDB.QueryResult

        if !isfile(ArrowFilename)
            throw(ArgumentError("'$ArrowFilename' does not exist"))
        end

        if !isfile(DuckQueryFilename)
            throw(ArgumentError("'$DuckQueryFilename' does not exist"))
        end

        arrow_table = Arrow.Table(ArrowFilename)
        query = get_query(DuckQueryFilename)
        table_name = get_table_name(query)
        DuckDB.register_data_frame(duck, arrow_table, table_name)

        return DBInterface.execute(duck, query)

    end
    #****************************************************************

end
