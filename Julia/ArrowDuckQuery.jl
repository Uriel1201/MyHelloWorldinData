module ArrowDuckQuery

    using .MetaQuery, Arrow, DuckDB, PrettyTables

    export get_ArrowDuckQuery

    #****************************************************************
    #*  get_ArrowDuckQuery:
    #** params:
    #****************************************************************
    function get_ArrowDuckQuery(ArrowFilename::AbstractString, Query:String)::DuckDB.QueryResult

    end
    #****************************************************************

end
