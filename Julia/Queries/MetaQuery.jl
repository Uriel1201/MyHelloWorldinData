module MetaQuery

    export get_Query, get_TableName
    #****************************************************************
    #*  get_Query:
    #** params:
    #****************************************************************
    function get_Query(filename::AbstractString)::String
        
        if !isfile(filename)
            throw(ArgumentError("'$filename' does not exist"))
        end

        query = open(filename) do file
            read(file, String)
        end

        return query
    
    end
    #****************************************************************
    
    #****************************************************************
    #*  get_TableName:
    #** params:
    #****************************************************************
    function get_TableName(query::String)::String

        m = match(r"FROM\s+'(\w+)'", query).captures[1]
    
        if m === nothing
            throw(ArgumentError("""Unable to locate the 
                                   table name in your SQL query 
                                """
                  )
            )
        end
    
        return m

    end
    #****************************************************************

end
