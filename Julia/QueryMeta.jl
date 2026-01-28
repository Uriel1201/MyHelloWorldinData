module QueryMeta

    export get_query, get_table_name
    #****************************************************************
    #*  get_query:
    #** params:
    #****************************************************************
    function get_query(filename::AbstractString)::String
        
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
    #*  get_table_name:
    #** params:
    #****************************************************************
    function get_table_name(query::String)::String

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
