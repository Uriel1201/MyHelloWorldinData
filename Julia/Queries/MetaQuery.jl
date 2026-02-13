module MetaQuery

    export get_query, table_name


    """
        get_query(filename::AbstractString) -> String

    Returns the SQL query from an SQL file as a String:
    """
    function get_query(filename::AbstractString)::String
        
        if !isfile(filename)
            throw(ArgumentError("file '$filename' not found"))
        end

        query = open(filename) do file
            read(file, String)
        end

        return query
    
    end


    """
        table_name(query::String) -> String

    Returns the table name from an SQL query:
    """
    function table_name(query::String)::String

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
