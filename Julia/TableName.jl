module TableName

    export get_table_name

    #****************************************************************
    #*  table_name:
    #** filename:
    #****************************************************************
    function get_table_name(filename::AbstractString)::String

        if !isfile(filename)
            throw(ArgumentError("'$filename' does not exist"))
        end
    
        query = open(filename) do file
            read(file, String)
        end
  
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

end
