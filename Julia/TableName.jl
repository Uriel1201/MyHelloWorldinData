module TableName

    export get_table_name

    #****************************************************************
    #*  table_name:
    #** filename:
    #****************************************************************
    function get_table_name(filename::AbstractString)::String

        query = open(filename) do file
            read(file, String)
        end
  
        table_name = match(r"FROM\s+'(\w+)'", query).captures[1]

        return table_name

    end

end
