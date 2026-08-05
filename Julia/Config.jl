module Config

    using Arrow

    export REGISTERED_TABLES
    export MyArrowTable 
    export get_my_arrow_table

    const REGISTERED_TABLES = Set(["USERS_01"])
    
    struct MyArrowTable
            name::String
            table::Arrow.Table
    end

    function get_my_arrow_table(ArrowFile::AbstractString)::MyArrowTable

        return Config.MyArrowTable(
                   splitext(basename(ArrowFile))[1],
                   Arrow.Table(ArrowFile)
               )
    
    end

end
