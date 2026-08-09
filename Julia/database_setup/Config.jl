module Config

    using Arrow

    export REGISTERED_TABLES, MyArrowTable, get_my_arrow_table

    const REGISTERED_TABLES = Set(["USERS_01"])
    
    struct MyArrowTable
               name::String
               table::Arrow.Table
    end

    struct DatabaseConfig

               db_path::String

    end

end
