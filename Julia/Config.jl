module Config

    export REGISTERED_TABLES
    export MyArrowTable 

    const REGISTERED_TABLES = Set(["USERS_01"])
    
    struct MyArrowTable
            name::String
            table::Arrow.Table
    end

end
