module DataPipeline

    include("Config.jl")
    include("MyDataBase.jl")
    include("SQLtoArrow.jl")
    include("MyQueries.jl")
    include("MyDataFrames.jl")

    using .MyDataBase, .Config, DBInterface, DuckDB, .MyQueries, .MyDataFrames 
    import .SQLtoArrow: main as convert_file

    MyDataBase.construct_database()

    function print_data_analysis(ArrowFile::AbstractString, DuckFile::AbstractString)::Nothing

        my_table = Config.get_my_arrow_table(ArrowFile)
        duck = DBInterface.connect(DuckDB.DB, ":memory:")
        if (my_table.name == "USERS_01")
            println("USING JULIA DATAFRAMES TO GET CANCELLATION RATES FOR EACH USER")
            println(MyDataFrames.cancellation_rates_01(my_table))
        end

        println("\nUSING DUCKDB TO PRINT THE CORRESPONDING QUERY")
        query = MyQueries.get_duck_query(duck, my_table, "rates.sql")
        MyQueries.print_duck_query(query)
        DBInterface.close!(duck)

    end

end
