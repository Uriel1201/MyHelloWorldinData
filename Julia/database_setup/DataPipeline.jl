module DataPipeline

    include("Config.jl")
    include("MyDataBase.jl")
    include("SQLtoArrow.jl")
    include("MyQueries.jl")
    include("MyDataFrames.jl")

    using .MyDataBase, .Config, DBInterface, DuckDB, .MyQueries, .MyDataFrames 
    import .SQLtoArrow: main as convert_file

    MyDataBase.construct_database()

end
