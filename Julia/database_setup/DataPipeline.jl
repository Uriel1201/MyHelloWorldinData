module DataPipeline

include("Extract/sqlite_to_arrow.jl")
include("Analytics/duckdb_queries.jl")
include("Analytics/dataframe_analysis.jl")

export create_arrow
export run_duckdb_analysis
export run_dataframe_analysis

end
