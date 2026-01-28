const DB_PATH = "MyDataBase.db"
using DataFrames, Arrow, SQLite, DuckDB, PrettyTables, BenchmarkTools

#=
**********************************************
01. Cancellation Rates.

Write a query to return the publication and cancellation
rate for each user.
**********************************************
=#

function is_available(db::SQLite.DB, table::String)::Bool

    name = uppercase(table)
    list_tables = collect(SQLite.tables(db))
    names = [t.name for t in list_tables]

    return name in names

end

#=
**********************************************
=#

function main(args = ARGS)

    db = SQLite.DB(DB_PATH)
    name = uppercase(args)

    if is_available(db, args) && (name == "USERS_01")

        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")

            println("--- WORKING WITH ARROW ARCHIVE ---\n")
            arrow_users = Arrow.Table("users.arrow")
            println("Loading in-Arrow-format 'USERS_01' table:\n$arrow_users")

            println("\n*** DuckdDB ***\n")
            DuckDB.register_data_frame(duck, arrow_users, "users")
            sample = DBInterface.execute(duck, "SELECT * FROM 'users' USING SAMPLE 50% (bernoulli)")
            println("USERS_01 arrow table (sample):")
            pretty_table(sample)

            arrow_query = """
            WITH
                DUCK_UPDATED AS (
                    SELECT
                        USER_ID,
                        ACTION,
                        STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
                    FROM
                        'users'),
                TOTALS AS (
                    SELECT
                        USER_ID,
                        SUM(IF(ACTION = 'start',1,0)) AS TOTAL_STARTS,
                        SUM(IF(ACTION = 'cancel',1,0)) AS TOTAL_CANCELS,
                        SUM(IF(ACTION = 'publish',1,0)) AS TOTAL_PUBLISHES
                    FROM
                        DUCK_UPDATED
                    GROUP BY
                        USER_ID)
            SELECT
                USER_ID,
                ROUND(TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS,
                                               0),
                      2) AS PUBLISH_RATE,
                ROUND(TOTAL_CANCELS / NULLIF(TOTAL_STARTS,
                                             0),
                      2) AS CANCEL_RATE
            FROM
                TOTALS
            ORDER BY
                1
            """
            arrow_result = DBInterface.execute(duck, arrow_query)
            println("\nQuerying publication and cancellation rates for each user:")
            pretty_table(arrow_result)

            users = DataFrame(arrow_users, copycols = false)
            dummy = select(users,
                           :USER_ID,
                           [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(users.ACTION)]
                    )

            result = combine(groupby(dummy,
                                     :USER_ID
                             ),
                             names(dummy,
                                   Not(:USER_ID)
                             ) .=> sum
                     )

            result.CANCEL_RATE = @.ifelse(result.start_sum != 0, result.cancel_sum ./ result.start_sum, NaN)
            result.PUBLISH_RATE = @.ifelse(result.start_sum != 0, result.publish_sum ./ result.start_sum, NaN)
            println("\n*** DataFrames.jl ***\n\nQuerying cancellation and publication rates for each user:\n$result\n")

            println("\n--- WORKING DIRECTLY WITH SQLITE DATABASE ---\n")
            println("*** DuckDB ***\n")
            DBInterface.execute(duck, "INSTALL sqlite")
            DBInterface.execute(duck, "LOAD sqlite")
            sqlite_query = """
            WITH
                DUCK_UPDATED AS (
                    SELECT
                        USER_ID,
                        ACTION,
                        STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
                    FROM
                        sqlite_scan('MyDataBase.db', $name)),
                TOTALS AS (
                    SELECT
                        USER_ID,
                        SUM(IF(ACTION = 'start',1,0)) AS TOTAL_STARTS,
                        SUM(IF(ACTION = 'cancel',1,0)) AS TOTAL_CANCELS,
                        SUM(IF(ACTION = 'publish',1,0)) AS TOTAL_PUBLISHES
                    FROM
                        DUCK_UPDATED
                    GROUP BY
                        USER_ID)
            SELECT
                USER_ID,
                ROUND(TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS,
                                               0),
                      2) AS PUBLISH_RATE,
                ROUND(TOTAL_CANCELS / NULLIF(TOTAL_STARTS,
                                             0),
                      2) AS CANCEL_RATE
            FROM
                TOTALS
            ORDER BY
                1
            """

            sqlite_result = DBInterface.execute(duck, sqlite_query)
            println("Querying publication and cancellation rates for each user:")
            pretty_table(sqlite_result)

        finally

            DBInterface.close!(duck)

        end

    else

        println("TABLE $args NOT AVAILABLE")

    end

end

#=
**********************************************
**********************************************
=#

if Base.@isdefined(PROGRAM_FILE) &&
   abspath(PROGRAM_FILE) == abspath(@__FILE__)

    main()

end
