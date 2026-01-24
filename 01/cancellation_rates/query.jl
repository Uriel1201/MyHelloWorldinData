const DB_PATH = "my_SQLite.db"
using .MyDataBase, DataFrames, Arrow, SQLite, DuckDB, .SQLiteArrowKit, PrettyTables 

MyDataBase.main() #This line saves the database 'mySQLite.db to disk
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

            arrow_users = get_ArrowTable(db, args)
            println("Loading 'USERS_01' table from disk using Arrow format:\n$arrow_users")
            DuckDB.register_data_frame(duck, arrow_users, "arrow_users")
            sample = DBInterface.execute(duck, "SELECT * FROM 'arrow_users' USING SAMPLE 50% (bernoulli)")
            println("\n", "*"^40)
            println("Querying table using DuckDB (SAMPLE):")
            pretty_table(sample)
            query = """
            WITH
                DUCK_UPDATED AS (
                    SELECT
                        USER_ID,
                        ACTION,
                        STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
                    FROM
                        'arrow_users'),
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
            duck_result = DBInterface.execute(duck, query)
            println("\n**DuckDB**\nQuerying publication and cancellation rates for each user:")
            print_DuckTable(duck_result)

            users = arrow_users |> DataFrame
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
            println("\n**DataFrames.jl**\nQuerying cancellation and publication rates for each user:\n$result")

        finally

            DBInterface.close!(duck)

        end

    else

        println("TABLE $args NOT AVAILABLE")

    end

end
#=
**********************************************
=#
if Base.@isdefined(PROGRAM_FILE) &&
   abspath(PROGRAM_FILE) == abspath(@__FILE__)

    main()

end
