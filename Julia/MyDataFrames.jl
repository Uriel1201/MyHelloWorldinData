module MyDataFrames

    using Arrow, DataFrames

    """
        cancellation_rates(users_01::Arrow.Table) -> DataFrame

    Computes per user cancellation and publication rates:
    - `CANCEL_RATE`  = (#cancels / #starts) or `missing` if there are no starts.
    - `PUBLISH_RATE` = (#publishes / #starts) or `missing` if there are no starts.
    """
    function cancellation_rates(users_01::Arrow.Table)::DataFrame

        df = DataFrame(users_01, copycols = false)

        return combine(groupby(df, :USER_ID)) do sdf

                   actions = skipmissing(sdf.ACTION)
                   starts    = count(==("start"),   actions) 
                   cancels   = count(==("cancel"),  actions)  
                   publishes = count(==("publish"), actions)

                   (CANCEL_RATE = starts > 0 ? cancels / starts : missing,
                    PUBLISH_RATE = starts > 0 ? publishes/ starts : missing
                   )

               end

    end

    function main(ArrowFilename::AbstractString)

        if !endswith(ArrowFilename, ".arrow")
            throw(ArgumentError("File must have extension.arrow: $ArrowFilename"))
        end
    
    
        println("Processing Arrow File: $ArrowFilename")
 
        table = Arrow.Table(ArrowFilename)
        print("\nCancellation Rates By Each User\n")
        print(cancellation_rates(table))

    end

end

if Base.@isdefined(PROGRAM_FILE) &&

    abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    main(a)

end
