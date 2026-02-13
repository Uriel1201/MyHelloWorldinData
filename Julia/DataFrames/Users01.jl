module Users01

    using DataFrames, Arrow

    export cancellation_rates

    #****************************************************************
    #*  cancellation_rates:
    #** params:
    #****************************************************************
    function cancellation_rates(Table::Arrow.Table)::DataFrame

        df = DataFrame(Table, copycols = false)

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
    #****************************************************************

end
