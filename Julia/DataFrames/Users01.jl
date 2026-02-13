module Users01

    using DataFrames, Arrow

    export cancellation_rates

    
    """
        cancellation_rates(table::Arrow.Table) -> DataFrame

    Computes per user cancellation and publication rates:
    - `CANCEL_RATE`  = (#cancels / #starts) or `missing` if there are no starts.
    - `PUBLISH_RATE` = (#publishes / #starts) or `missing` if there are no starts.
    """
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
