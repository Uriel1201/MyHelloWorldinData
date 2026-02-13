module Users01

    using DataFrames, Arrow

    export cancellation_rates

    
    """
        cancellation_rates(table::Arrow.Table) -> DataFrame

    Calcula para cada usuario las tasas de cancelación y publicación:
    - `CANCEL_RATE`  = (#cancel / #start) o `missing` si no hay starts.
    - `PUBLISH_RATE` = (#publish / #start) o `missing` si no hay starts.
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
