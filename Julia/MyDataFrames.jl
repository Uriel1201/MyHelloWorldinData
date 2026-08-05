module MyDataFrames

    using Arrow, DataFrames, ..Config

    """
        cancellation_rates(users_01::Arrow.Table) -> DataFrame

    Computes per user cancellation and publication rates:
    - `CANCEL_RATE`  = (#cancels / #starts) or `missing` if there are no starts.
    - `PUBLISH_RATE` = (#publishes / #starts) or `missing` if there are no starts.
    """
    function cancellation_rates_01(users_01::Config.MyArrowTable)::DataFrame

        if !(users_01.name == "USERS_01")
            throw(ArgumentError("Table name not validated: $users_01"))
        end
        df = DataFrame(users_01.table, copycols = false)

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
