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

                   starts = 0
                   cancels = 0
                   publishes = 0

                   for a in sdf.ACTION

                       starts += a == "start"
                       cancels += a == "cancel"
                       publishes += a == "publish"

                   end

                   (CANCEL_RATE = starts > 0 ? cancels / starts : missing,
                    PUBLISH_RATE = starts > 0 ? publishes/ starts : missing
                   )

               end

    end
    #****************************************************************

end
