module Users01

    using DataFrames, Arrow

    export cancellation_rates

    #****************************************************************
    #*  cancellation_rates:
    #** params:
    #****************************************************************
    function cancellation_rates(table::Arrow.Table)::DataFrame

        df = DataFrame(table, copycols = false)
        dummy = select(df,
                       :USER_ID,
                       [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(df.ACTION)]
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

        return result 

    end
    #****************************************************************

end
