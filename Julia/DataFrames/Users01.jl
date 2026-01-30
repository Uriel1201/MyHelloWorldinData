module Users01

    using DataFrames, Arrow

    export get_CancellationRates

    #****************************************************************
    #*  get_CancellationRates:
    #** params:
    #****************************************************************
    function get_CancellationRates(ArrowTable::Arrow.Table)::DataFrame

        df = DataFrame(ArrowTable, copycols = false)
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
