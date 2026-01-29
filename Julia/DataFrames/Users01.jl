module Users01

    using DataFrames, Arrow

    export get_CancellationRates

    #****************************************************************
    #*  get_CancellationRates:
    #** params:
    #****************************************************************
    function get_CancellationRates(ArrowFilename::AbstractString)::DataFrame

        if !isfile(ArrowFilename)
            throw(ArgumentError("'$ArrowFilename' does not exist"))
        end
   
        arrow_table = Arrow.Table(ArrowFilename)
        df = DataFrame(arrow_table, copycols = false)
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
