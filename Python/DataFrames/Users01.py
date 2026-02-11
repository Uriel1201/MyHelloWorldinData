import polars as pl
import pyarrow as pa

# ============================================================
# get_CancellationRates:
# params:
# ============================================================
def get_CancellationRates(table:pa.Table) -> pl.DataFrame:

    pl_rates = (pl.from_arrow(table)
                  .lazy()
                  .group_by('USER_ID')
                  .agg([pl.col('ACTION').filter(pl.col('ACTION')=='start').count().alias('STARTS'),
                        pl.col('ACTION').filter(pl.col('ACTION')=='cancel').count().alias('CANCELS'),
                        pl.col('ACTION').filter(pl.col('ACTION')=='publish').count().alias('PUBLISHES')
                       ]
                   )
                  .select(pl.col('USER_ID'),
                          pl.when(pl.col('STARTS') > 0)
                            .then(pl.col('CANCELS') / pl.col('STARTS'))
                            .otherwise(None)
                            .alias('CANCEL_RATE'),
                          pl.when(pl.col('STARTS') > 0)
                            .then(pl.col('PUBLISHES') / pl.col('STARTS'))
                            .otherwise(None)
                            .alias('PUBLISH_RATE')
                   )
               )

    return pl_rates.collect()
