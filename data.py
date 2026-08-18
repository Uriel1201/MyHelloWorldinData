# /// script
# dependencies = [
#   "polars"
# ]
# ///
import polars 
data = {"a": [1, 2], "b": [3, 4]}
df = pl.DataFrame(data)
df
