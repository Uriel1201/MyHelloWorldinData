import duckdb
import config

# ============================================================
# print_duck_query:
# params:
# ============================================================
def print_duck_query(ArrowTable: config.MyArrowTable, DuckQuery: str) -> None:

    duckdb.register(ArrowTable.alias, ArrowTable.table)
    duckdb.sql(sa.get_query(DuckQuery)).show()
