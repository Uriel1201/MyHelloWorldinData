import duckdb
import config
import adbc_driver_sqlite.dbapi as dbapi
import SQLtoArrow as sa


# ============================================================
# print_duck_query:
# params:
# ============================================================
def print_duck_query(ArrowTable: config.MyArrowTable, DuckQuery: str) -> None:

    duckdb.register(ArrowTable.alias, ArrowTable.table)
    duckdb.sql(sa.get_query(DuckQuery)).show()


# ============================================================
# print_sqlite_query:
# params:
# ============================================================
def print_sqlite_query(conn: dbapi.AdbcSqliteConnection, SQLiteQuery: str) -> None:

    query = sa.get_query(SQLiteQuery)
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_options(
            **{
                "adbc.sqlite.query.batch_rows": 1,
            }
        )
        batches = cursor.execute(query).fetch_record_batch()
        for batch in batches:
            print(f"**********\n{batch}")
