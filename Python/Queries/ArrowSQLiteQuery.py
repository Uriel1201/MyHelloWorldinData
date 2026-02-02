import adbc_driver_sqlite.dbapi as dbapi
import MetaQuery 
import pyarrow as pa

# ============================================================
# is_available:
# params:
# ============================================================
def is_available(conn:dbapi.AdbcSqliteConnection, table:str) -> bool:

    name = table.upper()
    catalog = conn.adbc_get_objects().read_all().to_pylist()[0]
    tables = catalog["catalog_db_schemas"][0]["db_schema_tables"]
    names = [item["table_name"] for item in tables]

    return name in names

# ============================================================
# get_ArrowTable:
# params:
# ============================================================
def get_ArrowTable(conn:dbapi.AdbcSqliteConnection, QueryFile:str) -> pa.Table:

    try:
        with conn.cursor() as cursor:

            query = MetaQuery.get_Query(QueryFile)
            table = MetaQuery.get_TableName(query)

            if is_available(conn, table):

                cursor.execute(query)
                return cursor.fetch_arrow_table()

    except Exception as e:

        print(f'Corrupted query or table not available: {e}')
