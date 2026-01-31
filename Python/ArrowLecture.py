import adbc_driver_sqlite.dbapi as dbapi
import pyarrow as pa
#============================================================
def is_available(conn:dbapi.AdbcSqliteConnection, table:str) -> bool:

    name = table.upper()
    catalog = conn.adbc_get_objects().read_all().to_pylist()[0]
    tables = catalog["catalog_db_schemas"][0]["db_schema_tables"]
    names = [item["table_name"] for item in tables]

    return name in names
#============================================================
def get_ArrowTable(conn:dbapi.AdbcSqliteConnection, table:str) -> pa.Table:

    try:
        with conn.cursor() as cursor:

            if is_available(conn, table):

                name = table.upper()
                cursor.execute(f'SELECT * FROM {name}')
                return cursor.fetch_arrow_table()

            else:

                cursor.execute(table)
                return cursor.fetch_arrow_table()

    except Exception as e:

        print(f'CORRUPTED QUERY OR TABLE NOT AVAILABLE IN DATABASE: {e}')
