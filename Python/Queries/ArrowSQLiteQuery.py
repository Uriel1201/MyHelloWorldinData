import adbc_driver_sqlite.dbapi as dbapi
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

        print(f'Corrupted query or table not available: {e}')
    #****************************************************************
    #*  get_query:
    #** params:
    #****************************************************************
    function get_query(filename::AbstractString)::String
        
        if !isfile(filename)
            throw(ArgumentError("'$filename' does not exist"))
        end

        query = open(filename) do file
            read(file, String)
        end

        return query
    
    end
    #****************************************************************
    
    #****************************************************************
    #*  get_table_name:
    #** params:
    #****************************************************************
    function get_table_name(query::String)::String

        m = match(r"FROM\s+'(\w+)'", query).captures[1]
    
        if m === nothing
            throw(ArgumentError("""Unable to locate the 
                                   table name in your SQL query 
                                """
                  )
            )
        end
    
        return m

    end
