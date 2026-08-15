import adbc_driver_sqlite.dbapi as dbapi
import oracledb as odb
import pyarrow as pa
import config 

# ============================================================
# get_query:
# params:
# ============================================================
def get_query(filename: str) -> str:

    try:
        
        with open(filename, 'r', encoding='utf-8') as file:
            
            return file.read()
            
    except FileNotFoundError:
        
        return f"Error:'{filename}' does not exist in directory."

# ============================================================
# sqlite_to_arrow:
# params:
# ============================================================
def sqlite_to_arrow(Query:str, outputNameFile:str) -> None:

    try:
        with dbapi.connect("file:/content/MyDataBase.db?mode=ro").cursor() as cursor:
          
            cursor.adbc_statement.set_options(
                **{
            "adbc.sqlite.query.batch_rows": 1,
                }
            )
            batches = cursor.execute(Query).fetch_record_batch()
            with pa.OSFile(f'{outputNameFile}.arrow', 'wb') as my_file:
                with pa.ipc.new_file(my_file, batches.schema) as writer:
                    for batch in batches:
                        writer.write_batch(batch)

    except Exception as e:

        print(f'Corrupted query or table not available: {e}')
        
# ============================================================
# oracledb_to_arrow:
# params:
# ============================================================
def oracledb_to_arrow(conn:odb.Connection, query: str, output: str) -> None:
    try:
        odf = conn.fetch_df_batches(statement = query, size = 10000)
        first_df = next(odf)    
        batch = pa.RecordBatch.from_arrays(first_df.column_arrays(), names = first_df.column_names())
        with pa.OSFile(f'{output}.arrow', 'wb') as my_file:
            with pa.ipc.new_file(my_file, batch.schema) as writer:
                writer.write(batch)
                for df in odf:
                    batches = pa.RecordBatch.from_arrays(df.column_arrays(), names = df.column_names())
                    writer.write_batch(batches)
    except Exception as e:
        print(f'Corrupted query or table not available: {e}')

# ============================================================
# get_my_arrow_table:
# params:
# ============================================================
def get_my_arrow_table(ArrowFile:str) -> config.MyArrowTable:

    with pa.memory_map(ArrowFile, 'rb') as source:

        return config.MyArrowTable(table = pa.ipc.open_file(source).read_all(),
                                   alias = splitext(basename(ArrowFile))[0]
                      )
