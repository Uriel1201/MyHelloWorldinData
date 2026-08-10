import adbc_driver_sqlite.dbapi as dbapi
import pyarrow as pa

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
          
            batches = cursor.execute(Query).fetch_record_batch()
            with pa.OSFile(f'{outputNameFile}.arrow', 'wb') as my_file:
                with pa.ipc.new_file(my_file, batches.schema) as writer:
                    for batch in batches:
                        writer.write_batch(batch)

    except Exception as e:

        print(f'Corrupted query or table not available: {e}')
