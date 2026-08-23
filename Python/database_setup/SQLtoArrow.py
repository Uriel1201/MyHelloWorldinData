# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "adbc-driver-manager>=1.9.0",
#   "oracledb",
#   "pyarrow"
# ]
# ///
from adbc_driver_manager import dbapi
import oracledb as odb
import pyarrow as pa
import pyarrow.dataset as ds
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
def sqlite_to_arrow(query:str, outputNameFile:str) -> None:
    try:
        with (
            dbapi.connect(driver="sqlite",
                          db_kwargs={"uri":"file:/content/MyDataBase.db?mode=ro"},
                  ) as con,
            con.cursor() as cursor
        ):
            batches = cursor.execute(query).fetch_record_batch()
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
def oracledb_to_arrow(query:str, output:str) -> None:
    try:
        with (
            odb.connect(
                        user=ODB_USER,
                        password=ODB_PASSWORD,
                        dsn=ODB_DSN
                ) as conn
        ):
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
# arrow_to_mysql:
# params:
# ============================================================
def arrow_to_mysql(arrowFile:str, tableName:str) -> None:

    try:
        with (
            dbapi.connect(
                      driver="mysql",
                      db_kwargs = {
                          "uri": URI_MYSQL,}
                ) as con,
            con.cursor() as cursor
        ):
            cursor.execute("""
                SELECT 
                    COALESCE (MAX(EVENT_ID),0)
                FROM
                    USERS_01
                """)
            last_event=cursor.fetchone()[0]
            with pa.memory_map(arrowFile, 'rb') as source:
                with pa.ipc.open_file(source) as reader:
                    for i in range(reader.num_record_batches):
                        batch=reader.get_batch(i)
                        ids=pa.array(range(last_event+1, last_event+1+batch.num_rows), type=pa.int64())
                        last_event+=batch.num_rows
                        batch = batch.add_column(0,"EVENT_ID", ids)
                        cursor.adbc_ingest(tableName, batch, mode="append")
    except Exception as e:
        print(f'Corrupted query or table not available: {e}')

# ============================================================
# csv_to_postgresql:
# params:
# ============================================================
def csv_to_postgresql(path: str, tableName:str, exists:bool) -> None:
    dataset = ds.dataset(
        path,
        format="csv"
    )
    reader = dataset.scanner().to_reader()
    try:
        with (
            dbapi.connect(
                      driver="postgresql",
                      db_kwargs={"uri": URI_POSTGRESQL},
                  ) as conn,
            conn.cursor() as cursor,
        ):
            if exists:
                first=False
            else:
                first=True
            for batch in reader:
                cursor.adbc_ingest(tableName, batch, mode="create" if first else "append")
                first=False
            conn.commit()
    except Exception as e:
        print(f'{e}')
# ============================================================
# get_my_arrow_table:
# params:
# ============================================================
def get_my_arrow_table(ArrowFile:str) -> config.MyArrowTable:
    with pa.memory_map(ArrowFile, 'rb') as source:
        return config.MyArrowTable(table = pa.ipc.open_file(source).read_all(),
                                   alias = splitext(basename(ArrowFile))[0]
            
                      )
# ============================================================
def main():
    csv_to_postgresql(
        config.DATA_PATH,
        "USERS_01",
        exists=False
    )

if __name__ == "__main__":
    main()
