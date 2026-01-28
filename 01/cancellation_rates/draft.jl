using DuckDB
using DBInterface

function ejecutar_consulta(sql_file, arrow_file)
    # Leer el archivo SQL
    sql = read(sql_file, String)

    # Extraer el nombre de la tabla
    tabla_nombre = match(r"FROM (\w+)", sql).captures[1]

    # Conectar a DuckDB
    conn = DBInterface.connect(DuckDB.DB, ":memory:")

    # Registrar la tabla
    DBInterface.execute(conn, "REGISTER '$arrow_file' AS $tabla_nombre")

    # Ejecutar la consulta
    resultado = DBInterface.execute(conn, sql)

    # Devolver el resultado
    return resultado
end
