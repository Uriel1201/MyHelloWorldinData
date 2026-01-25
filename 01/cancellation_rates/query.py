import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import arrowkit

"""
01. Cancellation Rates.
**********************************************

Write a query to return the publication and cancellation
rate for each user.
"""

def main(table:str):

    conn = dbapi.connect("file:/content/MyDataBase.db?mode=ro")
    name = table.upper()
    if (name == "USERS_01"):

        try:

            arrow_users = arrowkit.get_ArrowTable(conn, table)
            print(f'Loading "USERS_01" table from disk using Arrow format:\n{arrow_users.schema}')
            users = pol.from_arrow(arrow_users)
            print(f'\nQuerying table using Polars (sample)\n{users.head(5)}')
            rates = (users.to_dummies(columns = 'ACTION')
                          .drop('DATES')
                          .group_by('USER_ID')
                          .agg(pol.col('*').sum())
                          .with_columns(publish_rate = pol.col('ACTION_publish') / pol.col('ACTION_start'),
                                        cancel_rate = pol.col('ACTION_cancel') / pol.col('ACTION_start')
                           )
            )

            print(f'\n**Polars**\nQuerying publication and cancellation rates for each user:\n{rates}')
            query = """
                    WITH
                        TOTALS AS (
                            SELECT
                                USER_ID,
                                1.0 * SUM(CASE WHEN ACTION = "start" THEN 1 ELSE 0 END) AS TOTAL_STARTS,
                                1.0 * SUM(CASE WHEN ACTION = "cancel" THEN 1 ELSE 0 END) AS TOTAL_CANCELS,
                                1.0 * SUM(CASE WHEN ACTION = "publish" THEN 1 ELSE 0 END) AS TOTAL_PUBLISHES
                            FROM
                                USERS_01
                            GROUP BY
                                USER_ID)
            SELECT
                USER_ID,
                TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS, 0) AS PUBLISH_RATE,
                TOTAL_CANCELS / NULLIF(TOTAL_STARTS, 0) AS CANCEL_RATE
            FROM
                TOTALS
            ORDER BY
                1
            """
            result = arrowkit.get_ArrowTable(conn, query)
            print(f'\n**SQL Query using ADBC Driver**\nQuerying publication and cancellation rates for each user:\n{result}')

        finally:

            conn.close()

    else:

        print(f'table {table} not available')
