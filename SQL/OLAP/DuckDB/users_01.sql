/* DUCKDB. */
/********************************************************************/
WITH DUCKTABLE AS (
    SELECT
        USER_ID,
        ACTION,
        STRPTIME(DATES, '%Y-%m-%d')
    FROM
        sqlite_scan('MyDataBase.db', 'USERS_01')
    )
SELECT 
    *
FROM
    DUCKTABLE;
