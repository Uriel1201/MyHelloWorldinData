/* DUCKDB. */
/********************************************************************/
WITH DUCKTABLE AS (
    SELECT
        USER_ID,
        ACTION,
        STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
    FROM
        sqlite_scan('MyDataBase.db', 'USERS_01')
SELECT 
    *
FROM
    DUCKTABLE;
