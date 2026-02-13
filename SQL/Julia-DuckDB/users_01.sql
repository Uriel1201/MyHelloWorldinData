/* DUCKDB. */
/********************************************************************/
WITH DUCKTABLE AS (
    SELECT
        USER_ID,
        ACTION,
        STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
    FROM
        'USERS_01')
SELECT 
    *
FROM
    DUCKTABLE;
