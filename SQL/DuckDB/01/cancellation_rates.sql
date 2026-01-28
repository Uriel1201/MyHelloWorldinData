/* 
01. Cancellation Rates.

From the following table of user IDs,
actions, and dates, write a query to
return the publication and cancellation
rate for each user. */

/* DUCKDB. */
/********************************************************************/
WITH
    DUCK_UPDATED AS (
        SELECT
            USER_ID,
            ACTION,
            STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
        FROM
            'USERS_01'),
    TOTALS AS (
        SELECT
            USER_ID,
            SUM(IF(ACTION = 'start',1,0)) AS TOTAL_STARTS,
            SUM(IF(ACTION = 'cancel',1,0)) AS TOTAL_CANCELS,
            SUM(IF(ACTION = 'publish',1,0)) AS TOTAL_PUBLISHES
        FROM
            DUCK_UPDATED
        GROUP BY
            USER_ID)
SELECT
    USER_ID,
    ROUND(TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS,
                                   0),
          2) AS PUBLISH_RATE,
    ROUND(TOTAL_CANCELS / NULLIF(TOTAL_STARTS,
                                 0),
          2) AS CANCEL_RATE
FROM
    TOTALS
ORDER BY
    1;
