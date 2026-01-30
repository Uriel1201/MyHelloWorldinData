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
          1;
