/* ORACLE 23ai. */
/********************************************************************/

SELECT
    ITS.PROBLEMATIC_USER
FROM
    USERS_01 MATCH_RECOGNIZE (
        PARTITION BY USER_ID
        ORDER BY
            DATES
        MEASURES
            USER_ID AS PROBLEMATIC_USER
        ONE ROW PER MATCH
    PATTERN ( A B C D ) DEFINE
        A AS A.ACTION = 'start',
        B AS B.ACTION = 'cancel',
        C AS C.ACTION = 'start',
        D AS D.ACTION = 'cancel'
    ) ITS;
