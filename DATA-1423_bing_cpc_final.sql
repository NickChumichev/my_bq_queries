WITH table_1 AS (
SELECT

  *
 FROM `funnel-flowwow.Analyt_ChumichevN.DATA-1452_bing_cpc_0801-0822`

 UNION ALL

SELECT
  *
 FROM `funnel-flowwow.Analyt_ChumichevN.DATA-1423_bing_cpc_0701-0731`
)
SELECT
  DATE_TRUNC(week, WEEK(MONDAY)) AS week1,
  *
  EXCEPT(week)
FROM table_1
ORDER BY DATE_TRUNC(week, WEEK(MONDAY)) DESC