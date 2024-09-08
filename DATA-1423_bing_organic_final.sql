WITH ga4_organic_transaction_id AS (
SELECT

  *
 FROM `funnel-flowwow.Analyt_ChumichevN.DATA-1423_bing_organic_0801-0822`

 UNION ALL

SELECT
  *
 FROM `funnel-flowwow.Analyt_ChumichevN.DATA-1423_bing_organic_0701-0731`

  UNION ALL

SELECT
  *
 FROM `funnel-flowwow.Analyt_ChumichevN.DATA-1423_bing_organic_0601-0630`

 UNION ALL

SELECT
  *
 FROM `funnel-flowwow.Analyt_ChumichevN.DATA-1423_bing_organic_0501-0531`

)
SELECT
  DATE_TRUNC(g4.day, WEEK(MONDAY)) AS week,
  g4.source,
  g4.medium,
  g4.city,
  crm.city_name AS city_of_purchase,
  g4.country,
  COUNT(g4.ga4_sessions) AS ga4_sessions,
  COUNT(DISTINCT g4.ga4_users) AS ga4_users,
  COUNT(g4.ga4_new_users) AS ga4_new_users,
COUNT(DISTINCT g4.click_pay) AS click_pay,
COUNT(DISTINCT g4.add_to_cart) AS add_to_cart,
-- COUNT(g4.transaction_id) AS transaction_id,
COUNT(crm.purchase_id) AS purchase_id,
SUM(crm.purchase_sum_rub) AS purchase_sum_rub
FROM ga4_organic_transaction_id g4
  LEFT JOIN  funnel-flowwow.CRM_DM_PRTND.crm_com crm ON CAST(g4.transaction_id AS STRING) = CAST(crm.purchase_id AS STRING)
  GROUP BY 1,2,3,4,5,6










-- SELECT
--   DATE_TRUNC(week, WEEK(MONDAY)) AS week1,
--   *
--   EXCEPT(week)
-- FROM table_1
-- ORDER BY DATE_TRUNC(week, WEEK(MONDAY)) DESC