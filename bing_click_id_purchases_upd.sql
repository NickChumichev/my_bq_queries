WITH table_1 AS (
SELECT 
  DATE_TRUNC(PARSE_DATE('%Y%m%d',event_date), WEEK(MONDAY)) AS week,
  traffic_source.source,
  -- traffic_source.name AS campaign,
  SUM(IF(event_name = 'click_pay',1,0)) as click_pay
FROM `firebase-flowwow.analytics_150948805.events_2023*`
WHERE 1=1
    AND event_date BETWEEN ('20230601') AND ('20230822')
    AND _TABLE_SUFFIX BETWEEN "0601" AND "0822"
    AND event_name = 'click_pay'
    AND REGEXP_CONTAINS(LOWER(traffic_source.source),"^bing$")
GROUP BY 1,2
)
,table_2 AS (
SELECT -- получить расход, кол-во транзакций, доход в разрезе кампаний и дней
  SPLIT(CAST(DATE_TRUNC(crm.purchase_date, WEEK(MONDAY)) AS STRING), ' ')[OFFSET(0)] AS week,
  sa.session_traffic_source_last_non_direct.source AS source,
  sa.session_traffic_source_last_non_direct.campaign AS campaign,
  COUNT(crm.purchase_id) AS cnt_purchases,
  SUM(crm.purchase_sum_rub) AS sum_purchases
FROM funnel-flowwow.BUSINESS_DM.cm_web_ga_4_sessions_attribution sa
  INNER JOIN  funnel-flowwow.BUSINESS_DM.cm_web_ga_4_transactions_attribution ta ON sa.user_pseudo_id = ta.user_pseudo_id AND sa.session_id = ta.attributed_session_id
  LEFT JOIN  funnel-flowwow.CRM_DM_PRTND.crm_com crm ON CAST(ta.transaction_id AS STRING) = CAST(crm.purchase_id AS STRING)
WHERE 1 = 1
  AND REGEXP_CONTAINS(LOWER(sa.session_traffic_source_last_non_direct.source),"^bing$")
  -- AND session_traffic_source_last_non_direct.medium IN ('cpc')
  AND sa.date between '2023-07-01' AND '2023-08-22'
GROUP BY
  SPLIT(CAST(DATE_TRUNC(crm.purchase_date, WEEK(MONDAY)) AS STRING), ' ')[OFFSET(0)],
  source,
  campaign
)
SELECT
t2.week,
t2.source,
t2.campaign,
t2.cnt_purchases,
t2.sum_purchases,
t1.campaign,
t1.click_pay
FROM table_1 t1 RIGHT JOIN table_2 t2 ON CAST(t1.week AS STRING) = CAST(t2.week AS STRING)
