WITH table_1 AS ( 
SELECT DISTINCT
  FIRST_VALUE(date) OVER (PARTITION BY user_pseudo_id ORDER BY session_id ASC) as first_date,
  user_pseudo_id AS ga4_new_users,
FROM funnel-flowwow.BUSINESS_DM.cm_web_ga_4_sessions_attribution 
WHERE 1=1
  AND date BETWEEN '2023-08-01' AND '2023-09-03'
  AND REGEXP_CONTAINS(session_traffic_source_last_non_direct.medium, '.*in-image-standart.*|.*july23.*')
  AND REGEXP_CONTAINS(session_traffic_source_last_non_direct.source, '.*astralab.*')
  AND REGEXP_CONTAINS(session_traffic_source_last_non_direct.campaign, '1sent|in_image_max|in_image_max')
)
, table_2 AS (
SELECT 
  sa.date,
  COUNT(DISTINCT session_id) AS session_id,
  COUNT(DISTINCT sa.user_pseudo_id) AS ga4_users,
  COUNT(DISTINCT ga4_new_users) AS ga4_new_users,
  COUNT(DISTINCT purchase_id) AS cnt_purchases, 
  SUM(CASE WHEN paid = 1 THEN purchase_sum_rub END) AS purchase_sum_rub, --взять доход из crm_com
FROM funnel-flowwow.BUSINESS_DM.cm_web_ga_4_sessions_attribution sa
  LEFT JOIN  funnel-flowwow.BUSINESS_DM.cm_web_ga_4_transactions_attribution ta ON sa.user_pseudo_id = ta.user_pseudo_id AND sa.session_id = ta.attributed_session_id
  LEFT JOIN  funnel-flowwow.CRM_DM_PRTND.crm_com crm ON CAST(ta.transaction_id AS STRING) = CAST(crm.purchase_id AS STRING)
  LEFT JOIN  table_1 t1 ON t1.first_date = sa.date AND t1.ga4_new_users=sa.user_pseudo_id
WHERE 1 = 1
  AND REGEXP_CONTAINS(session_traffic_source_last_non_direct.medium, '.*in-image-standart.*|.*july23.*')
  AND REGEXP_CONTAINS(session_traffic_source_last_non_direct.source, '.*astralab.*')
  AND REGEXP_CONTAINS(session_traffic_source_last_non_direct.campaign, '1sent|in_image_max|in_image_max')
  AND sa.date between '2023-08-01' AND '2023-09-03'
GROUP BY
  sa.date
)
-- , table_3 AS (
SELECT
  DATE(_created_at_) AS date, -- взял дату создания _activity_kind_
  COUNT(CASE WHEN _activity_kind_ = 'session' THEN _created_at_ END) AS adj_session, --чекнуть как считается
  COUNT(DISTINCT _adid_) AS adj_users,
  SUM(IF(_event_name_ = 'first_open',1,0)) AS adj_new_users,
  SUM(IF(_activity_kind_ ='install',1,0)) AS adj_installs,
  SUM(IF(_activity_kind_ ='reattribution',1,0)) AS adj_reatrib,
  COUNT(purchase_id) AS crm_cnt_purchases, --проверить не задваиваются ли покупки
  COUNT (CASE WHEN _event_name_ = "s2s_ecommerce_purchase_paid" THEN _purchase_id_ END) AS adj_cnt_purchases, --проверить не задваиваются ли покупки
  SUM(CASE WHEN paid = 1 THEN purchase_sum_rub END) AS adj_purchase_sum_rub, --взять доход из crm_com
  SUM (CASE WHEN _event_name_ = "s2s_ecommerce_purchase_paid" THEN _revenue__201 END) AS adj_revenue --почему доход сильно расходится
FROM `funnel-flowwow.ADJUST_RAW.clients_app` aj
LEFT JOIN  funnel-flowwow.CRM_DM_PRTND.crm_com crm ON CAST(aj._purchase_id_ AS STRING) = CAST(crm.purchase_id AS STRING)
WHERE 1=1
  -- AND REGEXP_CONTAINS(_campaign_name_, '.*astralab.*|.*july23.*')
  -- AND REGEXP_CONTAINS(_adgroup_name_, '.*sent.*|.*july23.*|.*in_image_max.*')
  -- AND REGEXP_CONTAINS(_creative_name_, '.*in_image_standart.*|.*in_image_max.*')
  AND TIMESTAMP_TRUNC(_click_time_, DAY) BETWEEN '2023-08-28' AND '2023-09-03'
  AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-08-01") AND TIMESTAMP("2023-09-03")
  GROUP BY 1
-- )