WITH table_1 AS ( 
SELECT DISTINCT
    PARSE_DATE('%Y%m%d',event_date) AS date,
    LOWER((SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params) WHERE key="ga_session_id")) AS ga4_sessions,
    user_pseudo_id AS ga4_users,
    -- FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',TIMESTAMP_MICROS(user_first_touch_timestamp)) AS first_touch_time,
    LOWER((SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params) WHERE key="ga_session_number" AND value.int_value = 1)) AS ga4_new_users,
    IF(event_name = 'purchase' AND event_name = 'refund',1,0) AS ga4_purchases, 
    (SELECT CAST(value.string_value AS FLOAT64) FROM UNNEST(event_params) WHERE key="revenue") AS ga4_revenue
FROM `firebase-flowwow.analytics_150948805.events_2023*`
WHERE 1=1
    AND event_date BETWEEN ('20230801') AND ('20230815')
    AND _TABLE_SUFFIX BETWEEN "0801" AND "0815"
    -- AND event_name IN ('purchase')
    AND REGEXP_CONTAINS(traffic_source.medium, '.*in-image-standart.*|.*july23.*')
    AND REGEXP_CONTAINS(traffic_source.source, '.*astralab.*')
    AND REGEXP_CONTAINS(collected_traffic_source.manual_campaign_name, '.*in_image_max.*|.*in-image-standart.*')
    -- GROUP BY 1  
)
,table_2 AS (
SELECT
    date,
    COUNT(ga4_sessions) AS cnt_ga4_sessions,
    COUNT(ga4_users) AS cnt_ga4_users,
    -- COUNT(first_touch_time) AS cnt_first_touch_time,
    COUNT(ga4_new_users) AS cnt_ga4_new_users,
    SUM(ga4_purchases) AS ga4_cnt_purchases, 
    SUM(ga4_revenue) AS ga4_revenue
FROM table_1 
GROUP BY date
)
,table_3 AS (
SELECT
  DATE(_click_time_) AS date,
  COUNT(_lifetime_session_count_) AS adj_session,
  COUNT(DISTINCT _adid_) AS adj_users,
  SUM(IF(_event_name_ = 'first_open',1,0)) AS adj_new_users,
  SUM(IF(_activity_kind_ ='install',1,0)) AS adj_installs,
  COUNT (DISTINCT CASE WHEN _event_name_ = "s2s_ecommerce_purchase_paid" AND _event_name_ = 'cancelled_order' THEN _purchase_id_ END) AS adj_cnt_purchases,
  -- COUNT( DISTINCT _purchase_id_) AS adj_purchases_2,
  SUM(_revenue__201) AS adj_revenue
FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
WHERE 1=1
  AND REGEXP_CONTAINS(_campaign_name_, '.*astralab.*')
  AND REGEXP_CONTAINS(_adgroup_name_, '.*sent.*|.*july23.*')
  AND REGEXP_CONTAINS(_creative_name_, 'in_image_standart|n_image_max')
  AND TIMESTAMP_TRUNC(_click_time_, DAY) BETWEEN '2023-08-01' AND '2023-08-15'
  AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-08-01") AND TIMESTAMP("2023-08-15")
  -- AND _event_name_ = 's2s_ecommerce_purchase_paid'
  GROUP BY 1)
 ,table_4 AS (   
SELECT
    t2.date,
    t2.cnt_ga4_sessions,
    t2.cnt_ga4_users,
    t2.cnt_ga4_new_users,
    -- t2.cnt_first_touch_time,
    t2.ga4_cnt_purchases, 
    t2.ga4_revenue,
    t3.adj_session,
    t3.adj_users,
    t3.adj_new_users,
    t3.adj_installs,
    t3.adj_cnt_purchases,
    t3.adj_revenue
    FROM table_2 t2 LEFT JOIN table_3 t3 ON  t2.date = t3.date
    ORDER BY date DESC
 )
 SELECT
 date,
 (IFNULL(cnt_ga4_sessions, 0) + IFNULL(adj_session,0)) AS sessions,
 (IFNULL(cnt_ga4_users,0) + IFNULL(adj_users,0)) AS users,
  -- cnt_first_touch_time,  
 (IFNULL(cnt_ga4_new_users,0) + IFNULL(adj_new_users,0)) AS new_users,
 (IFNULL(adj_installs,0)) AS adj_installs,
 (IFNULL(ga4_cnt_purchases,0) + IFNULL(adj_cnt_purchases,0)) AS purchases,
 (IFNULL(ga4_revenue,0) + IFNULL(adj_revenue,0)) AS revenue
 FROM table_4 
 ORDER BY date DESC
