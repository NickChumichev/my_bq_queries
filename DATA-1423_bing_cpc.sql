SELECT
  DATE_TRUNC(PARSE_DATE('%Y%m%d',event_date), WEEK(MONDAY)) AS week,
  traffic_source.source,
  traffic_source.medium,
  -- traffic_source.name AS campaign,
  geo.city,
  geo.country,
  COUNT(LOWER((SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params) WHERE key="ga_session_id"))) AS ga4_sessions,
  COUNT(DISTINCT user_pseudo_id) AS ga4_users,
  COUNT(DISTINCT CASE WHEN event_name IN ('first_visit','first_open') THEN user_pseudo_id ELSE NULL END) AS ga4_new_users,
  COUNT(DISTINCT CASE WHEN event_name IN ('click_pay') THEN 'click_pay' ELSE NULL END) AS click_pay,
  COUNT(DISTINCT CASE WHEN event_name IN ('begin_checkout') THEN 'begin_checkout' ELSE NULL END) AS begin_checkout,
  COUNT(DISTINCT CASE WHEN event_name IN ('add_to_cart') THEN 'add_to_cart' ELSE NULL END) AS add_to_cart
FROM `firebase-flowwow.analytics_150948805.events_2023*`
WHERE 1=1
    AND event_date BETWEEN ('20230801') AND ('20230822')
    AND _TABLE_SUFFIX BETWEEN "0801" AND "0822"
    -- AND event_name = 'click_pay'
    AND REGEXP_CONTAINS(LOWER(traffic_source.source),"^bing$")
    AND traffic_source.medium IN ('cpc')
GROUP BY 1,2,3,4,5

UNION ALL

SELECT
  DATE_TRUNC(PARSE_DATE('%Y%m%d',event_date), WEEK(MONDAY)) AS week,
  traffic_source.source,
  traffic_source.medium,
  -- traffic_source.name AS campaign,
  geo.city,
  geo.country,
  COUNT(LOWER((SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params) WHERE key="ga_session_id"))) AS ga4_sessions,
  COUNT(DISTINCT user_pseudo_id) AS ga4_users,
  COUNT(DISTINCT CASE WHEN event_name IN ('first_visit','first_open') THEN user_pseudo_id ELSE NULL END) AS ga4_new_users,
  COUNT(DISTINCT CASE WHEN event_name IN ('click_pay') THEN 'click_pay' ELSE NULL END) AS click_pay,
  COUNT(DISTINCT CASE WHEN event_name IN ('begin_checkout') THEN 'begin_checkout' ELSE NULL END) AS begin_checkout,
  COUNT(DISTINCT CASE WHEN event_name IN ('add_to_cart') THEN 'add_to_cart' ELSE NULL END) AS add_to_cart
FROM `firebase-flowwow.analytics_150948805.events_2023*`
WHERE 1=1
    AND event_date BETWEEN ('20230701') AND ('20230731')
    AND _TABLE_SUFFIX BETWEEN "0701" AND "0731"
    -- AND event_name = 'click_pay'
    AND REGEXP_CONTAINS(LOWER(traffic_source.source),"^bing$")
    AND traffic_source.medium IN ('cpc')
GROUP BY 1,2,3,4,5

UNION ALL

SELECT
  DATE_TRUNC(PARSE_DATE('%Y%m%d',event_date), WEEK(MONDAY)) AS week,
  traffic_source.source,
  traffic_source.medium,
  -- traffic_source.name AS campaign,
  geo.city,
  geo.country,
  COUNT(LOWER((SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params) WHERE key="ga_session_id"))) AS ga4_sessions,
  COUNT(DISTINCT user_pseudo_id) AS ga4_users,
  COUNT(DISTINCT CASE WHEN event_name IN ('first_visit','first_open') THEN user_pseudo_id ELSE NULL END) AS ga4_new_users,
  COUNT(DISTINCT CASE WHEN event_name IN ('click_pay') THEN 'click_pay' ELSE NULL END) AS click_pay,
  COUNT(DISTINCT CASE WHEN event_name IN ('begin_checkout') THEN 'begin_checkout' ELSE NULL END) AS begin_checkout,
  COUNT(DISTINCT CASE WHEN event_name IN ('add_to_cart') THEN 'add_to_cart' ELSE NULL END) AS add_to_cart
FROM `firebase-flowwow.analytics_150948805.events_2023*`
WHERE 1=1
    AND event_date BETWEEN ('20230601') AND ('20230630')
    AND _TABLE_SUFFIX BETWEEN "0601" AND "0630"
    -- AND event_name = 'click_pay'
    AND REGEXP_CONTAINS(LOWER(traffic_source.source),"^bing$")
    AND traffic_source.medium IN ('cpc')
GROUP BY 1,2,3,4,5