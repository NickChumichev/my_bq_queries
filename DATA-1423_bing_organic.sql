SELECT DISTINCT
  PARSE_DATE('%Y%m%d',event_date) AS day,
  traffic_source.source,
  traffic_source.medium,
  geo.city,
  geo.country,
  LOWER((SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params) WHERE key="ga_session_id")) AS ga4_sessions,
  user_pseudo_id AS ga4_users,
  CASE WHEN event_name IN ('first_visit','first_open') THEN user_pseudo_id ELSE NULL END AS ga4_new_users,
  CASE WHEN event_name IN ('click_pay') THEN 'click_pay' ELSE NULL END AS click_pay,
  CASE WHEN event_name IN ('begin_checkout') THEN 'begin_checkout' ELSE NULL END AS begin_checkout,
  CASE WHEN event_name IN ('add_to_cart') THEN 'add_to_cart' ELSE NULL END AS add_to_cart,
  (SELECT CAST(value.string_value AS STRING) FROM UNNEST(event_params) WHERE key="transaction_id") AS transaction_id
FROM `firebase-flowwow.analytics_150948805.events_2023*`
WHERE 1=1
    AND event_date BETWEEN ('20230501') AND ('20230531')
    AND _TABLE_SUFFIX BETWEEN "0501" AND "0531"
    -- AND event_name = 'click_pay'
    AND REGEXP_CONTAINS(LOWER(traffic_source.source),"^bing$")
    AND traffic_source.medium IN ('organic')
