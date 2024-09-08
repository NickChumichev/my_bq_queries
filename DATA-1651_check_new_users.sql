WITH table_1 AS (
SELECT 
    PARSE_DATE('%Y%m%d',event_date) AS date,
    user_pseudo_id AS ga4_users,
    LOWER((SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params) WHERE key="ga_session_id")) AS ga4_sessions,
    -- FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',TIMESTAMP_MICROS(event_timestamp)) AS event_date, 
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',TIMESTAMP_MICROS(user_first_touch_timestamp)) AS first_touch_time,
    IF(event_name = 'first_visit',1,0) AS ga4_new_users
FROM `firebase-flowwow.analytics_150948805.events_2023*`
WHERE 1=1
    AND event_date BETWEEN ('20230801') AND ('20230815')
    AND _TABLE_SUFFIX BETWEEN "0801" AND "0815"
    -- AND event_name IN ('purchase')
    AND REGEXP_CONTAINS(traffic_source.medium, '.*in-image-standart.*|.*july23.*')
    AND REGEXP_CONTAINS(traffic_source.source, '.*astralab.*')
    AND REGEXP_CONTAINS(collected_traffic_source.manual_campaign_name, '.*in_image_max.*|.*in-image-standart.*')
    -- GROUP BY user_pseudo_id,event_name,FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',TIMESTAMP_MICROS(user_first_touch_timestamp))
    -- GROUP BY 1,3,4,5
)
,table_2 AS (
    SELECT
    date, 
    ga4_users,
    ARRAY_AGG(ga4_sessions) AS arr_ga4_sessions,
    -- ARRAY_AGG(event_date) AS arr_event_date, 
    first_touch_time,
    ga4_new_users
    FROM table_1
    GROUP BY date,ga4_users,first_touch_time,ga4_new_users
)
SELECT DISTINCT
    date,
    count(distinct ga4_users) as ga4_users,
    count(arr_ga4_sessions[SAFE_ORDINAL(1)]) AS ga4_first_session,
    -- arr_event_date[SAFE_ORDINAL(2)] AS first_event_date,
    count(first_touch_time) AS first_touch_time,
    count(ga4_new_users) AS ga4_new_users
    FROM table_2
    GROUP BY date