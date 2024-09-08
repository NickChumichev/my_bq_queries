WITH a AS (
SELECT
    event_date,
    -- traffic_source.name AS campaign,
    -- traffic_source.name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key="source") AS source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key="medium") AS medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key="campaign") AS campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "transaction_id") as transaction_id,
    -- ecommerce.transaction_id AS transaction_id 
FROM `firebase-flowwow.analytics_150948805.events_2023*`
WHERE 1=1
    AND event_date BETWEEN ('20230926') AND ('20231112')
    AND _TABLE_SUFFIX BETWEEN "0923" AND "1114"
    AND REGEXP_CONTAINS(LOWER(traffic_source.name),'corp')
    -- AND event_name = "ecommerce_purchase"
    -- AND  ecommerce.transaction_id IS NOT NULL
    -- AND ecommerce.transaction_id = "8547505"
)
SELECT
    event_date,
    source,
    medium,
    campaign,
    transaction_id
FROM a
WHERE 1=1
AND REGEXP_CONTAINS(LOWER(campaign),'corp') 