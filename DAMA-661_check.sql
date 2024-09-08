--проверка purchaser_first_year РФ
SELECT
  category_name,
  COUNT(DISTINCT IF(y.is_first_purchase = 1, y.purchase_id, NULL)) AS first_purchases,
  COUNT(DISTINCT y.purchase_id) AS all_purchases,
  COUNT(DISTINCT IF (y.country_to = r.country_by_phone AND y.country_to = y.country_from AND r.country_by_phone != 'Россия', y.purchase_id, NULL)) AS local_purchase,
  COUNT(DISTINCT IF (y.country_to != r.country_by_phone AND r.country_by_phone != 'Россия' AND l.language != 'ru' AND y.country_to != y.country_from, purchase_id, NULL)) AS foreign_purchase,
  COUNT(DISTINCT IF (l.language = 'ru' AND STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")) OR (STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))),purchase_id,NULL)) AS russian_purchase 
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y

LEFT JOIN (
  WITH a AS (
SELECT
  id AS user_id,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,"jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=") AS phone
FROM `funnel-flowwow.MYSQL_EXPORT.f_user`
  )
SELECT 
  a.user_id,
  a.phone,
  CASE
    WHEN STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))THEN "Россия"
  ELSE  es.TitleRU
  END AS country_by_phone
FROM a
LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.country_phone_codes` es ON STARTS_WITH(a.phone,es.PhoneCode)) r ON y.user_id = r.user_id

LEFT JOIN (SELECT DISTINCT
  _user_id_ AS user_id, 
  LAST_VALUE(_language_) OVER (PARTITION BY _user_id_  ORDER BY DATE(_created_at_) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS language,
  FROM `funnel-flowwow.ADJUST_RAW.clients_app` p
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2024-05-01") AND DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
AND _language_ IS NOT NULL

UNION DISTINCT

SELECT DISTINCT 
  user_id,
  LAST_VALUE(REGEXP_EXTRACT(device.language, r'^([^-]*)')) OVER (PARTITION BY user_id  ORDER BY EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS language
FROM `firebase-flowwow.analytics_150948805.events_2024*` 
WHERE 1=1
AND device.language IS NOT NULL
AND _TABLE_SUFFIX BETWEEN "0501" AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) l ON CAST(l.user_id AS STRING) = CAST(y.user_id AS STRING)

LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s ON s.transactionid = y.purchase_id 

WHERE DATE(y.purchase_timestamp) BETWEEN '2024-05-01' AND '2024-05-26'
AND y.country_from = 'Казахстан'
AND country_by_phone = 'Казахстан'
AND y.platform = 'ios app'
AND segment = 'aso_organic'
GROUP BY 1