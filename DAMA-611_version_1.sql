WITH a AS (
SELECT
  id AS user_id,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,"jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=") AS phone
FROM `funnel-flowwow.MYSQL_EXPORT.f_user`
)
, b AS ( --получить страны регистрации по номеру телефона
SELECT
  user_id,
  phone,
  CASE
    WHEN STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))THEN "Россия"
    WHEN STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77") THEN "Казахстан"
    WHEN STARTS_WITH(phone,"41") THEN "Швейцария"
    WHEN STARTS_WITH(phone,"62") THEN "Индонезия"
    WHEN STARTS_WITH(phone,"62") THEN "Асеньон"
    WHEN STARTS_WITH(phone,"672") THEN "Рождественсткие о-ва"
    WHEN STARTS_WITH(phone,"21") THEN "Тунис"
    WHEN STARTS_WITH(phone,"47") THEN "Норвегия"
    WHEN STARTS_WITH(phone,"351") THEN "Португалия"
    WHEN STARTS_WITH(phone,"39") THEN "Италия"
    WHEN STARTS_WITH(phone,"243") THEN "ДРК"
    WHEN STARTS_WITH(phone,"382") THEN "Черногория"
    WHEN STARTS_WITH(phone,"383") OR STARTS_WITH(phone,"990") THEN 'other_countries'
    ELSE es.TitleRU
  END AS country_by_phone, --страна регистрации пользователя  
FROM a LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.country_phone_codes` es ON STARTS_WITH(a.phone,es.PhoneCode)
)
, c AS ( --получить последний язык ОС и браузера для user_id
SELECT DISTINCT
  _user_id_ AS user_id, 
  LAST_VALUE(_language_) OVER (PARTITION BY _user_id_  ORDER BY DATE(_created_at_) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS language,
FROM `funnel-flowwow.ADJUST_RAW.clients_app` p
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2024-01-01") AND DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
-- AND _user_id_ = '2983815'
AND _language_ IS NOT NULL

UNION DISTINCT

SELECT DISTINCT 
  user_id,
  LAST_VALUE(REGEXP_EXTRACT(device.language, r'^([^-]*)')) OVER (PARTITION BY user_id  ORDER BY EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS language
FROM `firebase-flowwow.analytics_150948805.events_2024*` 
WHERE 1=1
AND device.language IS NOT NULL
AND _TABLE_SUFFIX BETWEEN "0101" AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
, d AS ( -- убрать дубликаты user_id
SELECT
  user_id,
  language
FROM c
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id) = 1
)
, e AS ( --разбить покупки по типам
SELECT DISTINCT 
  DATE(y.purchase_timestamp) AS date,
  b.phone,
  b.country_by_phone,
  y.country_from,
  y.city_from_name AS city_from, 
  y.country_to,
  y.city_name AS city_to,
  y.user_id,
  y.purchase_id,
  y.is_first_purchase,
  y.platform,
  d.language,
  y.category_name AS category,
  s.segment,
  CASE 
    WHEN y.country_to = b.country_by_phone AND y.country_to = y.country_from AND b.country_by_phone != 'Россия' THEN 'local_purchase' --локальная покупка
    WHEN y.country_to != b.country_by_phone AND b.country_by_phone != 'Россия' AND y.country_to != y.country_from AND (d.language IS NULL OR d.language != 'ru') THEN 'foreign_purchase' --иностранная покупка
    WHEN (d.language = 'ru' AND STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")))  
    OR (STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))) OR (d.language = 'ru')
    THEN 'russian_purchase' --рф покупка
    WHEN (d.language = 'ru' AND STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")) AND y.country_to = y.country_from)  
    OR (STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")) AND y.country_to = y.country_from) OR (d.language = 'ru' AND y.country_to = y.country_from)
    THEN 'russian_purchase_in_country_from' -- покупка россиянина в стране заказа
    ELSE 'other_purchase'
    END AS purchase_type
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y
LEFT JOIN b ON CAST(y.user_id AS STRING) = CAST(b.user_id AS STRING)
LEFT JOIN d ON CAST(y.user_id AS STRING) = CAST(d.user_id AS STRING)
LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s ON s.transactionid = y.purchase_id   
WHERE DATE(purchase_timestamp) BETWEEN '2024-01-01' AND CURRENT_DATE()-1 
AND y.country_to != 'Россия'
)
, f AS ( --покупки с категориями
SELECT
  EXTRACT(YEAR FROM DATE_TRUNC(date,MONTH)) AS year,
  EXTRACT(MONTH FROM DATE_TRUNC(date,MONTH)) AS month,
  country_by_phone,
  country_from,
  country_to,
  platform,
  category, --задвоение в 1%
  segment,
  COUNT(DISTINCT purchase_id) AS all_purchases,
  COUNT(DISTINCT IF(purchase_type = 'local_purchase', purchase_id, NULL)) AS local_purchases,
  COUNT(DISTINCT IF(purchase_type = 'foreign_purchase', purchase_id, NULL)) AS foreign_purchases,
  COUNT(DISTINCT IF(purchase_type = 'russian_purchase', purchase_id, NULL)) AS russian_purchases,
  COUNT(DISTINCT IF(purchase_type = 'other_purchase', purchase_id, NULL)) AS other_purchases,
  COUNT(DISTINCT IF(is_first_purchase = 0, purchase_id, NULL)) AS repeated_purchases,
  COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id, NULL)) AS first_purchases,
FROM e
GROUP BY 1,2,3,4,5,6,7,8
)
, g AS ( --покупки без категорий
SELECT
  EXTRACT(YEAR FROM DATE_TRUNC(date,MONTH)) AS year,
  EXTRACT(MONTH FROM DATE_TRUNC(date,MONTH)) AS month,
  country_by_phone,
  country_from,
  country_to,
  platform,
  segment,
  COUNT(DISTINCT purchase_id) AS all_purchases,
  COUNT(DISTINCT IF(purchase_type = 'local_purchase', purchase_id, NULL)) AS local_purchases,
  COUNT(DISTINCT IF(purchase_type = 'foreign_purchase', purchase_id, NULL)) AS foreign_purchases,
  COUNT(DISTINCT IF(purchase_type = 'russian_purchase', purchase_id, NULL)) AS russian_purchases,
  COUNT(DISTINCT IF(purchase_type = 'other_purchase', purchase_id, NULL)) AS other_purchases,
  COUNT(DISTINCT IF(is_first_purchase = 0, purchase_id, NULL)) AS repeated_purchases,
  COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id, NULL)) AS first_purchases,
FROM e
GROUP BY 1,2,3,4,5,6,7
)
SELECT
  f.year,
  f.month,
  f.country_by_phone,
  f.country_from,
  f.country_to,
  f.platform,
  f.category,
  f.segment,
  ROUND(g.all_purchases * SAFE_DIVIDE(f.all_purchases , SUM(f.all_purchases) OVER (PARTITION BY f.year, f.month, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment)),0) AS all_purchases,
  ROUND(g.local_purchases * SAFE_DIVIDE(f.local_purchases , SUM(f.local_purchases) OVER (PARTITION BY f.year, f.month, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment)),0) AS local_purchases,
  ROUND(g.foreign_purchases * SAFE_DIVIDE(f.foreign_purchases , SUM(f.foreign_purchases) OVER (PARTITION BY f.year, f.month, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment)),0) AS foreign_purchases,
  ROUND(g.russian_purchases * SAFE_DIVIDE(f.russian_purchases , SUM(f.russian_purchases) OVER (PARTITION BY f.year, f.month, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment)),0) AS russian_purchases,
  ROUND(g.other_purchases * SAFE_DIVIDE(f.other_purchases , SUM(f.other_purchases) OVER (PARTITION BY f.year, f.month, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment)),0) AS other_purchases,
  ROUND(g.repeated_purchases * SAFE_DIVIDE(f.repeated_purchases , SUM(f.repeated_purchases) OVER (PARTITION BY f.year, f.month, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment)),0) AS repeated_purchases,
  ROUND(g.first_purchases * SAFE_DIVIDE(f.first_purchases , SUM(f.first_purchases) OVER (PARTITION BY f.year, f.month, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment)),0) AS first_purchases,
FROM f
LEFT JOIN g USING(year, month, country_by_phone, country_from, country_to, platform, segment)