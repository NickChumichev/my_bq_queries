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
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2024-05-01") AND TIMESTAMP(CURRENT_DATE()-1)
AND _language_ IS NOT NULL

UNION DISTINCT

SELECT DISTINCT 
  user_id,
  LAST_VALUE(REGEXP_EXTRACT(device.language, r'^([^-]*)')) OVER (PARTITION BY user_id  ORDER BY EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS language
FROM `firebase-flowwow.analytics_150948805.events_2024*` 
WHERE 1=1
AND device.language IS NOT NULL
AND _TABLE_SUFFIX >= "0501" 
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
  CASE
    WHEN y.is_first_purchase =  0 THEN 1 --признак повторной покупки
    WHEN y.is_first_purchase =  1 THEN 0 -- признак первой покупки
  END AS repeated_first_purchase,
  s.segment, 
  IF(y.country_to = b.country_by_phone AND y.country_to = y.country_from AND b.country_by_phone != 'Россия'AND d.language != 'ru', purchase_id, NULL) AS local_purchase, --локальная покупка
  IF(y.country_to != b.country_by_phone AND b.country_by_phone != 'Россия' AND y.country_to != y.country_from AND (d.language IS NULL OR d.language != 'ru'), purchase_id, NULL) AS foreign_purchase, --иностранная покупка
  IF((d.language = 'ru' AND STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")))  
    OR (STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))) OR (d.language = 'ru'), purchase_id, NULL) AS russian_purchase, --рф покупка
  IF((d.language = 'ru' AND STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")) AND y.country_to = y.country_from)  
    OR (STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")) AND y.country_to = y.country_from) OR (d.language = 'ru' AND y.country_to = y.country_from), purchase_id, NULL) AS russian_purchase_in_country_from, -- покупка россиянина в стране заказа
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y
LEFT JOIN b ON CAST(y.user_id AS STRING) = CAST(b.user_id AS STRING)
LEFT JOIN d ON CAST(y.user_id AS STRING) = CAST(d.user_id AS STRING)
LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s ON s.transactionid = y.purchase_id   
WHERE DATE(purchase_timestamp) BETWEEN '2024-05-01' AND CURRENT_DATE()-1
AND y.country_to != 'Россия'
)
, f AS ( --покупки с категориями
SELECT DISTINCT
  DATE_TRUNC(date,WEEK(MONDAY)) AS week,
  country_by_phone,
  country_from,
  country_to,
  city_from,
  city_to,
  platform,
  category, --задвоение в 1%
  segment,
  repeated_first_purchase,
  purchase_id,
  IF(local_purchase IS NOT NULL, purchase_id, NULL) AS local_purchase,
  IF(foreign_purchase IS NOT NULL, purchase_id, NULL) AS foreign_purchase,
  IF(russian_purchase IS NOT NULL, purchase_id, NULL) AS russian_purchase,
  IF(russian_purchase_in_country_from IS NOT NULL, purchase_id, NULL) AS russian_purchase_in_country_from,
  IF(local_purchase IS NULL AND foreign_purchase IS NULL AND russian_purchase IS NULL AND russian_purchase_in_country_from IS NULL, purchase_id, NULL) AS other_purhases,
FROM e
)
SELECT DISTINCT 
  f.week,
  f.country_by_phone,
  f.country_from,
  f.country_to,
  f.city_from,
  f.city_to,
  f.platform,
  f.category,
  f.segment,
  f.purchase_id,
  ROUND(SAFE_DIVIDE(COUNT (DISTINCT  IF(f.repeated_first_purchase = 1,f.repeated_first_purchase,NULL)) , COUNT( IF(f.repeated_first_purchase = 1,f.repeated_first_purchase,NULL)) OVER (PARTITION BY f.week, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment,f.purchase_id)),2) AS  repeated_first_purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT f.purchase_id) , COUNT(f.purchase_id) OVER (PARTITION BY f.week, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment, f.purchase_id)),2) AS purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT f.local_purchase) , COUNT(f.local_purchase) OVER (PARTITION BY f.week, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment,f.local_purchase)),2) AS local_purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT f.foreign_purchase) , COUNT(f.foreign_purchase) OVER (PARTITION BY f.week, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment,f.purchase_id)),2) AS foreign_purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT f.russian_purchase) , COUNT(f.russian_purchase) OVER (PARTITION BY f.week, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment, f.purchase_id)),2) AS russian_purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT f.russian_purchase_in_country_from) , COUNT(f.russian_purchase_in_country_from) OVER (PARTITION BY f.week, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment,f.purchase_id)),2) AS russian_purchase_in_country_from,
  ROUND(SAFE_DIVIDE(COUNT (DISTINCT f.other_purhases) , COUNT(f.other_purhases) OVER (PARTITION BY f.week, f.country_by_phone, f.country_from, f.country_to, f.platform, f.segment,f.purchase_id)),2) AS other_purchases,
FROM f 
GROUP BY  f.week,f.country_by_phone,f.country_from, f.country_to, f.city_from, f.city_to,f.platform,f.category,f.segment, f.purchase_id,f.repeated_first_purchase,f.local_purchase,f.foreign_purchase,f.russian_purchase,f.russian_purchase_in_country_from,f.other_purhases
ORDER BY   f.purchase_id,f.week DESC