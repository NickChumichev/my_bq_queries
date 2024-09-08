WITH phone_num AS (
SELECT
  id AS user_id,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,"jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=") AS phone
FROM `funnel-flowwow.MYSQL_EXPORT.f_user`
)
, country_by_phone_num AS ( --получить страны регистрации по номеру телефона
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
    ELSE phone_codes.title_ru
  END AS country_by_phone, --страна регистрации пользователя
FROM phone_num AS _phone_num LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_MGS_country_phone_codes` AS phone_codes ON STARTS_WITH(_phone_num.phone,phone_codes.phonecode)  
WHERE SAFE_CAST(phone AS NUMERIC) IS NOT NULL
)
, language AS ( --получить последний язык ОС и браузера для user_id
SELECT DISTINCT
  _user_id_ AS user_id, 
  LAST_VALUE(_language_) OVER (PARTITION BY _user_id_  ORDER BY DATE(_created_at_) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS language,
FROM `funnel-flowwow.ADJUST_RAW.clients_app` AS adjust_raw
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(CURRENT_DATE()-1)
AND _language_ IS NOT NULL

UNION DISTINCT

SELECT DISTINCT 
  user_id,
  LAST_VALUE(REGEXP_EXTRACT(device.language, r'^([^-]*)')) OVER (PARTITION BY user_id  ORDER BY EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS language
FROM `firebase-flowwow.analytics_150948805.events_*` AS ga4_raw 
WHERE 1=1
AND device.language IS NOT NULL
AND _TABLE_SUFFIX = REPLACE(CAST(CURRENT_DATE()-1 AS STRING),"-","") 
)
, no_duplicates AS ( -- убрать дубликаты user_id
SELECT
  user_id,
  language
FROM language AS _language
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id) = 1
)
, purchases AS ( --покупки
SELECT
  DATE(purchase_timestamp) AS date,
  country_from,
  city_from_name AS city_from,
  country_to,
  city_name AS city_to,
  user_id,
  purchase_id,
  is_first_purchase,
  platform,
  category_name AS category
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` AS _purchases
WHERE DATE(_purchases.purchase_timestamp) = CURRENT_DATE()-1
)
, purchase_types AS ( --разбить покупки по типам
SELECT DISTINCT 
  crm.date,
  _country_by_phone_num.phone,
  _country_by_phone_num.country_by_phone,
  crm.country_from,
  crm.city_from,
  crm.country_to,
  crm.city_to,
  _country_by_phone_num.user_id,
  crm.purchase_id,
  crm.is_first_purchase,
  crm.platform,
  _no_duplicates.language,
  crm.category,
  CASE
    WHEN crm.is_first_purchase =  0 THEN 1 --признак повторной покупки
    WHEN crm.is_first_purchase =  1 THEN 0 -- признак первой покупки
  END AS repeated_first_purchase,
  source_transactions.segment, 
  IF((crm.country_to = _country_by_phone_num.country_by_phone AND crm.country_to = crm.country_from AND _country_by_phone_num.country_by_phone != 'Россия'AND _no_duplicates.language != 'ru') OR (crm.country_to = _country_by_phone_num.country_by_phone AND crm.country_to = crm.country_from AND _country_by_phone_num.country_by_phone != 'Россия'AND _no_duplicates.language IS NULL), purchase_id, NULL) AS local_purchase, --локальная покупка
  IF(crm.country_to != _country_by_phone_num.country_by_phone AND _country_by_phone_num.country_by_phone != 'Россия' AND crm.country_to != crm.country_from AND (_no_duplicates.language IS NULL OR _no_duplicates.language != 'ru'), purchase_id, NULL) AS foreign_purchase, --иностранная покупка
  IF((_no_duplicates.language = 'ru' AND STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")))  
    OR (STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))) OR (_no_duplicates.language = 'ru'), purchase_id, NULL) AS russian_purchase, --рф покупка
  IF((_no_duplicates.language = 'ru' AND STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")) AND crm.country_to = crm.country_from)  
    OR (STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77")) AND crm.country_to = crm.country_from) OR (_no_duplicates.language = 'ru' AND crm.country_to = crm.country_from), purchase_id, NULL) AS russian_purchase_in_country_from, -- покупка россиянина в стране заказа
FROM purchases AS crm
LEFT JOIN country_by_phone_num AS _country_by_phone_num ON CAST(crm.user_id AS STRING) = CAST(_country_by_phone_num.user_id AS STRING)
LEFT JOIN no_duplicates AS _no_duplicates ON CAST(crm.user_id AS STRING) = CAST(_no_duplicates.user_id AS STRING)
LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` AS source_transactions ON source_transactions.transactionid = crm.purchase_id   
WHERE 1=1
AND crm.country_to != 'Россия'
)
, purchases_with_categories AS ( --покупки с категориями
SELECT DISTINCT
  DATE_TRUNC(date,WEEK(MONDAY)) AS week,
  date as partition_date,
  country_by_phone,
  country_from,
  country_to,
  city_from,
  city_to,
  phone,
  language,
  platform,
  category,
  segment,
  repeated_first_purchase,
  purchase_id,
  IF(local_purchase IS NOT NULL, purchase_id, NULL) AS local_purchase,
  IF(foreign_purchase IS NOT NULL, purchase_id, NULL) AS foreign_purchase,
  IF(russian_purchase IS NOT NULL, purchase_id, NULL) AS russian_purchase,
  IF(russian_purchase_in_country_from IS NOT NULL, purchase_id, NULL) AS russian_purchase_in_country_from,
  IF(local_purchase IS NULL AND foreign_purchase IS NULL AND russian_purchase IS NULL AND russian_purchase_in_country_from IS NULL, purchase_id, NULL) AS other_purhases,
FROM purchase_types AS _purchase_types
)
SELECT DISTINCT 
  _purchases_with_categories.week,
  _purchases_with_categories.partition_date,
  _purchases_with_categories.country_by_phone,
  _purchases_with_categories.country_from,
  _purchases_with_categories.country_to,
  _purchases_with_categories.city_from,
  _purchases_with_categories.city_to,
  _purchases_with_categories.phone,
  _purchases_with_categories.language,
  _purchases_with_categories.platform,
  _purchases_with_categories.category,
  _purchases_with_categories.segment,
  _purchases_with_categories.purchase_id,
  ROUND(SAFE_DIVIDE(COUNT (DISTINCT  IF(_purchases_with_categories.repeated_first_purchase = 1,_purchases_with_categories.repeated_first_purchase,NULL)) , COUNT( IF(_purchases_with_categories.repeated_first_purchase = 1,_purchases_with_categories.repeated_first_purchase,NULL)) OVER (PARTITION BY _purchases_with_categories.week, _purchases_with_categories.country_by_phone, _purchases_with_categories.country_from, _purchases_with_categories.country_to, _purchases_with_categories.platform, _purchases_with_categories.segment,_purchases_with_categories.purchase_id)),2) AS  repeated_first_purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT _purchases_with_categories.purchase_id) , COUNT(_purchases_with_categories.purchase_id) OVER (PARTITION BY _purchases_with_categories.week, _purchases_with_categories.country_by_phone, _purchases_with_categories.country_from, _purchases_with_categories.country_to, _purchases_with_categories.platform, _purchases_with_categories.segment, _purchases_with_categories.purchase_id)),2) AS purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT _purchases_with_categories.local_purchase) , COUNT(_purchases_with_categories.local_purchase) OVER (PARTITION BY _purchases_with_categories.week, _purchases_with_categories.country_by_phone, _purchases_with_categories.country_from, _purchases_with_categories.country_to, _purchases_with_categories.platform, _purchases_with_categories.segment, _purchases_with_categories.local_purchase)),2) AS local_purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT _purchases_with_categories.foreign_purchase) , COUNT(_purchases_with_categories.foreign_purchase) OVER (PARTITION BY _purchases_with_categories.week, _purchases_with_categories.country_by_phone, _purchases_with_categories.country_from, _purchases_with_categories.country_to, _purchases_with_categories.platform, _purchases_with_categories.segment, _purchases_with_categories.purchase_id)),2) AS foreign_purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT _purchases_with_categories.russian_purchase) , COUNT(_purchases_with_categories.russian_purchase) OVER (PARTITION BY _purchases_with_categories.week, _purchases_with_categories.country_by_phone, _purchases_with_categories.country_from, _purchases_with_categories.country_to, _purchases_with_categories.platform, _purchases_with_categories.segment, _purchases_with_categories.purchase_id)),2) AS russian_purchase,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT _purchases_with_categories.russian_purchase_in_country_from) , COUNT(_purchases_with_categories.russian_purchase_in_country_from) OVER (PARTITION BY _purchases_with_categories.week, _purchases_with_categories.country_by_phone, _purchases_with_categories.country_from, _purchases_with_categories.country_to, _purchases_with_categories.platform, _purchases_with_categories.segment, _purchases_with_categories.purchase_id)),2) AS russian_purchase_in_country_from,
  ROUND(SAFE_DIVIDE(COUNT (DISTINCT _purchases_with_categories.other_purhases) , COUNT(_purchases_with_categories.other_purhases) OVER (PARTITION BY _purchases_with_categories.week, _purchases_with_categories.country_by_phone, _purchases_with_categories.country_from, _purchases_with_categories.country_to, _purchases_with_categories.platform, _purchases_with_categories.segment, _purchases_with_categories.purchase_id)),2) AS other_purchases,
FROM purchases_with_categories AS _purchases_with_categories
GROUP BY  _purchases_with_categories.week, _purchases_with_categories.partition_date, _purchases_with_categories.country_by_phone, _purchases_with_categories.country_from, _purchases_with_categories.country_to, _purchases_with_categories.city_from, _purchases_with_categories.city_to, _purchases_with_categories.language, _purchases_with_categories.phone, _purchases_with_categories.platform, _purchases_with_categories.category, _purchases_with_categories.segment, _purchases_with_categories.purchase_id, _purchases_with_categories.repeated_first_purchase, _purchases_with_categories.local_purchase, _purchases_with_categories.foreign_purchase, _purchases_with_categories.russian_purchase, _purchases_with_categories.russian_purchase_in_country_from, _purchases_with_categories.other_purhases