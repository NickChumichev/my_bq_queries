WITH phone_num AS (
SELECT
  id AS user_id,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,"jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=") AS phone
FROM `funnel-flowwow.MYSQL_EXPORT.f_user`
WHERE DATE(create_at) >= '2024-01-01'
)
, country_by_phone_num AS (
SELECT --получить страны регистрации по номеру телефона
  user_id,
  phone,
  CASE 
    WHEN STARTS_WITH(_phone_num.phone,"7") AND NOT (STARTS_WITH(_phone_num.phone,"76") OR STARTS_WITH(_phone_num.phone,"77"))THEN "Россия"
    WHEN STARTS_WITH(_phone_num.phone,"76") OR STARTS_WITH(_phone_num.phone,"77") THEN "Казахстан"
    WHEN STARTS_WITH(_phone_num.phone,"41") THEN "Швейцария"
    WHEN STARTS_WITH(_phone_num.phone,"62") THEN "Индонезия"
    WHEN STARTS_WITH(_phone_num.phone,"62") THEN "Асеньон"
    WHEN STARTS_WITH(_phone_num.phone,"672") THEN "Рождественсткие о-ва"
    WHEN STARTS_WITH(_phone_num.phone,"21") THEN "Тунис"
    WHEN STARTS_WITH(_phone_num.phone,"47") THEN "Норвегия"
    WHEN STARTS_WITH(_phone_num.phone,"351") THEN "Португалия"
    WHEN STARTS_WITH(_phone_num.phone,"39") THEN "Италия"
    WHEN STARTS_WITH(_phone_num.phone,"243") THEN "ДРК"
    WHEN STARTS_WITH(_phone_num.phone,"382") THEN "Черногория"
    WHEN STARTS_WITH(_phone_num.phone,"383") OR STARTS_WITH(_phone_num.phone,"990") THEN 'other_countries'
    WHEN phone_codes.phonecode IS NULL THEN 'undefined_phones'
  ELSE  phone_codes.title_ru 
  END AS country_by_phone,
  phonecode --дубль появляется, т.к у РФ и Казахстана один код
FROM phone_num AS _phone_num 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_MGS_country_phone_codes` AS phone_codes 
ON STARTS_WITH(_phone_num.phone,phone_codes.phonecode)  
WHERE SAFE_CAST(phone AS NUMERIC) IS NOT NULL
)
, purchases AS (
SELECT--покупки
  DATE(_purchases.purchase_timestamp) AS date,
  _purchases.country_from,
  _purchases.country_to,
  _purchases.user_id,
  _purchases.purchase_id,
  _purchases.is_first_purchase
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` AS _purchases
WHERE _purchases.user_id IN (SELECT user_id FROM country_by_phone_num)
)
, purchases_types AS ( -- получить типы покупок
SELECT DISTINCT
  country_by_phone_num.user_id AS registered_user,
  purchases.user_id AS purchaser,
  purchases.is_first_purchase AS is_first_purchase,
  country_by_phone_num.country_by_phone AS country_by_phone,
  purchases.country_from,
  purchases.country_to,
  IF(country_by_phone_num.country_by_phone = purchases.country_to, purchases.purchase_id,NULL) AS purchase_country_to_by_phone,
  IF(country_by_phone_num.country_by_phone = purchases.country_to AND purchases.is_first_purchase = 1, purchases.purchase_id,NULL) AS first_purchase_country_to_by_phone,
  IF(country_by_phone_num.country_by_phone != purchases.country_to, purchases.purchase_id,NULL) AS purchase_country_to_no_by_phone,
  IF(country_by_phone_num.country_by_phone = 'Россия', purchases.purchase_id,NULL) AS purchase_country_by_phone_rf,
FROM country_by_phone_num FULL JOIN purchases USING(user_id)
)
SELECT
  country_by_phone,
  COUNT(DISTINCT registered_user) AS registered_users,
  COUNT(DISTINCT purchaser) AS purchasers,
  SAFE_DIVIDE(COUNT(DISTINCT purchaser) , COUNT(DISTINCT registered_user)) AS conv_to_purchasers,
  COUNT(DISTINCT IF(purchase_country_to_by_phone IS NOT NULL, purchaser,NULL)) AS purchasers_country_to_by_phone,
  SAFE_DIVIDE(COUNT(DISTINCT IF(purchase_country_to_by_phone IS NOT NULL, purchaser,NULL)) ,COUNT(DISTINCT registered_user)) AS conv_to_purchasers_country_to_by_phone,
  COUNT(DISTINCT IF(first_purchase_country_to_by_phone IS NOT NULL,purchaser,NULL)) AS first_purchasers_country_to_by_phone,
  SAFE_DIVIDE(COUNT(DISTINCT IF(first_purchase_country_to_by_phone IS NOT NULL,purchaser,NULL)) , COUNT(DISTINCT registered_user)) AS conv_to_first_purchasers_country_to_by_phone,  
  COUNT(DISTINCT IF(purchase_country_to_no_by_phone IS NOT NULL,purchaser,NULL)) AS purchasers_country_to_no_by_phone,
  SAFE_DIVIDE(COUNT(DISTINCT IF(purchase_country_to_no_by_phone IS NOT NULL,purchaser,NULL)) , COUNT(DISTINCT registered_user)) AS conv_to_purchasers_country_to_no_by_phone, 
  COUNT(DISTINCT IF(purchase_country_by_phone_rf IS NOT NULL,purchaser,NULL)) AS purchasers_country_by_phone_rf,
  SAFE_DIVIDE(COUNT(DISTINCT IF(purchase_country_by_phone_rf IS NOT NULL,purchaser,NULL)) , COUNT(DISTINCT registered_user)) AS conv_to_purchasers_country_by_phone_rf
FROM purchases_types
GROUP BY 1
ORDER BY registered_users DESC