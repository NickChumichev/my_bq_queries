WITH a AS ( --купили из ОАЭ в ОАЭ
SELECT DISTINCT
  user_id
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` 
WHERE 1=1 
AND LOWER(country_to) = 'объединенные арабские эмираты' 
AND LOWER(country_from) = 'объединенные арабские эмираты'
AND DATE(purchase_timestamp) BETWEEN '2024-01-01' AND CURRENT_DATE()-1
)
, b AS (
SELECT DISTINCT
  user_id,
  COUNT(DISTINCT purchase_id) AS all_purchases,
  COUNT(DISTINCT IF(LOWER(country_from) = 'объединенные арабские эмираты', purchase_id, NULL)) AS aue_purchases,
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` 
WHERE user_id IN (SELECT user_id FROM a)
GROUP BY 1 
)
, c AS (
SELECT DISTINCT
  user_id,
  LAST_VALUE(DATE(purchase_timestamp)) OVER (PARTITION BY user_id ORDER BY purchase_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_purchase_date
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` 
WHERE user_id IN (SELECT user_id FROM a)
)
, d AS ( -- телефоны покупателей из ОАЭ
SELECT
  create_at,
  id AS user_id,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(email,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS email
FROM `funnel-flowwow.MYSQL_EXPORT.f_user` 
WHERE id IN (SELECT user_id FROM a)
)
, e AS ( --определить страну по номеру телефона
SELECT DISTINCT 
  DATE(create_at) AS create_at,
  user_id, -- id покупателя
  CASE
    WHEN STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))THEN "Россия"
    WHEN STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77") THEN "Казахстан"
    WHEN STARTS_WITH(phone,"41") THEN "Швейцария"
    WHEN STARTS_WITH(phone,"351") THEN "Португалия"
    WHEN STARTS_WITH(phone,"39") THEN "Италия"
    ELSE s.TitleRU
    END AS country,
  phone,
  email,
  all_purchases,
  aue_purchases,
  last_purchase_date
FROM d 
  LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.country_phone_codes` s ON (STARTS_WITH(LOWER(phone),s.PhoneCode))
  LEFT JOIN b USING(user_id) 
  LEFT JOIN c USING(user_id) 
ORDER BY user_id, country DESC
)
, f AS (
SELECT DISTINCT 
   _user_id_, 
   last_is_allowed 
FROM `funnel-flowwow.Analyt_ChumichevN.DAMA_581_push_access_push_opened` 
WHERE _event_name_ = 'push_access'
)
SELECT DISTINCT
  e.create_at,--дата регистрации пользователя
  e.user_id,
  e.country,
  e.phone,
  e.email,
  e.all_purchases,
  e.aue_purchases,
  e.last_purchase_date,
  p._user_id_,
  CASE
    WHEN f.last_is_allowed = 'is_allowed":"0"' THEN 0
    WHEN f.last_is_allowed = 'is_allowed":"1"' THEN 1
    ELSE NULL
    END AS push_access,
  IF(p._user_id_ IS NULL,0,1) AS is_app,
  COUNT(DISTINCT IF(_event_name_ = 'push_opened',1, NULL)) AS push_opened, 
FROM e 
LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.DAMA_581_push_access_push_opened` p ON CAST(e.user_id AS STRING) = CAST(p._user_id_ AS STRING)
LEFT JOIN f ON CAST(e.user_id AS STRING) = CAST(f._user_id_ AS STRING)
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY e.country, e.user_id DESC
