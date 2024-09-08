WITH a AS (
SELECT -- получить список телефонов, email зарегистрированных пользователей
  DATE(create_at) AS registration_date,
  CAST(id AS STRING) AS registered_user, 
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(email,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS email,
FROM `funnel-flowwow.MYSQL_EXPORT.f_user` user   
WHERE user.create_at >= '2023-01-01'
GROUP BY registration_date,registered_user,phone,email
)
, b AS (
SELECT -- определить регион по номеру
  registration_date,
  registered_user,
  phone,
  CASE
    WHEN STARTS_WITH(phone,'995')THEN 'Грузия' 
    WHEN STARTS_WITH(phone,'996') OR STARTS_WITH(phone,'374') OR STARTS_WITH(phone,'375') OR STARTS_WITH(phone,'994') OR STARTS_WITH(phone,'992') OR STARTS_WITH(phone,'993') OR  STARTS_WITH(phone,'998') OR STARTS_WITH(phone,'76') OR STARTS_WITH(phone,'77') OR STARTS_WITH(phone,'373') THEN 'СНГ'
    WHEN STARTS_WITH(phone,'7') AND NOT (STARTS_WITH(phone,'76') OR STARTS_WITH(phone,'77'))THEN 'Россия'
    WHEN STARTS_WITH(phone,'34')THEN 'Испания'
    WHEN STARTS_WITH(phone,'44')THEN 'Британия' 
    WHEN STARTS_WITH(phone,'971')THEN 'ОАЭ'
    ELSE 'other country'
    END AS region_by_phone,
    email
FROM a
)
, c AS (
SELECT --количество отзывов на пользователя #надо исключить отзывы, где user_id c несколькими одинаковыми оценками и одним типом под одним заказом?
  CAST(user_id AS STRING) AS user_id,
  COUNT(DISTINCT id) AS cnt_reviews --количество отзывов
FROM `funnel-flowwow.MYSQL_EXPORT.f_review`
WHERE order_id != 0 AND create_at >= '2023-01-01'
GROUP BY user_id
)
, d AS ( -- получить данные по покупкам
SELECT DISTINCT
  CAST(user_id AS STRING) AS purchaser,
  STRING_AGG(DISTINCT city_from_name) AS city_from,
  STRING_AGG(DISTINCT country_to) AS country_to,
  STRING_AGG(DISTINCT city_name) AS city_to, --город в
  COUNT(DISTINCT purchase_id) AS purchases,
  STRING_AGG(DISTINCT category_name) AS category_name,
  COUNT(DISTINCT IF(promocode IS NOT NULL,1,NULL)) AS promocode --использовал хотя бы один раз промокод
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y 
GROUP BY purchaser
)
,e AS (
SELECT
  CAST(id AS STRING) AS user_id,
  STRING_AGG(DISTINCT address) AS address
FROM  `funnel-flowwow.MYSQL_EXPORT.f_order_archive`
GROUP BY user_id
)
SELECT -- соединить регистрации с покупками
  registration_date,
  registered_user,
  phone,
  region_by_phone,
  email,
  address,
  cnt_reviews,
  purchaser,
  city_from,
  country_to,
  city_to,
  purchases,
  category_name,
  promocode
FROM b 
LEFT JOIN d ON b.registered_user = d.purchaser 
LEFT JOIN c ON b.registered_user = c.user_id
LEFT JOIN e ON b.registered_user = e.user_id
ORDER BY registration_date