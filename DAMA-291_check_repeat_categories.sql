-- берутся категории повторных покупок. ОК.
--покупки в первом месяце считаются правильно. ОК.
WITH a AS (
SELECT -- получить список телефонов зарегистрированных пользователей
  DATE_TRUNC(DATE(create_at),MONTH) AS month_of_registration,
  CAST(id AS STRING) AS registered_user, 
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone
FROM `funnel-flowwow.MYSQL_EXPORT.f_user` 
WHERE create_at >= '2023-01-01'
)
, b AS (
SELECT -- определить страну по номеру
  month_of_registration,
  registered_user,
  phone AS country_by_phone
FROM a
)
, c AS ( -- получить данные по покупкам
SELECT DISTINCT 
  DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS purchase_month,
  CAST(user_id AS STRING) AS purchaser,
  purchase_id,
  IF(is_first_purchase = 1, purchase_id, 0) AS first_purchase,
  IF(is_first_purchase = 0, purchase_id, 0) AS repeated_purchase,
  product_id,
  category_name, --из-за этого идёт задваивание
  product_price
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
WHERE user_id = 5327716
)
, d AS ( --соединить покупки по месяцам с регистрациями
SELECT
  b.month_of_registration,
  COUNT(DISTINCT b.registered_user) AS registered_user,
  FORMAT_DATE('%Y_%m', c.purchase_month) AS purchase_month,
  b.country_by_phone,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.repeated_purchase,NULL)) AS repeated_purchase, -- посчитать уникальные повторные покупки, где нет 0
  SUM(c.product_price) AS revenue
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5327716'
GROUP BY purchase_month,country_by_phone,month_of_registration
ORDER BY registered_user
)
, e AS ( --соединить покупки по годам с регистрациями
SELECT
  b.month_of_registration,
  COUNT(DISTINCT b.registered_user) AS registered_user,
  FORMAT_DATE('%Y',c.purchase_month) AS purchase_year,
  b.country_by_phone,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.repeated_purchase,NULL)) AS repeated_purchase,
  SUM(c.product_price) AS revenue
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5327716'
GROUP BY month_of_registration,purchase_year,country_by_phone,category_name
ORDER BY registered_user
)
, f AS ( -- сгруппировать по категориям и месяцам покупки
SELECT DISTINCT
  b.month_of_registration,
  FORMAT_DATE('%Y_%m',c.purchase_month) AS category_month,
  b.country_by_phone,
  c.category_name,
  -- STRING_AGG(DISTINCT c.category_name) AS arr_category_name,
  -- STRING_AGG(c.category_name) OVER (PARTITION BY b.month_of_registration,c.purchase_month,b.country_by_phone) AS arr_category_name,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5327716' AND repeated_purchase != 0
GROUP BY month_of_registration,purchase_month,country_by_phone,c.category_name
)
, h AS ( -- сгруппировать по категориям и годам покупки
SELECT DISTINCT
  b.month_of_registration,
  FORMAT_DATE('%Y',c.purchase_month) AS category_year,
  b.country_by_phone,
  c.category_name,
  -- STRING_AGG(DISTINCT c.category_name) AS arr_category_name
  -- STRING_AGG(c.category_name) OVER (PARTITION BY b.month_of_registration,FORMAT_DATE('%Y',c.purchase_month),b.country_by_phone) AS arr_category_name,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5327716' AND repeated_purchase !=0
GROUP BY month_of_registration,purchase_month,country_by_phone,c.category_name
)
-- , j AS ( -- покупки первого месяца
SELECT
  b.month_of_registration,
  b.country_by_phone,
  STRING_AGG(DISTINCT c.category_name) AS arr_category_name, --категории товаров в первом месяце
  COUNT(DISTINCT IF(b.month_of_registration = c.purchase_month,purchase_id,NULL)) AS first_month_purchases, --количество покупок в первом месяце - месяце регистрации
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5327716'
GROUP BY month_of_registration,country_by_phone
-- )