
WITH a AS (
SELECT -- получить список телефонов зарегистрированных пользователей
  DATE_TRUNC(DATE(create_at),MONTH) AS month_of_registration,
  CAST(id AS STRING) AS registered_user, 
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone
FROM `funnel-flowwow.MYSQL_EXPORT.f_user` 
WHERE create_at >= '2023-01-01' AND  CAST(id AS STRING) = '5327716'
)
, b AS (
SELECT -- определить страну по номеру
  month_of_registration,
  registered_user,
  phone AS country_by_phone
FROM a
-- WHERE STARTS_WITH(phone,'1')
)
, c AS ( -- получить данные по покупкам
SELECT DISTINCT 
  DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS purchase_month,
  CAST(user_id AS STRING) AS purchaser,
  purchase_id,
  IF(is_first_purchase = 1, purchase_id, 0) AS first_purchase,
  IF(is_first_purchase = 0, purchase_id, 0) AS repeated_purchase,
  product_id,
  category_name,
  product_price
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
WHERE user_id = 5327716
)
, d AS ( --соединить покупки по месяцам с регистрациями
SELECT
  b.month_of_registration,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.purchaser,NULL)) AS purchaser,
  FORMAT_DATE('%Y_%m', c.purchase_month) AS purchase_month,
  b.country_by_phone,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.repeated_purchase,NULL)) AS repeated_purchase, -- посчитать уникальные повторные покупки, где нет 0
  SUM(c.product_price) AS revenue
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5327716'
GROUP BY purchase_month,country_by_phone,month_of_registration
)
, e AS ( --соединить покупки по годам с регистрациями
SELECT
  b.month_of_registration,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.purchaser,NULL)) AS purchaser,
  FORMAT_DATE('%Y',c.purchase_month) AS purchase_year,
  b.country_by_phone,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.repeated_purchase,NULL)) AS repeated_purchase,
  SUM(c.product_price) AS revenue
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5327716'
GROUP BY month_of_registration,purchase_year,country_by_phone
)
, f AS ( -- сгруппировать по категориям и месяцам покупки
SELECT DISTINCT
  b.month_of_registration,
  FORMAT_DATE('%Y_%m',c.purchase_month) AS category_month,
  b.country_by_phone,
  c.category_name,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE 1=1 
AND registered_user = '5327716' 
AND repeated_purchase IS NOT NULL
GROUP BY month_of_registration,purchase_month,country_by_phone,c.category_name
)
, g AS ( -- сгруппировать по категориям и годам покупки
SELECT DISTINCT
  b.month_of_registration,
  FORMAT_DATE('%Y',c.purchase_month) AS category_year,
  b.country_by_phone,
  c.category_name,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE 1=1 
AND registered_user = '5327716' 
AND repeated_purchase IS NOT NULL
GROUP BY month_of_registration,purchase_month,country_by_phone,c.category_name
)
, h AS ( -- количество регистраций по месяцам
SELECT
  month_of_registration,
  country_by_phone,
  COUNT(DISTINCT registered_user) AS registered_user
FROM b
GROUP BY month_of_registration,country_by_phone
)
, i AS ( -- покупки первого месяца
SELECT
  b.month_of_registration,
  b.country_by_phone,
  STRING_AGG(DISTINCT IF(b.month_of_registration = c.purchase_month, c.category_name, NULL)) AS arr_category_name, --категории товаров в первом месяце
  COUNT(DISTINCT IF(b.month_of_registration = c.purchase_month,purchase_id,NULL)) AS first_month_purchases, --количество покупок в первом месяце - месяце регистрации
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE registered_user = '5327716'
GROUP BY month_of_registration,country_by_phone
)
, k AS (
SELECT -- соединить сводные таблицы по месяцам, по годам с покупками первого месяца
  *
FROM d
PIVOT(
  SUM(purchaser) AS purchaser,
  SUM(repeated_purchase) AS repeated_purchase,
  SUM(revenue) AS revenue
FOR purchase_month IN (
'2024_01',
'2024_02')
)
FULL JOIN 
(
  SELECT
    *
FROM e 
PIVOT(
  SUM(purchaser) AS purchaser,
  SUM(repeated_purchase) AS repeated_purchase, 
  SUM(revenue) AS revenue
FOR purchase_year IN (
'2024'))
)
USING (month_of_registration,country_by_phone)
FULL JOIN 
(SELECT
  *
FROM f
PIVOT(STRING_AGG(DISTINCT category_name) FOR category_month IN ('2024_01','2024_02'))) 
USING (month_of_registration,country_by_phone)
FULL JOIN 
(SELECT
  *
FROM g
PIVOT(STRING_AGG(DISTINCT category_name) FOR category_year IN ('2024'))) 
USING (month_of_registration,country_by_phone)
FULL JOIN
(SELECT
  month_of_registration,
  country_by_phone,
  arr_category_name AS first_month_categories, --категории в первом месяце
  first_month_purchases, --количество покупок в первом месяце - месяце регистрации
FROM i)
USING (month_of_registration,country_by_phone)
FULL JOIN
(SELECT
  month_of_registration,
  country_by_phone,
  registered_user
FROM h)
USING (month_of_registration,country_by_phone)
)
SELECT DISTINCT
  month_of_registration,
  country_by_phone,
  registered_user, 
  first_month_purchases,
  first_month_categories,
  purchaser_2024_01,
  repeated_purchase_2024_01,
  revenue_2024_01,
  `2024_01` AS repeated_category_2024_01, --категории повторных покупок
  purchaser_2024_02,
  repeated_purchase_2024_02,
  revenue_2024_02,
  `2024_02` AS repeated_category_2024_02,  
  purchaser_2024,
  repeated_purchase_2024,
  revenue_2024,
  `2024` AS category_2024 
FROM k
ORDER BY country_by_phone, month_of_registration ASC




