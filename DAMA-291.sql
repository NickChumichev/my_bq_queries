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
  phone,
  CASE
    WHEN STARTS_WITH(phone,'995')THEN 'Грузия' 
    WHEN STARTS_WITH(phone,'996')THEN 'СНГ'
    WHEN STARTS_WITH(phone,'374')THEN 'СНГ'
    WHEN STARTS_WITH(phone,'375')THEN 'СНГ' 
    WHEN STARTS_WITH(phone,'994')THEN 'СНГ'
    WHEN STARTS_WITH(phone,'7') AND NOT (STARTS_WITH(phone,'76') OR STARTS_WITH(phone,'77'))THEN 'Россия'
    WHEN STARTS_WITH(phone,'34')THEN 'Испания'
    WHEN STARTS_WITH(phone,'44')THEN 'Великобритания' 
    WHEN STARTS_WITH(phone,'992')THEN 'СНГ'
    WHEN STARTS_WITH(phone,'993')THEN 'СНГ'
    WHEN STARTS_WITH(phone,'998')THEN 'СНГ' 
    WHEN STARTS_WITH(phone,'76') OR STARTS_WITH(phone,'77') THEN 'СНГ'
    WHEN STARTS_WITH(phone,'373')THEN 'СНГ'
     WHEN STARTS_WITH(phone,'971')THEN 'ОАЭ'
    ELSE 'other country'
    END AS country_by_phone
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
-- WHERE user_id = 5327716
)
, d AS ( --соединить покупки по месяцам с регистрациями
SELECT
  b.month_of_registration,
  COUNT(DISTINCT IF(c.repeated_purchase !=0 AND b.month_of_registration != c.purchase_month,c.purchaser,NULL)) AS repeated_purchaser, --определить повторного покупателя по повторной покупке и где месяц регистрации не совпадает с месяцем покупки 
  FORMAT_DATE('%Y_%m', c.purchase_month) AS purchase_month,
  b.country_by_phone,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.repeated_purchase,NULL)) AS repeated_purchase, -- посчитать уникальные повторные покупки, где нет 0
  SUM(IF(c.repeated_purchase !=0,c.product_price,NULL)) AS repeated_revenue -- доход с повторных покупок
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
-- WHERE registered_user = '5327716'
GROUP BY purchase_month,country_by_phone,month_of_registration
)
, e AS ( --соединить покупки по годам с регистрациями
SELECT
  b.month_of_registration,
  COUNT(DISTINCT IF(c.repeated_purchase !=0 AND b.month_of_registration != c.purchase_month,c.purchaser,NULL)) AS repeated_purchaser,
  FORMAT_DATE('%Y',c.purchase_month) AS purchase_year,
  b.country_by_phone,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.repeated_purchase,NULL)) AS repeated_purchase,
  SUM(IF(c.repeated_purchase !=0,c.product_price,NULL)) AS repeated_revenue
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
-- WHERE registered_user = '5327716'
GROUP BY month_of_registration,purchase_year,country_by_phone
)
, f AS ( -- сгруппировать по категориям и месяцам покупки
SELECT DISTINCT
  b.month_of_registration,
  FORMAT_DATE('%Y_%m',c.purchase_month) AS repeated_category_month,
  b.country_by_phone,
  c.category_name,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE 1=1 
-- AND registered_user = '5327716' 
AND repeated_purchase != 0  --посчитать только для повторных покупок
GROUP BY month_of_registration,purchase_month,country_by_phone,c.category_name
)
, g AS ( -- сгруппировать по категориям и годам покупки
SELECT DISTINCT
  b.month_of_registration,
  FORMAT_DATE('%Y',c.purchase_month) AS repeated_category_year,
  b.country_by_phone,
  c.category_name,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE 1=1 
-- AND registered_user = '5327716' 
AND repeated_purchase != 0
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
-- WHERE registered_user = '5327716'
GROUP BY month_of_registration,country_by_phone
)
, j AS ( -- количество покупателей по месяцам регистрации
SELECT
  b.month_of_registration,
  b.country_by_phone,
  COUNT(DISTINCT IF(purchaser IS NOT NULL,purchaser,NULL)) AS purchaser_first_month
FROM b LEFT JOIN c ON b.registered_user = c.purchaser AND  b.month_of_registration = c.purchase_month
GROUP BY b.month_of_registration, b.country_by_phone
)
, k AS (
SELECT -- соединить сводные таблицы по месяцам, по годам с покупками первого месяца
  *
FROM d
PIVOT(
  SUM(repeated_purchaser) AS repeated_purchaser,
  SUM(repeated_purchase) AS repeated_purchase,
  SUM(repeated_revenue) AS repeated_revenue
FOR purchase_month IN ('2023_01', '2023_02', '2023_03', '2023_04', '2023_05', '2023_06', '2023_07', '2023_08', '2023_09', '2023_10', '2023_11', '2023_12')
)
FULL JOIN 
(
  SELECT
    *
FROM e 
PIVOT(
  SUM(repeated_purchaser) AS repeated_purchaser,
  SUM(repeated_purchase) AS repeated_purchase, 
  SUM(repeated_revenue) AS repeated_revenue
FOR purchase_year IN (
'2024'))
)
USING (month_of_registration,country_by_phone)
FULL JOIN 
(SELECT
  *
FROM f
PIVOT(STRING_AGG(DISTINCT category_name) AS repeated_category FOR repeated_category_month IN ('2023_01','2023_02','2023_03','2023_04','2023_05','2023_06','2023_07','2023_08','2023_09','2023_10','2023_11','2023_12'))) 
USING (month_of_registration,country_by_phone)
FULL JOIN 
(SELECT
  *
FROM g
PIVOT(STRING_AGG(DISTINCT category_name) AS repeated_category FOR repeated_category_year IN ('2024'))) 
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
FULL JOIN
(SELECT
  month_of_registration,
  country_by_phone,
  purchaser_first_month
FROM j)
USING (month_of_registration,country_by_phone)
)
SELECT DISTINCT
  month_of_registration,
  country_by_phone,
  registered_user, 
  purchaser_first_month,
  first_month_purchases,
  first_month_categories,
  repeated_purchase_2023_01,
  repeated_revenue_2023_01,
  repeated_category_2023_01, --категории повторных покупок
  repeated_purchaser_2023_02,
  SAFE_DIVIDE(repeated_purchaser_2023_02,registered_user) AS rr_repeated_purchaser_2023_02,
  repeated_purchase_2023_02,
  repeated_revenue_2023_02,
  repeated_category_2023_02, 
  repeated_purchaser_2023_03,
  SAFE_DIVIDE(repeated_purchaser_2023_03,registered_user) AS rr_repeated_purchaser_2023_03,
  repeated_purchase_2023_03,
  repeated_revenue_2023_03,
  repeated_category_2023_03, 
  repeated_purchaser_2023_04,
  SAFE_DIVIDE(repeated_purchaser_2023_04,registered_user) AS rr_repeated_purchaser_2023_04,
  repeated_purchase_2023_04,
  repeated_revenue_2023_04,
  repeated_category_2023_04, 
  repeated_purchaser_2023_05,
  SAFE_DIVIDE(repeated_purchaser_2023_05,registered_user) AS rr_repeated_purchaser_2023_05,
  repeated_purchase_2023_05,
  repeated_revenue_2023_05,
  repeated_category_2023_05, 
  repeated_purchaser_2023_06,
  SAFE_DIVIDE(repeated_purchaser_2023_06,registered_user) AS rr_repeated_purchaser_2023_06,
  repeated_purchase_2023_06,
  repeated_revenue_2023_06,
  repeated_category_2023_06, 
  repeated_purchaser_2023_07,
  SAFE_DIVIDE(repeated_purchaser_2023_07,registered_user) AS rr_repeated_purchaser_2023_07,
  repeated_purchase_2023_07,
  repeated_revenue_2023_07,
  repeated_category_2023_07, 
  repeated_purchaser_2023_08,
  SAFE_DIVIDE(repeated_purchaser_2023_08,registered_user) AS rr_repeated_purchaser_2023_08,
  repeated_purchase_2023_08,
  repeated_revenue_2023_08,
  repeated_category_2023_08, 
  repeated_purchaser_2023_09,
  SAFE_DIVIDE(repeated_purchaser_2023_09,registered_user) AS rr_repeated_purchaser_2023_09,
  repeated_purchase_2023_09,
  repeated_revenue_2023_09,
  repeated_category_2023_09, 
  repeated_purchaser_2023_10,
  SAFE_DIVIDE(repeated_purchaser_2023_10,registered_user) AS rr_repeated_purchaser_2023_10,
  repeated_purchase_2023_10,
  repeated_revenue_2023_10,
  repeated_category_2023_10, 
  repeated_purchaser_2023_11,
  SAFE_DIVIDE(repeated_purchaser_2023_11,registered_user) AS rr_repeated_purchaser_2023_11,
  repeated_purchase_2023_11,
  repeated_revenue_2023_11,
  repeated_category_2023_11, 
  repeated_purchaser_2023_12,
  SAFE_DIVIDE(repeated_purchaser_2023_12,registered_user) AS rr_repeated_purchaser_2023_12,
  repeated_purchase_2023_12,
  repeated_revenue_2023_12,
  repeated_category_2023_12, 
  repeated_purchaser_2024,
  SAFE_DIVIDE(repeated_purchaser_2024,registered_user) AS rr_repeated_purchaser_2024,
  repeated_purchase_2024,
  repeated_revenue_2024,
  repeated_category_2024 
FROM k
ORDER BY country_by_phone, month_of_registration ASC