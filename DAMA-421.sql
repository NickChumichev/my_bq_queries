WITH a AS (
SELECT -- получить список телефонов зарегистрированных пользователей
  DATE_TRUNC(DATE(create_at),MONTH) AS month_of_registration,
  CAST(id AS STRING) AS registered_user, 
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,"jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=") AS phone
FROM `funnel-flowwow.MYSQL_EXPORT.f_user` 
WHERE create_at >= "2023-01-01"
)
, b AS (
SELECT -- определить страну по номеру
  month_of_registration,
  registered_user,
  phone,
  CASE
    WHEN STARTS_WITH(phone,"995")THEN "Грузия" 
    WHEN STARTS_WITH(phone,"996")THEN "СНГ"
    WHEN STARTS_WITH(phone,"374")THEN "СНГ"
    WHEN STARTS_WITH(phone,"375")THEN "СНГ" 
    WHEN STARTS_WITH(phone,"994")THEN "СНГ"
    WHEN STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))THEN "Россия"
    WHEN STARTS_WITH(phone,"34")THEN "Испания"
    WHEN STARTS_WITH(phone,"44")THEN "Великобритания" 
    WHEN STARTS_WITH(phone,"992")THEN "СНГ"
    WHEN STARTS_WITH(phone,"993")THEN "СНГ"
    WHEN STARTS_WITH(phone,"998")THEN "СНГ" 
    WHEN STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77") THEN "СНГ"
    WHEN STARTS_WITH(phone,"373")THEN "СНГ"
     WHEN STARTS_WITH(phone,"971")THEN "ОАЭ"
    ELSE "other country"
    END AS country_by_phone
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
  category_name,
  product_price
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
)
, d AS ( --соединить покупки по месяцам с регистрациями
SELECT
  b.month_of_registration,
  b.country_by_phone,
  c.category_name,
  COUNT(DISTINCT IF(c.repeated_purchase !=0 AND b.month_of_registration != c.purchase_month,c.purchaser,NULL)) AS repeated_purchaser, --определить повторного покупателя по повторной покупке и где месяц регистрации не совпадает с месяцем покупки 
  FORMAT_DATE("%Y_%m", c.purchase_month) AS purchase_month,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.repeated_purchase,NULL)) AS repeated_purchase, -- посчитать уникальные повторные покупки, где нет 0
  SUM(IF(c.repeated_purchase !=0,c.product_price,NULL)) AS repeated_revenue, -- доход с повторных покупок
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
GROUP BY purchase_month,country_by_phone,month_of_registration,category_name
)
, e AS ( --соединить покупки по годам с регистрациями
SELECT
  b.month_of_registration,
  b.country_by_phone,
  c.category_name,
  COUNT(DISTINCT IF(c.repeated_purchase !=0 AND b.month_of_registration != c.purchase_month,c.purchaser,NULL)) AS repeated_purchaser,
  FORMAT_DATE("%Y",c.purchase_month) AS purchase_year,
  COUNT(DISTINCT IF(c.repeated_purchase !=0,c.repeated_purchase,NULL)) AS repeated_purchase,
  SUM(IF(c.repeated_purchase !=0,c.product_price,NULL)) AS repeated_revenue,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
GROUP BY month_of_registration,purchase_year,country_by_phone,category_name
)
, f1 AS ( -- посчитать количество повторно купленных товаров по категориям к количеству повторно купленных товаров без категорий по месяцам
SELECT DISTINCT
  b.month_of_registration,
  FORMAT_DATE("%Y_%m",c.purchase_month) AS repeated_category_month,
  COUNT(IF(c.repeated_purchase !=0,c.product_id,NULL)) OVER (PARTITION BY b.month_of_registration,c.purchase_month,c.category_name, b.country_by_phone) AS repeated_products_by_categories, --количество повторно купленных товаров по категориям
  COUNT(IF(c.repeated_purchase !=0,c.product_id,NULL)) OVER (PARTITION BY b.month_of_registration,c.purchase_month,b.country_by_phone) AS repeated_products_without_categories, -- количество повторно купленных товаров без категорий
  ROUND(SAFE_DIVIDE(COUNT(IF(c.repeated_purchase !=0,c.product_id,NULL)) OVER (PARTITION BY b.month_of_registration,c.purchase_month,c.category_name, b.country_by_phone) , COUNT(IF(c.repeated_purchase !=0,c.product_id,NULL)) OVER (PARTITION BY b.month_of_registration,c.purchase_month,b.country_by_phone)),4) AS repeated_purchases_products_proportion, -- отношение количества повторно купленных товаров по категориям к количеству повторно купленных товаров без категорий
  b.country_by_phone,
  c.category_name,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE 1=1  
AND repeated_purchase != 0  --посчитать только для повторных покупок
ORDER BY repeated_category_month,b.month_of_registration,b.country_by_phone ASC
)
,f2 AS ( -- получить рейтинг категорий по процентному соотношению повторно купленных товаров
SELECT
  month_of_registration,
  repeated_category_month,
  CAST(repeated_products_by_categories AS STRING) AS repeated_products_by_categories,
  CAST(repeated_products_without_categories AS STRING) AS repeated_products_without_categories,
  CAST(repeated_purchases_products_proportion AS STRING) AS  repeated_purchases_products_proportion, 
  country_by_phone,
  category_name,
  ROW_NUMBER() OVER (PARTITION BY month_of_registration,repeated_category_month,country_by_phone  ORDER BY  repeated_purchases_products_proportion DESC) AS num -- рейтинг категорий по процентному соотношению
FROM f1
QUALIFY num <=3 -- топ-3 категории
)
, f3 AS ( --cгруппировать значения для pivot
SELECT  
  month_of_registration,
  repeated_category_month,
  country_by_phone,
  category_name,
  repeated_purchases_products_proportion,
FROM f2
GROUP BY 1,2,3,4,5
)
, g1 AS ( --посчитать количество повторно купленных товаров по категориям к количеству повторно купленных товаров без категорий по годам
SELECT DISTINCT
  b.month_of_registration,
  FORMAT_DATE("%Y",c.purchase_month) AS repeated_category_year,
  b.country_by_phone,
  c.category_name,
  COUNT(IF(c.repeated_purchase !=0,c.product_id,NULL)) OVER (PARTITION BY b.month_of_registration,FORMAT_DATE("%Y",c.purchase_month),c.category_name, b.country_by_phone) AS repeated_products_by_categories, 
  COUNT(IF(c.repeated_purchase !=0,c.product_id,NULL)) OVER (PARTITION BY b.month_of_registration,FORMAT_DATE("%Y",c.purchase_month),b.country_by_phone) AS repeated_products_without_categories, 
  ROUND(SAFE_DIVIDE(COUNT(IF(c.repeated_purchase !=0,c.product_id,NULL)) OVER (PARTITION BY b.month_of_registration,FORMAT_DATE("%Y",c.purchase_month),c.category_name, b.country_by_phone) , COUNT(IF(c.repeated_purchase !=0,c.product_id,NULL)) OVER (PARTITION BY b.month_of_registration,FORMAT_DATE("%Y",c.purchase_month),b.country_by_phone)),4) AS repeated_purchases_products_proportion,
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
WHERE 1=1 
AND repeated_purchase != 0
ORDER BY repeated_category_year,b.month_of_registration,b.country_by_phone ASC
)
,g2 AS ( -- получить рейтинг категорий по процентному соотношению повторно купленных товаров за год
SELECT
  month_of_registration,
  repeated_category_year,
  CAST(repeated_products_by_categories AS STRING) AS repeated_products_by_categories,
  CAST(repeated_products_without_categories AS STRING) AS repeated_products_without_categories,
  CAST(repeated_purchases_products_proportion AS STRING) AS  repeated_purchases_products_proportion, 
  country_by_phone,
  category_name,
  ROW_NUMBER() OVER (PARTITION BY month_of_registration,repeated_category_year,country_by_phone  ORDER BY  repeated_purchases_products_proportion DESC) AS num -- рейтинг категорий по процентному соотношению
FROM g1
QUALIFY num <=3 -- топ-3 категории
)
, g3 AS (
SELECT  
  month_of_registration,
  repeated_category_year,
  country_by_phone,
  category_name,
  repeated_purchases_products_proportion,
FROM g2
GROUP BY 1,2,3,4,5
)
, h AS ( -- количество регистраций по месяцам
SELECT
  b.month_of_registration,
  b.country_by_phone,
  c.category_name,
  COUNT(DISTINCT registered_user) AS registered_user
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
GROUP BY month_of_registration,country_by_phone,category_name
)
, i AS ( -- покупки первого месяца
SELECT
  b.month_of_registration,
  b.country_by_phone,
  c.category_name,
  COUNT(DISTINCT IF(b.month_of_registration = c.purchase_month,purchase_id,NULL)) AS first_month_purchases, --количество покупок в первом месяце - месяце регистрации
FROM b LEFT JOIN c ON b.registered_user = c.purchaser 
GROUP BY month_of_registration,country_by_phone,category_name
)
, j1 AS ( --посчитать количество купленных товаров по категориям к количеству купленных товаров без категорий в первом месяце
SELECT DISTINCT
  b.month_of_registration,
  b.country_by_phone,
  c.category_name AS first_month_categories,
  COUNT(IF(b.month_of_registration = c.purchase_month,product_id,NULL)) OVER (PARTITION BY b.month_of_registration, c.purchase_month,c.category_name, b.country_by_phone) AS products_by_categories,
  COUNT(IF(b.month_of_registration = c.purchase_month,product_id,NULL)) OVER (PARTITION BY b.month_of_registration,c.purchase_month,b.country_by_phone) AS products_without_categories,
  ROUND(SAFE_DIVIDE(COUNT(IF(b.month_of_registration = c.purchase_month,product_id,NULL)) OVER (PARTITION BY b.month_of_registration, c.purchase_month,c.category_name, b.country_by_phone) , COUNT(IF(b.month_of_registration = c.purchase_month,product_id,NULL)) OVER (PARTITION BY b.month_of_registration,c.purchase_month,b.country_by_phone)),4) AS first_month_products_proportion
FROM b LEFT JOIN c ON b.registered_user = c.purchaser
)
, j2 AS ( -- получить рейтинг категорий по процентному соотношению купленных товаров в первом месяце
SELECT
  month_of_registration,
  first_month_categories,
  country_by_phone,
  CAST(products_by_categories AS STRING) AS products_by_categories,
  CAST(products_without_categories AS STRING) AS products_without_categories,
  CAST(first_month_products_proportion AS STRING) AS first_month_products_proportion, 
  ROW_NUMBER() OVER (PARTITION BY month_of_registration,country_by_phone  ORDER BY first_month_products_proportion DESC) AS num -- рейтинг категорий по процентному соотношению
FROM j1
QUALIFY num <=3 -- топ-3 категории
)
, k AS ( -- количество покупателей по месяцам регистрации
SELECT
  b.month_of_registration,
  b.country_by_phone,
  c.category_name,
  COUNT(DISTINCT IF(purchaser IS NOT NULL,purchaser,NULL)) AS purchaser_first_month
FROM b LEFT JOIN c ON b.registered_user = c.purchaser AND  b.month_of_registration = c.purchase_month
GROUP BY b.month_of_registration, b.country_by_phone, c.category_name
)

SELECT DISTINCT
  month_of_registration,
  country_by_phone,
  category_name,
  registered_user,
  purchaser_first_month,
  first_month_purchases,
  -- first_month_products_proportion,
  repeated_purchases_products_proportion,
  purchase_period,
  repeated_purchaser,
  repeated_purchase,
  repeated_revenue,
  -- RANK() OVER (PARTITION BY month_of_registration,country_by_phone, purchase_period ORDER BY  registered_user DESC) AS num
   FROM 
(SELECT -- посчитать покупки по месяцам
  month_of_registration,
  purchase_month AS purchase_period,
  country_by_phone,
  category_name,
  SUM(repeated_purchaser) AS repeated_purchaser,
  SUM(repeated_purchase) AS repeated_purchase,
  SUM(repeated_revenue) AS repeated_revenue
FROM d
WHERE purchase_month < '2024-01'-- считать по месяцам до 2024-01
GROUP BY  month_of_registration,purchase_month,country_by_phone,category_name

UNION ALL -- соединить сводные таблицы по месяцам, по годам с покупками первого месяца 

SELECT -- посчитать покупки по годам
  month_of_registration,
  purchase_year AS purchase_period,
  country_by_phone,
  category_name,
  SUM(repeated_purchaser) AS repeated_purchaser,
  SUM(repeated_purchase) AS repeated_purchase, 
  SUM(repeated_revenue) AS repeated_revenue
FROM e
WHERE purchase_year > '2023' --считать с 2024 года
GROUP BY  month_of_registration,purchase_year,country_by_phone,category_name
)

FULL JOIN 

(
  SELECT
  month_of_registration,
  repeated_category_month AS purchase_period,
  country_by_phone,
  category_name,
  repeated_purchases_products_proportion
FROM f3
WHERE repeated_category_month < '2024-01'

UNION ALL

SELECT
  month_of_registration,
  repeated_category_year AS purchase_period,
  country_by_phone,
  category_name,
  repeated_purchases_products_proportion
FROM g3
WHERE repeated_category_year > '2023'
) 
USING (month_of_registration, purchase_period, country_by_phone,category_name)

FULL JOIN

(SELECT
  month_of_registration,
  country_by_phone,
  category_name,
  first_month_purchases, --количество покупок в первом месяце - месяце регистрации
FROM i)
USING (month_of_registration,country_by_phone,category_name)

FULL JOIN

(
SELECT
  month_of_registration,
  country_by_phone,
  category_name,
  registered_user
FROM h)
USING (month_of_registration,country_by_phone,category_name)

FULL JOIN

(SELECT
  month_of_registration,
  country_by_phone,
  first_month_categories AS category_name,
  first_month_products_proportion
FROM j2)
USING (month_of_registration,country_by_phone,category_name)

FULL JOIN

(SELECT
  month_of_registration,
  country_by_phone,
  category_name,
  purchaser_first_month
FROM k)
USING (month_of_registration,country_by_phone,category_name)
-- QUALIFY num <= 3
ORDER BY month_of_registration, country_by_phone, category_name, purchase_period ASC