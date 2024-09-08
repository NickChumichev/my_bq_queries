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
  IF(category_name IS NULL,'no_category',category_name) AS category_name,
  product_price
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
)
, d AS ( --соединить покупки по месяцам и категориям с регистрациями
SELECT
  _b.month_of_registration,
  _b.country_by_phone,
  _c.category_name,
  _c.purchase_month AS purchase_month,
  COUNT(DISTINCT IF(_c.repeated_purchase !=0 AND _b.month_of_registration != _c.purchase_month,_c.purchaser,NULL)) AS repeated_purchaser, --определить повторного покупателя по повторной покупке и где месяц регистрации не совпадает с месяцем покупки 
  COUNT(DISTINCT IF(_c.first_purchase !=0,_c.first_purchase,NULL)) AS first_purchase,
  COUNT(DISTINCT IF(_c.repeated_purchase !=0,_c.repeated_purchase,NULL)) AS repeated_purchase, -- посчитать уникальные повторные покупки, где нет 0
  SUM(IF(_c.repeated_purchase !=0,_c.product_price,NULL)) AS repeated_revenue, -- доход с повторных покупок
FROM b AS _b
LEFT JOIN c AS _c
ON _b.registered_user = _c.purchaser 
GROUP BY purchase_month,country_by_phone,month_of_registration,category_name
ORDER BY month_of_registration, country_by_phone ASC
)
, d1 AS ( --соединить покупки по месяцам с регистрациями
SELECT
  _b1.month_of_registration,
  _b1.country_by_phone,
  _c1.purchase_month AS purchase_month,
  COUNT(DISTINCT IF(_c1.first_purchase !=0,_c1.first_purchase,NULL)) AS  first_purchase_without_categories,
  COUNT(DISTINCT IF(_c1.repeated_purchase !=0 AND _b1.month_of_registration != _c1.purchase_month,_c1.purchaser,NULL)) AS repeated_purchaser_without_categories,
  COUNT(DISTINCT IF(_c1.repeated_purchase !=0,_c1.repeated_purchase,NULL)) AS  repeated_purchase_without_categories, 
FROM b AS _b1
LEFT JOIN c AS _c1
ON _b1.registered_user = _c1.purchaser 
GROUP BY purchase_month,country_by_phone,month_of_registration
ORDER BY month_of_registration, country_by_phone ASC
)
, d2 AS ( --посчитать повторные покупки и покупателей без дублей
SELECT
  _d.month_of_registration,
  _d.country_by_phone,
  _d.category_name,
  _d.purchase_month,
  SAFE_DIVIDE(_d.repeated_purchaser , SUM(_d.repeated_purchaser) OVER (PARTITION BY _d.month_of_registration,_d.purchase_month, _d.country_by_phone)) AS repeated_purchaser_proportion,
  _d1.repeated_purchaser_without_categories * SAFE_DIVIDE(_d.repeated_purchaser , SUM(_d.repeated_purchaser) OVER (PARTITION BY _d.month_of_registration,_d.purchase_month, _d.country_by_phone)) AS repeated_purchaser,
  SAFE_DIVIDE(_d.repeated_purchase , SUM(_d.repeated_purchase) OVER (PARTITION BY _d.month_of_registration,_d.purchase_month, _d.country_by_phone)) AS repeated_purchase_proportion,
  _d1.repeated_purchase_without_categories * SAFE_DIVIDE(_d.repeated_purchase , SUM(_d.repeated_purchase) OVER (PARTITION BY _d.month_of_registration,_d.purchase_month, _d.country_by_phone)) AS repeated_purchase,
  _d1.first_purchase_without_categories * SAFE_DIVIDE(_d.first_purchase , SUM(_d.first_purchase) OVER (PARTITION BY _d.month_of_registration,_d.purchase_month, _d.country_by_phone)) AS first_purchase,
  _d.repeated_revenue
FROM d AS _d
LEFT JOIN d1 AS _d1
USING(month_of_registration,country_by_phone,purchase_month)  
ORDER BY month_of_registration, country_by_phone ASC
)
, f1 AS ( -- посчитать количество повторно купленных товаров по категориям к количеству повторно купленных товаров без категорий по месяцам
SELECT DISTINCT
  _b3.month_of_registration,
  _c3.purchase_month AS repeated_category_month,
  COUNT(IF(_c3.repeated_purchase !=0,_c3.product_id,NULL)) OVER (PARTITION BY _b3.month_of_registration,_c3.purchase_month,_c3.category_name, _b3.country_by_phone) AS repeated_products_by_categories, --количество повторно купленных товаров по категориям
  COUNT(IF(_c3.repeated_purchase !=0,_c3.product_id,NULL)) OVER (PARTITION BY _b3.month_of_registration,_c3.purchase_month,_b3.country_by_phone) AS repeated_products_without_categories, -- количество повторно купленных товаров без категорий
  ROUND(SAFE_DIVIDE(COUNT(IF(_c3.repeated_purchase !=0,_c3.product_id,NULL)) OVER (PARTITION BY _b3.month_of_registration,_c3.purchase_month,_c3.category_name, _b3.country_by_phone) , COUNT(IF(_c3.repeated_purchase !=0,_c3.product_id,NULL)) OVER (PARTITION BY _b3.month_of_registration,_c3.purchase_month,_b3.country_by_phone)),4) AS repeated_purchases_products_proportion, -- отношение количества повторно купленных товаров по категориям к количеству повторно купленных товаров без категорий
  _b3.country_by_phone,
  _c3.category_name,
FROM b AS _b3
LEFT JOIN c AS _c3
ON _b3.registered_user = _c3.purchaser 
WHERE 1=1  
AND _c3.repeated_purchase != 0  --посчитать только для повторных покупок
ORDER BY repeated_category_month,_b3.month_of_registration,_b3.country_by_phone ASC
)
,f2 AS ( -- получить рейтинг категорий по процентному соотношению повторно купленных товаров
SELECT
  _f1.month_of_registration,
  _f1.repeated_category_month,
  CAST(_f1.repeated_products_by_categories AS STRING) AS repeated_products_by_categories,
  CAST(_f1.repeated_products_without_categories AS STRING) AS repeated_products_without_categories,
  _f1.repeated_purchases_products_proportion, 
  _f1.country_by_phone,
  _f1.category_name,
  ROW_NUMBER() OVER (PARTITION BY _f1.month_of_registration,_f1.repeated_category_month,_f1.country_by_phone  ORDER BY  _f1.repeated_purchases_products_proportion DESC) AS num -- рейтинг категорий по процентному соотношению
FROM f1 AS _f1
)
, f3 AS ( --cгруппировать значения
SELECT  
  _f2.month_of_registration,
  _f2.repeated_category_month,
  _f2.country_by_phone,
  _f2.category_name,
  _f2.repeated_purchases_products_proportion
FROM f2 AS _f2
GROUP BY month_of_registration,repeated_category_month,country_by_phone,category_name,repeated_purchases_products_proportion
)
, h AS ( -- количество регистраций по месяцам и категориям
SELECT
  _b4.month_of_registration,
  _b4.country_by_phone,
  _c4.category_name,
  COUNT(DISTINCT _b4.registered_user) AS registered_user
FROM b AS _b4
LEFT JOIN c AS _c4
ON _b4.registered_user = _c4.purchaser 
GROUP BY month_of_registration,country_by_phone,category_name
)
, h1 AS ( -- количество регистраций по месяцам и категориям
SELECT
  _b5.month_of_registration,
  _b5.country_by_phone,
  COUNT(DISTINCT _b5.registered_user) AS registered_user
FROM b AS _b5
LEFT JOIN c AS _c5
ON _b5.registered_user = _c5.purchaser 
GROUP BY month_of_registration,country_by_phone
)
, h2 AS ( -- посчитать регистрации без дублей
SELECT
  _h.month_of_registration,
  _h.country_by_phone,
  _h.category_name,
  SAFE_DIVIDE(_h.registered_user , SUM(_h.registered_user) OVER (PARTITION BY _h.month_of_registration,_h.country_by_phone)) AS registered_user_proportion,
  _h1.registered_user * SAFE_DIVIDE(_h.registered_user , SUM(_h.registered_user) OVER (PARTITION BY _h.month_of_registration,_h.country_by_phone)) AS registered_user,
FROM h AS _h
LEFT JOIN h1 AS _h1
USING(month_of_registration,country_by_phone)  
ORDER BY month_of_registration, country_by_phone ASC
)
, i AS ( -- покупки первого месяца по категориям
SELECT
  _b6.month_of_registration,
  _b6.country_by_phone,
  _c6.category_name,
  COUNT(DISTINCT IF(_b6.month_of_registration = _c6.purchase_month, _c6.purchase_id,NULL)) AS first_month_purchases, --количество покупок в первом месяце - месяце регистрации
FROM b AS _b6
LEFT JOIN c AS _c6
ON _b6.registered_user = _c6.purchaser 
GROUP BY month_of_registration,country_by_phone,category_name
)
, i1 AS ( -- покупки первого месяца
SELECT
  _b7.month_of_registration,
  _b7.country_by_phone,
  COUNT(DISTINCT IF(_b7.month_of_registration = _c7.purchase_month, _c7.purchase_id,NULL)) AS first_month_purchases, --количество покупок в первом месяце - месяце регистрации
FROM b AS _b7
LEFT JOIN c AS _c7
ON _b7.registered_user = _c7.purchaser 
GROUP BY month_of_registration,country_by_phone
)
, i2 AS ( -- покупки первого месяца
SELECT
  _i.month_of_registration,
  _i.country_by_phone,
  _i.category_name,
  SAFE_DIVIDE(_i.first_month_purchases , SUM( _i.first_month_purchases) OVER (PARTITION BY  _i.first_month_purchases, _i.first_month_purchases)) AS first_month_purchases_proportion,
  _i1.first_month_purchases * SAFE_DIVIDE(_i.first_month_purchases , SUM(_i.first_month_purchases) OVER (PARTITION BY _i.month_of_registration, _i.country_by_phone)) AS first_month_purchases,
FROM i AS _i
LEFT JOIN i1 AS _i1
USING(month_of_registration,country_by_phone)  
ORDER BY month_of_registration, country_by_phone ASC
)
, j1 AS ( --посчитать количество купленных товаров по категориям к количеству купленных товаров без категорий в первом месяце
SELECT DISTINCT
  _b8.month_of_registration,
  _b8.country_by_phone,
  _c8.category_name AS first_month_categories,
  COUNT(IF(_b8.month_of_registration = _c8.purchase_month, _c8.product_id,NULL)) OVER (PARTITION BY _b8.month_of_registration, _c8.purchase_month, _c8.category_name, _b8.country_by_phone) AS products_by_categories,
  COUNT(IF(_b8.month_of_registration = _c8.purchase_month, _c8.product_id,NULL)) OVER (PARTITION BY _b8.month_of_registration, _c8.purchase_month, _b8.country_by_phone) AS products_without_categories,
  ROUND(SAFE_DIVIDE(COUNT(IF(_b8.month_of_registration = _c8.purchase_month, _c8.product_id,NULL)) OVER (PARTITION BY _b8.month_of_registration, _c8.purchase_month, _c8.category_name, _b8.country_by_phone) , COUNT(IF(_b8.month_of_registration = _c8.purchase_month, _c8.product_id,NULL)) OVER (PARTITION BY _b8.month_of_registration, _c8.purchase_month, _b8.country_by_phone)),4) AS first_month_products_proportion
FROM b AS _b8
LEFT JOIN c AS _c8
ON _b8.registered_user = _c8.purchaser
)
, j2 AS ( -- получить рейтинг категорий по процентному соотношению купленных товаров в первом месяце
SELECT
  month_of_registration,
  first_month_categories,
  country_by_phone,
  CAST(products_by_categories AS STRING) AS products_by_categories,
  CAST(products_without_categories AS STRING) AS products_without_categories,
  first_month_products_proportion, 
  ROW_NUMBER() OVER (PARTITION BY month_of_registration,country_by_phone  ORDER BY first_month_products_proportion DESC) AS num -- рейтинг категорий по процентному соотношению
FROM j1
)
, k AS ( -- количество покупателей по месяцам регистрации и категориям
SELECT
  _b9.month_of_registration,
  _b9.country_by_phone,
  _c9.category_name,
  COUNT(DISTINCT IF(_c9.purchaser IS NOT NULL, _c9.purchaser,NULL)) AS purchaser_first_month
FROM b AS _b9
LEFT JOIN c AS _c9
ON _b9.registered_user = _c9.purchaser AND  _b9.month_of_registration = _c9.purchase_month
GROUP BY _b9.month_of_registration, _b9.country_by_phone, _c9.category_name
)
, k1 AS ( -- количество покупателей по месяцам регистрации
SELECT
  _b10.month_of_registration,
  _b10.country_by_phone,
  COUNT(DISTINCT IF(_c10.purchaser IS NOT NULL, _c10.purchaser,NULL)) AS purchaser_first_month
FROM b AS _b10
LEFT JOIN c AS _c10
ON _b10.registered_user = _c10.purchaser AND  _b10.month_of_registration = _c10.purchase_month
GROUP BY _b10.month_of_registration, _b10.country_by_phone
)
, k2 AS ( -- количество покупателей по месяцам регистрации без дублей
SELECT
  _k.month_of_registration,
  _k.country_by_phone,
  _k.category_name,
  SAFE_DIVIDE(_k.purchaser_first_month , SUM(_k.purchaser_first_month) OVER (PARTITION BY _k.month_of_registration, _k.country_by_phone)) AS purchaser_first_month_proportion,
  _k1.purchaser_first_month * SAFE_DIVIDE(_k.purchaser_first_month , SUM(_k.purchaser_first_month) OVER (PARTITION BY _k.month_of_registration, _k.country_by_phone)) AS purchaser_first_month,
FROM k AS _k
LEFT JOIN k1 AS _k1
USING(month_of_registration,country_by_phone)
ORDER BY month_of_registration, country_by_phone ASC
),
sub_2 AS(
   SELECT
   month_of_registration,
   repeated_category_month AS purchase_period,
   country_by_phone,
   category_name,
   repeated_purchases_products_proportion
 FROM f3
),
sub_3 AS (SELECT
   month_of_registration,
   country_by_phone,
   category_name,
   first_month_purchases, --количество покупок в первом месяце - месяце регистрации
 FROM i2),
sub_4 AS (
 SELECT
   month_of_registration,
   country_by_phone,
   category_name,
   registered_user
 FROM h2
 WHERE month_of_registration = '2023-04-01'
AND country_by_phone = 'Россия'
),
sub_5 AS (SELECT
   month_of_registration,
   country_by_phone,
   first_month_categories AS category_name,
   first_month_products_proportion
 FROM j2
 ),
sub_6 AS (SELECT
   month_of_registration,
   country_by_phone,
   category_name,
   purchaser_first_month
 FROM k2)


SELECT DISTINCT
  COALESCE(
    sub_1.month_of_registration,
    -- _sub_2.month_of_registration,
    -- _sub_3.month_of_registration,
    _sub_4.month_of_registration
    -- _sub_5.month_of_registration,
    -- _sub_6.month_of_registration
  ) AS month_of_registration,
  COALESCE(
    sub_1.country_by_phone,
    -- _sub_2.country_by_phone,
    -- _sub_3.country_by_phone,
    _sub_4.country_by_phone
    -- _sub_5.country_by_phone,
    -- _sub_6.country_by_phone
  ) AS country_by_phone,
  COALESCE(
    sub_1.category_name,
    -- _sub_2.category_name,
    -- _sub_3.category_name,
    _sub_4.category_name
    -- _sub_5.category_name,
    -- _sub_6.category_name
  ) AS category_name,
  _sub_4.registered_user,
  -- _sub_6.purchaser_first_month,
  -- _sub_3.first_month_purchases, --покупки в первом месяце
  -- sub_1.first_purchase AS first_purchases,
  -- _sub_2.repeated_purchases_products_proportion,
  COALESCE(
    sub_1.purchase_period
    -- _sub_2.purchase_period
  ) AS purchase_period,
  -- sub_1.repeated_purchaser,
  -- sub_1.repeated_purchase AS repeated_purchases,
  -- sub_1.repeated_revenue
FROM 
(SELECT -- посчитать покупки по месяцам
  month_of_registration,
  purchase_month AS purchase_period,
  country_by_phone,
  category_name,
  SUM(first_purchase) AS first_purchase,
  SUM(repeated_purchaser) AS repeated_purchaser,
  SUM(repeated_purchase) AS repeated_purchase,
  SUM(repeated_revenue) AS repeated_revenue
FROM d2
WHERE month_of_registration = '2023-04-01'
AND purchase_month = '2023-05-01'
AND country_by_phone = 'Россия'
GROUP BY  month_of_registration,purchase_month,country_by_phone,category_name
) AS sub_1

-- FULL JOIN sub_2 AS _sub_2 
-- USING (month_of_registration, purchase_period, country_by_phone,category_name)

-- FULL JOIN sub_3 AS _sub_3
-- USING (month_of_registration,country_by_phone,category_name)

FULL JOIN sub_4 AS _sub_4
USING (month_of_registration,country_by_phone,category_name)

-- FULL JOIN sub_5 AS _sub_5
-- USING (month_of_registration,country_by_phone,category_name)

-- FULL JOIN sub_6 AS _sub_6
-- USING (month_of_registration,country_by_phone,category_name)
-- ORDER BY month_of_registration, country_by_phone, category_name, purchase_period ASC