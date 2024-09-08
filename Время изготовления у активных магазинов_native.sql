--медианное время изготовления товаров у активных магазинов
WITH a AS ( --получить количество транзакций по магазинам
SELECT  
  shop_id,
  COUNT(DISTINCT id) AS transactions
FROM `funnel-flowwow.MYSQL_EXPORT.f_order_archive`
WHERE status = 3 AND paid = 1 AND DATE(create_at) BETWEEN '2023-01-01' AND CURRENT_DATE() 
GROUP BY shop_id
ORDER BY transactions DESC
)
, b AS (
SELECT
  id AS shop_id,
  name,
  city_id,
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop` 
WHERE id NOT IN (SELECT shop_id FROM `funnel-flowwow.BUSINESS_DM.ii_fmart_actual_shops` 
WHERE DATE(max_end_date) >= '2023-01-01') --убрать магазины fmart
AND id IN (SELECT shop_id FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_main_assortment` WHERE range_group_id = 1) -- категория цветы и подарки
)
, c AS ( -- получить список активных магазинов на последнюю дату
SELECT
  created_at,
  shop_id,
  active,
  ROW_NUMBER() OVER (PARTITION BY shop_id ORDER BY created_at DESC) AS row 
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_active_log` 
GROUP BY 1,2,3
)
, d AS (
SELECT
  b.shop_id,
  b.name,
  b.city_id,
  a.transactions
FROM b INNER JOIN c ON b.shop_id = c.shop_id
INNER JOIN a ON a.shop_id = b.shop_id 
WHERE row = 1 AND active = 1
ORDER BY  a.transactions DESC
LIMIT 10
)
, e AS ( --посчитать  среднее времени изготовления
SELECT DISTINCT
  d.shop_id,
  d.name,
  d.city_id,  
  SAFE_DIVIDE(SUM(t.production_time), COUNT(id)) AS avg_production_time_minutes,
  d.transactions
FROM `funnel-flowwow.MYSQL_EXPORT.f_product` t
RIGHT JOIN d ON t.shop_id = d.shop_id
WHERE 1=1
  AND t.production_time !=0
  AND t.approved = 1 
  AND t.status = 1
GROUP BY shop_id,name,city_id,transactions
)
, f AS ( 
SELECT
  shop_id,
  PERCENTILE_CONT(production_time,0.5) OVER (PARTITION BY shop_id) AS median_production_time_minutes,
  ROW_NUMBER() OVER (PARTITION BY shop_id) AS row
FROM `funnel-flowwow.MYSQL_EXPORT.f_product`
WHERE approved = 1 
AND status = 1
AND production_time !=0
)
, g AS ( --посчитать  медиану времени изготовления
SELECT DISTINCT
  shop_id,
  median_production_time_minutes,
  row
FROM f 
)
SELECT DISTINCT
  e.shop_id,
  e.name,
  l.city,
  l.region,
  g.median_production_time_minutes,
  e.avg_production_time_minutes,
  e.transactions
FROM e LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.city_all` l ON e.city_id = l.city_id
LEFT JOIN g ON g.shop_id = e.shop_id
ORDER BY transactions DESC