--количество заказов в ноябре по активным магазинам общее
WITH a AS ( --получить транзакции по магазинам
SELECT  
  shop_id,
  COUNT(DISTINCT id) AS transactions
FROM `funnel-flowwow.MYSQL_EXPORT.f_order_archive`
WHERE status = 3 AND paid = 1 AND DATE(create_at) BETWEEN '2023-11-01' AND '2023-11-30'
AND shop_id IN (657,12528,110413,27234,109339,46227,38178,97503,148869,16396) 
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
, c AS ( -- получить список активных магазинов на последнюю дату в ноябре
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
)
SELECT
  d.shop_id,
  d.name,
  l.city,
  l.region,
  d.transactions
FROM `funnel-flowwow.MYSQL_EXPORT.city_all` l
RIGHT JOIN d ON l.city_id = d.city_id 
ORDER BY transactions DESC