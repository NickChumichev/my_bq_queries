--количество товаров со скидкой у активных магазинов на текущую дату
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
, e AS ( -- количество активных товаров со скидкой
SELECT 
  t.shop_id,
  d.name,
  d.city_id,
  COUNT(DISTINCT t.id) AS cnt_products,
  d.transactions
FROM `funnel-flowwow.MYSQL_EXPORT.f_product` t
RIGHT JOIN d ON t.shop_id = d.shop_id
WHERE 1=1
AND t.discount != 0 -- есть скидка 
AND t.approved = 1 
AND t.status = 1
GROUP BY 1,2,3,transactions
)
SELECT
  shop_id,
  name,
  l.city,
  l.region,
  cnt_products AS discount_products,--количество товаров со скидкой
  transactions
FROM e LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.city_all` l ON e.city_id = l.city_id
ORDER BY transactions DESC
LIMIT 10