--количество товаров у магазинов
WITH a AS ( --список цветочных магазинов без fmart 
SELECT
  id AS shop_id,
  name,
  city_id,
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop` 
WHERE id NOT IN (SELECT shop_id FROM `funnel-flowwow.BUSINESS_DM.ii_fmart_actual_shops` 
WHERE DATE(max_end_date) >= '2023-01-01') --убрать магазины fmart
AND id IN (SELECT shop_id FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_main_assortment` WHERE range_group_id = 1) -- категория цветы и подарки
)
, b AS ( -- получить список активных магазинов на последнюю дату
SELECT
  created_at,
  shop_id,
  active,
  ROW_NUMBER() OVER (PARTITION BY shop_id ORDER BY created_at DESC) AS row 
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_active_log` 
GROUP BY 1,2,3
)
, c AS (
SELECT
  a.shop_id,
  a.name,
  a.city_id
FROM a INNER JOIN b ON a.shop_id = b.shop_id 
WHERE row = 1 AND active = 1
)
, d AS ( --получить количество активных товаров на последнюю дату
SELECT
  shop_id,
  created_at, --дата обновления количества товаров
  count,
  ROW_NUMBER() OVER (PARTITION BY shop_id ORDER BY created_at DESC) AS row
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_product_count_log`
GROUP BY created_at, count, shop_id
)
SELECT --получить количество активных товаров на последнюю дату
  c.shop_id,
  c.name,
  l.city,
  l.region,
  d.created_at, --дата обновления количества товаров
  d.count AS cnt_active_products, -- количество активных товаров 
FROM c LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.city_all` l ON c.city_id = l.city_id
LEFT JOIN d ON c.shop_id = d.shop_id 
WHERE DATE(d.created_at) >= '2023-01-01' AND d.row = 1
-- GROUP BY 1,2,3,4,5
ORDER BY cnt_active_products DESC
LIMIT 10

