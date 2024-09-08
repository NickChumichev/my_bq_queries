--количество товаров у активных магазинов из топ-10 по количеству транзакций
WITH a AS ( --получить количество транзакций по магазинам
SELECT  
  shop_id,
  COUNT(DISTINCT id) AS transactions
FROM `funnel-flowwow.MYSQL_EXPORT.f_order_archive`
WHERE status = 3 AND paid = 1 AND DATE(create_at) BETWEEN '2023-01-01' AND CURRENT_DATE() 
GROUP BY shop_id
ORDER BY transactions DESC
)
, b AS ( --список цветочных магазинов без fmart 
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
, d AS ( --получить топ-10 активных магазинов без fmart
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
, e AS ( --получить количество активных товаров на последнюю дату
SELECT
  shop_id,
  created_at, --дата обновления количества товаров
  count,
  ROW_NUMBER() OVER (PARTITION BY shop_id ORDER BY created_at DESC) AS row
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_product_count_log`
GROUP BY created_at, count, shop_id
)
SELECT --получить количество активных товаров на последнюю дату
  d.shop_id,
  d.name,
  l.city,
  l.region,
  e.created_at, --дата обновления количества товаров
  e.count AS active_products, -- количество активных товаров
  d.transactions 
FROM d LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.city_all` l ON d.city_id = l.city_id
LEFT JOIN e ON d.shop_id = e.shop_id 
WHERE DATE(e.created_at) >= '2023-01-01' AND e.row = 1
ORDER BY transactions DESC