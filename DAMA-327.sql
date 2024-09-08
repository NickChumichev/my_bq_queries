WITH a AS ( --получить количество транзакций по магазинам
SELECT  
  shop_id,
  COUNT(DISTINCT purchase_id) AS transactions,
  SUM(product_price) AS revenue
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
WHERE DATE(purchase_timestamp) BETWEEN '2023-12-01' AND '2024-06-12'
GROUP BY shop_id
ORDER BY transactions DESC
)
, b AS ( --список цветочных
SELECT
  id AS shop_id,
  name,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(address,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS address,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(email,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS email,
  translit,
  city_id,
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop` 
WHERE id IN (SELECT shop_id FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_main_assortment` WHERE range_group_id = 1) -- категория цветы
AND id NOT IN (SELECT shop_id FROM `funnel-flowwow.BUSINESS_DM.ii_fmart_actual_shops` ) --убрать магазины fmart
AND translit NOT IN ('tehpodderzhka') 
)
SELECT
  b.shop_id,
  b.name,
  b.translit,
  b.phone,
  b.email,
  b.address,
  l.city,
  l.region,
  a.transactions,
  a.revenue
FROM b LEFT JOIN a ON a.shop_id = b.shop_id 
LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.city_all` l ON b.city_id = l.city_id
WHERE l.city_id IN (2,49,72)
ORDER BY  a.transactions DESC

