WITH a AS ( --получить количество транзакций по магазинам
SELECT  
  shop_id,
  COUNT(DISTINCT id) AS transactions
FROM `funnel-flowwow.MYSQL_EXPORT.f_order_archive`
WHERE status = 3 AND paid = 1 AND DATE(create_at) BETWEEN '2023-06-01' AND CURRENT_DATE()-1 
GROUP BY shop_id
ORDER BY transactions DESC
)
, b AS ( --список кондитерских
SELECT
  id AS shop_id,
  name,
  translit,
  url,
  city_id,
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop` 
WHERE id IN (SELECT shop_id FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_main_assortment` WHERE range_group_id = 5) -- категория кондитерка
AND id NOT IN (SELECT shop_id FROM `funnel-flowwow.BUSINESS_DM.ii_fmart_actual_shops` ) --убрать магазины fmart
AND translit NOT IN ('tehpodderzhka') 
)
SELECT
  b.shop_id,
  b.name,
  b.translit,
  b.url,
  l.city,
  l.region,
  a.transactions
FROM b LEFT JOIN a ON a.shop_id = b.shop_id 
LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.city_all` l ON b.city_id = l.city_id
WHERE l.city_id IN (1,428,1463,6181,21856)
ORDER BY  a.transactions DESC
