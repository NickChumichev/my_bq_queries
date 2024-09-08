WITH a AS ( --получить магазины
SELECT
  id,
  name,
  url,
  city_id
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop` 
WHERE url NOT IN ('') AND url NOT IN ('tema')  
)
SELECT --получить названия городов
  a.id,
  a.name,
  u.legal_name,
  a.url,
  l.city,
  l.region,
FROM a INNER JOIN `funnel-flowwow.MYSQL_EXPORT.city_all` l ON a.city_id = l.city_id
INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_shop_main_assortment` t ON a.id = t.shop_id 
INNER JOIN `Analyt_ChumichevN.f_holding_legal_ru` u ON a.id = u.holding_id
WHERE 1=1
AND l.city_id IN (1,428,1463,6181,21856) -- добавил города, которые включены в мск
AND t.range_group_id = 1