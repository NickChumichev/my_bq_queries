--средняя стоимость доставки
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
, c AS ( -- получить список активных магазинов на последнюю дату в ноябре
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
, e AS ( --получить последнюю стоимость по зонам доставки для активных магазинов 
SELECT DISTINCT
  e.shop_id,
  d.name,
  d.city_id,
  e.radius_from, -- радиус отправки
  e.radius_to,--радиус доставки
  LAST_VALUE(e.delivery_cost) OVER(PARTITION BY e.shop_id, e.radius_to  ORDER BY e.updated_at rows between unbounded preceding and unbounded following) AS last_delivery_cost, -- последняя стоимость доставки
  d.transactions
 FROM `funnel-flowwow.Analyt_ChumichevN.DATA-3202_f_shop_delivery_zone`e
 RIGHT JOIN d ON d.shop_id = e.shop_id  
 WHERE 1=1
 AND e.currency = 'RUB' 
 GROUP BY e.shop_id,city_id,name,delivery_cost,shop_zone_id,updated_at,radius_from,radius_to,transactions
)
, f AS ( 
SELECT DISTINCT -- распределить  доставки на зоны,км
  shop_id,
  name,
  city_id,
  radius_from,
  IF(radius_to BETWEEN 0 AND 4,last_delivery_cost,NULL) AS delivery_cost_0_4,
  IF(radius_to BETWEEN 5 AND 10,last_delivery_cost,NULL) AS delivery_cost_5_10,
  IF(radius_to BETWEEN 11 AND 30,last_delivery_cost,NULL) AS delivery_cost_11_30,
  IF(radius_to BETWEEN 31 AND 50,last_delivery_cost,NULL) AS delivery_cost_31_50,
  IF(radius_to BETWEEN 51 AND 70,last_delivery_cost,NULL) AS delivery_cost_51_70,
  IF(radius_to BETWEEN 71 AND 90,last_delivery_cost,NULL) AS delivery_cost_71_90,
  IF(radius_to BETWEEN 91 AND 110,last_delivery_cost,NULL) AS delivery_cost_91_110,
  IF(radius_to BETWEEN 111 AND 130,last_delivery_cost,NULL) AS delivery_cost_111_130, 
  IF(radius_to BETWEEN 131 AND 150,last_delivery_cost,NULL) AS delivery_cost_131_150,
  IF(radius_to BETWEEN 151 AND 170,last_delivery_cost,NULL) AS delivery_cost_151_170,
  IF(radius_to BETWEEN 171 AND 190,last_delivery_cost,NULL) AS delivery_cost_171_190,
  IF(radius_to BETWEEN 191 AND 255,last_delivery_cost,NULL) AS delivery_cost_191_255,
  transactions
FROM e
GROUP BY shop_id,name,city_id,radius_from,radius_to,last_delivery_cost,transactions
)
SELECT DISTINCT
  f.shop_id,
  f.name,
  l.city,
  l.region,
  PERCENTILE_CONT(delivery_cost_0_4,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_0_4_km,
  PERCENTILE_CONT(delivery_cost_5_10,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_5_10_km,
  PERCENTILE_CONT(delivery_cost_11_30,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_11_30_km,
  PERCENTILE_CONT(delivery_cost_31_50,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_31_50_km,
  PERCENTILE_CONT(delivery_cost_51_70,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_51_70_km,
  PERCENTILE_CONT(delivery_cost_71_90,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_71_90_km,
  PERCENTILE_CONT(delivery_cost_91_110,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_91_110_km,
  PERCENTILE_CONT(delivery_cost_111_130,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_111_130_km,
  PERCENTILE_CONT(delivery_cost_131_150,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_131_150_km,
  PERCENTILE_CONT(delivery_cost_151_170,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_151_170_km,
  PERCENTILE_CONT(delivery_cost_171_190,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_171_190_km,
  PERCENTILE_CONT(delivery_cost_191_255,0.5) OVER (PARTITION BY radius_from, shop_id) AS median_191_255_km,
  transactions
  FROM f LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.city_all` l ON f.city_id = l.city_id
  ORDER BY transactions DESC