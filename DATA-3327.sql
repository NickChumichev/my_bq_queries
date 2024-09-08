WITH a AS (--получить платформу и промокод для инфлюенсеров из РФ
SELECT
  LOWER(personal_promocode) AS promocode,
  PARSE_DATE('%m/%d/%Y', date) AS date_promo,
  ROW_NUMBER() OVER (PARTITION BY LOWER(personal_promocode) ORDER BY PARSE_DATE('%m/%d/%Y', date) ASC) AS row,
FROM `funnel-flowwow.BUSINESS_DM.influencers_rf_gs_view`
)
, b AS (
SELECT
  promocode,
  date_promo,
  row
FROM a
WHERE row = 1 --начало промоактивности
)
,c AS ( -- посчитать количество покупок с промокодом
SELECT
  DATE(purchase_timestamp) as date,
  LOWER(promocode) as promocode,
  user_id, --покупатель
  purchase_id,
  product_price as revenue
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  )
, d AS ( --найти покупки по промокодам
SELECT
DATE_TRUNC(date_promo,MONTH) AS month,
promocode,
COUNT(DISTINCT user_id) as users,
SUM(revenue) as revenue,
FROM (
SELECT DISTINCT
  c.date, 
  b.date_promo,
  c.promocode,
  c.purchase_id,
  c.user_id,   
  SUM(c.revenue) AS revenue,
  DATE_DIFF(c.date,b.date_promo, DAY) as d,
  ROW_NUMBER() OVER (PARTITION BY c.purchase_id,c.user_id ORDER BY c.date) AS row
FROM b
INNER JOIN c USING(promocode)
WHERE DATE_DIFF(date,date_promo, DAY)>=0 -- дата покупки по промокоду и дата выпуска промокода не совпадают
GROUP BY 1,2,3,4,5
)
GROUP BY 1,2
ORDER BY month ASC
)
, e AS ( -- платформы и промокоды по месяцам
SELECT
  LOWER(platform) AS platform, 
  LOWER(personal_promocode) AS promocode,
  DATE_TRUNC(PARSE_DATE('%m/%d/%Y', date),MONTH) AS month_promo,
  cost,
  audience  --охват
FROM `funnel-flowwow.BUSINESS_DM.influencers_rf_gs_view`
)
, f AS ( --соединить данные по покупкам с расходом и охватом по промокодам
SELECT 
  e.month_promo,
  e.platform,
  e.promocode,
  SUM(d.users) AS users,
  SUM(d.revenue) AS revenue,
  SUM(CAST(e.cost AS numeric)) AS cost,
  SUM(CAST(e.audience AS numeric)) AS audience, 
FROM d FULL JOIN e ON 
d.month = e.month_promo 
AND d.promocode = e.promocode
GROUP BY 1,2,3
-- ORDER BY month DESC
)
, g AS ( --посчитать количество по полю users в разрезе месяца и платформы
SELECT
  month_promo,
  platform,
  users,
  revenue,
  cost,
  audience,
  COUNT(users) OVER (PARTITION BY month_promo,promocode ORDER BY revenue) AS count_number
FROM  f
)
, h AS (
SELECT -- распределить пользователей пропорционально  
  month_promo,
  platform,
  users,
  revenue,
  cost,
  audience,
  ROUND(SAFE_DIVIDE(users,count_number),0) AS normolized_users
FROM g
WHERE month_promo IS NOT NULL
ORDER BY month_promo ASC
)
SELECT --посчитать CAC и CPM
  month_promo,
  platform,
  SUM(normolized_users) AS users,
  SUM(revenue) AS revenue,
  SUM(cost) AS cost,
  SUM(audience) AS audience,
  ROUND(SAFE_DIVIDE(SUM(cost),SUM(normolized_users)),2) AS CAC,
  ROUND(SAFE_DIVIDE(SUM(cost),SUM(audience))*1000,2) AS CPM
FROM h
GROUP BY 1,2