--кейс 1
WITH a AS (
SELECT -- получить order_id, где есть оценка 3
  user_id,
  order_id,
  flow_rate,
  courier_rate,
  shop_rate,
  compliance_rate
FROM
  `funnel-flowwow.MYSQL_EXPORT.f_review`
WHERE 1=1 
AND (flow_rate = 3
  OR courier_rate = 3
  OR shop_rate = 3
  OR compliance_rate = 3)
)
, b AS (
SELECT -- получить данные по покупкам
  CAST(user_id AS STRING) AS user_id,
  DATE(purchase_date) AS purchase_date,
  LAST_VALUE(city_from_name) OVER (PARTITION BY user_id ORDER BY DATE(purchase_date) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_city,
  purchase_id AS purchases,
  LAST_VALUE(DATE(purchase_date)) OVER (PARTITION BY user_id ORDER BY DATE(purchase_date) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_purchase_date
FROM `funnel-flowwow.CRM_DM_PRTND.crm_com`
WHERE paid = 1 AND purchase_status = 'Завершён'
AND user_id !=0
AND user_id NOT IN (SELECT id FROM `funnel-flowwow.MYSQL_EXPORT.f_order`) --нет активных заказов
AND user_id NOT IN (SELECT user_id FROM  a) -- нет ни одной оценки 3
AND city_from_name IN ('Сочи','Москва','Воронеж','Краснодар', 'Санкт-Петербург')
QUALIFY DATE(last_purchase_date) BETWEEN  DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) --последняя покупка в последние 90 дней
)
, c AS (
SELECT
  user_id,
  last_city,
  COUNT(purchases) AS purchases
  FROM b
GROUP BY 1,2
HAVING purchases < 3 --менее 3 покупок
)
, d AS (
SELECT DISTINCT  
  _user_id_ AS user_id,
  LAST_VALUE(_app_version_short_) OVER (PARTITION BY _user_id_ ORDER BY _created_at_ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS app_version_short,
  LAST_VALUE(_os_name_) OVER (PARTITION BY _user_id_ ORDER BY _created_at_ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS os_name
  FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2019-01-01") AND TIMESTAMP("2024-04-15") 
  AND DATE(_created_at_) BETWEEN '2019-01-01' AND '2024-04-15' 
)
SELECT DISTINCT
  user_id,
  last_city,
  app_version_short,
  os_name,
  purchases
FROM c INNER JOIN d USING(user_id)