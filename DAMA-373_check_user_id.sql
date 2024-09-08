WITH a AS (
SELECT -- пользователи с завершенной регистрацией
  user_id, last_event_city, event_name, web_to_app, if_app
  FROM (
SELECT  
  _user_id_ AS user_id,
  LAST_VALUE(_city_) OVER (PARTITION BY _user_id_ ORDER BY _created_at_ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_city, 
  _event_name_ AS event_name,
  IF(_network_name_='flowwow_com',1,0) AS web_to_app, 
  1 AS if_app
  FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2022-12-29") AND TIMESTAMP("2024-01-26") 
  -- AND _event_name_='authorization_success'
  AND DATE(_created_at_) BETWEEN '2023-01-01' AND '2024-01-24' 
  AND _city_ IN ('Moscow','Sochi','St Petersburg')
  AND _country_ = 'ru'   
  ) 
  GROUP BY 1,2,3,4,5
)
, b AS ( --оставить user_id c успешной авторизацией
SELECT DISTINCT 
   user_id, 
   last_event_city, 
  --  event_name, 
   web_to_app, 
   if_app
FROM a
-- WHERE REGEXP_CONTAINS(LOWER(event_name), 'purchase') = false 
-- WHERE event_name ='authorization_success'  
)
, c AS (
SELECT DISTINCT --назначить платформы регистрации
  user_id,
  last_event_city, 
    CASE
      WHEN if_app=1 AND web_to_app=1 THEN "web_to_app"
      WHEN if_app=1 AND web_to_app=0 THEN "app"
      ELSE "web"
      END AS 
  platform_of_registration
 FROM b
)
, d AS (
SELECT -- зарегистрированные пользователи без покупок
  CAST(id AS STRING) AS user_id,
FROM `funnel-flowwow.MYSQL_EXPORT.f_user`
WHERE id NOT IN (SELECT user_id FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`)
-- AND WHERE DATE(create_at) BETWEEN '2023-03-01' AND '2024-10-01'

UNION ALL

SELECT
  user_id,
FROM (SELECT DISTINCT-- зарегистрированные пользователи с одной покупкой и чтобы покупка была полгода назад
  CAST(user_id AS STRING) AS user_id,
  DATE(purchase_timestamp) AS date,
  purchase_id,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_timestamp) AS num
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` 
WHERE 1=1
AND user_id NOT IN (SELECT user_id FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` WHERE DATE(purchase_timestamp) >= '2023-09-10'))
GROUP BY 1
HAVING COUNT(purchase_id) = 1
)
SELECT DISTINCT
 c.user_id,
 last_event_city,
 platform_of_registration
FROM c INNER JOIN d USING(user_id) 