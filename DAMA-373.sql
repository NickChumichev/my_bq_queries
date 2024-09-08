--города через ip адреса определены
WITH a AS (
SELECT -- пользователи с завершенной регистрацией
  user_id, city, web_to_app, if_app
  FROM (
  SELECT  
  _user_id_ AS user_id,_city_ AS city, IF(_network_name_='flowwow_com',1,0) AS web_to_app, 1 AS if_app
  FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-02-27") AND TIMESTAMP("2023-10-03") AND _event_name_='complete_registration'
  AND DATE(_created_at_) BETWEEN '2023-03-01' AND '2023-10-01'
  AND _city_ IN ('Moscow','Sochi','St Petersburg')
  AND _country_ = 'ru'     
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _user_id_ ORDER BY _created_at_)=1
  ) 
  GROUP BY 1,2,3,4
)
, b AS (
SELECT --назначить платформы регистрации
  user_id,
  city,
    CASE
    WHEN if_app=1 AND web_to_app=1 THEN "web_to_app"
    WHEN if_app=1 AND web_to_app=0 THEN "app"
    ELSE "web"
    END AS 
  platform_of_registration
 FROM a
)
, c AS (
SELECT -- зарегистрированные пользователи без покупок
  CAST(id AS STRING) AS user_id,
FROM `funnel-flowwow.MYSQL_EXPORT.f_user`
WHERE id NOT IN (SELECT user_id FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`)
-- AND WHERE DATE(create_at) BETWEEN '2023-03-01' AND '2024-10-01'

UNION ALL

SELECT
  user_id,
FROM (SELECT DISTINCT-- зарегистрированные пользователи без покупок
  CAST(user_id AS STRING) AS user_id,
  DATE(purchase_timestamp) AS date,
  purchase_id,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_timestamp) AS num
  -- COUNT(DISTINCT IF(DATE(purchase_timestamp) >= '2023-09-01',purchase_id, NULL)) AS purchases
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` 
WHERE 1=1
AND user_id NOT IN (SELECT user_id FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` WHERE DATE(purchase_timestamp) >= '2023-09-01'))
GROUP BY 1
HAVING COUNT(purchase_id) = 1
)
SELECT DISTINCT
 c.user_id,
 city,
 platform_of_registration
FROM c INNER JOIN b USING(user_id) 