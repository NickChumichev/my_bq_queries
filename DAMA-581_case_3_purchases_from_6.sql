WITH a AS ( 
SELECT 
  user_id,
  COUNT(DISTINCT purchase_id) AS purchases
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` 
WHERE 1=1 
AND DATE(purchase_timestamp) BETWEEN '2024-01-01' AND CURRENT_DATE()-1
GROUP BY 1
HAVING purchases  >= 6
)
, b AS (
SELECT DISTINCT
  user_id,
  COUNT(DISTINCT purchase_id) AS all_purchases,
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` 
WHERE user_id IN (SELECT user_id FROM a)
GROUP BY 1 
)
, c AS (
SELECT DISTINCT
  user_id,
  LAST_VALUE(DATE(purchase_timestamp)) OVER (PARTITION BY user_id ORDER BY purchase_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_purchase_date
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` 
WHERE user_id IN (SELECT user_id FROM a)
)
, d AS (
SELECT
  create_at,
  id AS user_id,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(email,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS email,
  all_purchases,
  last_purchase_date
FROM `funnel-flowwow.MYSQL_EXPORT.f_user` r 
  LEFT JOIN b ON r.id = b.user_id 
  LEFT JOIN c ON r.id = c.user_id
WHERE id IN (SELECT user_id FROM a)
)
, e AS (
SELECT DISTINCT 
   _user_id_, 
   last_is_allowed 
FROM `funnel-flowwow.Analyt_ChumichevN.DAMA_581_push_access_push_opened` 
WHERE _event_name_ = 'push_access'
)
SELECT DISTINCT
  d.create_at,--дата регистрации пользователя
  d.user_id,
  d.phone,
  d.email,
  d.all_purchases,
  d.last_purchase_date,
  p._user_id_,
  CASE
    WHEN e.last_is_allowed = 'is_allowed":"0"' THEN 0
    WHEN e.last_is_allowed = 'is_allowed":"1"' THEN 1
    ELSE NULL
    END AS push_access,
  IF(p._user_id_ IS NULL,0,1) AS is_app,
  COUNT(DISTINCT IF(_event_name_ = 'push_opened',1, NULL)) AS push_opened, 
FROM d
LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.DAMA_581_push_access_push_opened` p ON CAST(d.user_id AS STRING) = CAST(p._user_id_ AS STRING)
LEFT JOIN e ON CAST(d.user_id AS STRING) = CAST(e._user_id_ AS STRING)
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY d.user_id DESC
