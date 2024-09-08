WITH a AS ( --user_id,где info_push = 1 
SELECT DISTINCT
  user_id
FROM `funnel-flowwow.MYSQL_EXPORT.f_user_subscriptions`
WHERE info_push = 1
)
, b AS (
SELECT DISTINCT --есть токен
	user_id
FROM `funnel-flowwow.Analyt_ChumichevN.DATA-3015_f_app_tokens`
WHERE token NOT IN ('') OR token IS NOT NULL
)
, c AS ( --не было заказов за последние 2 дня и нет активной доставки
SELECT DISTINCT
  user_id,
  status,
  paid 
FROM `funnel-flowwow.MYSQL_EXPORT.f_order_archive`
WHERE 1=1
AND user_id NOT IN (SELECT user_id FROM `funnel-flowwow.MYSQL_EXPORT.f_order_archive` 
WHERE create_at > '2023-11-22 00:00:00'
AND refuse != 0
AND deleted != 1
AND paid = 1
)
AND status IN (2,6,8,9,10) --нет активной доставки
)
, d AS (
SELECT
  a.user_id AS user_subscriptions_user_id,
  b.user_id AS app_tokens_user_id,
  c.user_id AS order_archive_user_id,
FROM c 
LEFT JOIN b ON c.user_id = b.user_id
LEFT JOIN a ON c.user_id = a.user_id
WHERE 1=1 
AND a.user_id IS NOT NULL 
AND b.user_id IS NOT NULL
)
SELECT DISTINCT -- сделали заказ в РФ
  d.order_archive_user_id,
  -- a.city_id,
  c.country_id
FROM d LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_order_archive` a ON d.order_archive_user_id = a.user_id
LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_city_all` c ON a.city_id = c.city_id
WHERE c.country_id = 1 AND order_archive_user_id != 0