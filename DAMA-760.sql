WITH a AS (
SELECT
  id,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,"jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=") AS phone,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(email,"jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=") AS email
FROM `funnel-flowwow.MYSQL_EXPORT.f_user`
WHERE id IN (SELECT user_id FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`)
)
, b AS (
SELECT DISTINCT
  user_id,
  phone,
  LOWER(email) AS email,
  purchase_id,
  LAST_VALUE(DATE(purchase_timestamp)) OVER (PARTITION BY user_id ORDER BY purchase_timestamp rows between unbounded preceding and unbounded following) AS last_purchase_date,
  DATE(purchase_timestamp) AS date
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
LEFT JOIN a ON id = user_id
WHERE user_id IN (SELECT id FROM a)
AND user_id IS NOT NULL
--AND user_id = 150
ORDER BY  user_id, date DESC
)
SELECT DISTINCT
  TO_HEX(MD5(CAST (user_id AS STRING))) AS external_id,
  TO_HEX(MD5(phone)) AS phone,
  TO_HEX(MD5(email)) AS email
FROM b
WHERE last_purchase_date BETWEEN '2023-03-01' AND CURRENT_DATE()-1
-- ORDER BY user_id, date DESC