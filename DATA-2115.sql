WITH a AS ( -- user_id, у которых есть номера телефонов из файла
SELECT DISTINCT
  f.order_id, 
  f.user_id, 
  w.phone, 
  w.email
FROM `funnel-flowwow.Analyt_ChumichevN.f_review` w
INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_user` r ON w.user_id = r.id
INNER JOIN funnel-flowwow.MYSQL_EXPORT.f_review f ON w.user_id = f.user_id AND w.order_id = f.order_id   
WHERE f.create_at >= "2023-05-01" 
AND f.type="buyer-flowwow" 
AND f.flow_rate>=4 
-- AND w.phone IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_2 WHERE phone NOT IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_1))
AND w.phone IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_1)
AND w.phone NOT IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_2)
-- GROUP BY 1,2
)
, b AS (
SELECT DISTINCT
  f.order_id, 
  f.user_id, 
  w.phone, 
  w.email
FROM `funnel-flowwow.Analyt_ChumichevN.f_review` w
INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_user` r ON w.user_id = r.id
INNER JOIN funnel-flowwow.MYSQL_EXPORT.f_review f ON w.user_id = f.user_id AND w.order_id = f.order_id   
WHERE f.create_at >= "2023-05-01" 
AND f.type="buyer-flowwow" 
AND f.flow_rate>=4 
-- AND w.phone IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_2 WHERE phone NOT IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_1))
AND w.phone NOT IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_1)
AND w.phone IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_2)
AND f.user_id NOT IN (SELECT user_id FROM a)
)
SELECT
  COUNT(DISTINCT purchase_id) AS cnt_purchases,
  SUM(product_price) AS revenue,  
  SUM(product_price)/COUNT(DISTINCT purchase_id) AS AOV
FROM b INNER JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON b.user_id = y.user_id AND b.order_id = y.purchase_id 
WHERE DATE(purchase_timestamp) BETWEEN '2023-07-01' AND '2023-07-31'