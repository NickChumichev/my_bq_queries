WITH a AS ( -- user_id, у которых есть номера телефонов из файла part_1
SELECT DISTINCT
  f.order_id, 
  f.user_id, 
  w.phone, 
  w.email
FROM `funnel-flowwow.Analyt_ChumichevN.DATA-2519_phone_number` w -- f_user c открытыми номерами и email
INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_user` r ON w.id = r.id 
INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_review` f ON w.id = f.user_id --получить order_id отсюда  
WHERE f.create_at >= "2023-05-01" --сделали заказ за последние 3 месяца 
AND f.type="buyer-flowwow" --покупатель flowwow
AND f.flow_rate>=4 
-- AND w.phone IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_2 WHERE phone NOT IN (SELECT phone FROM funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_1))
AND w.phone IN (SELECT phone FROM `funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_1`)
AND w.phone NOT IN (SELECT phone FROM `funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_2`)
-- GROUP BY 1,2
)
, b AS ( -- user_id, у которых есть номера телефонов из файла part_2
SELECT DISTINCT
  f.order_id, 
  f.user_id, 
  w.phone, 
  w.email
FROM `funnel-flowwow.Analyt_ChumichevN.f_review` w
INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_user` r ON w.user_id = r.id
INNER JOIN funnel-flowwow.MYSQL_EXPORT.f_review f ON w.user_id = f.user_id AND w.order_id = f.order_id   
WHERE 1=1
AND f.create_at >= "2023-05-01" 
AND f.type="buyer-flowwow" 
AND f.flow_rate>=4 
AND w.phone NOT IN (SELECT phone FROM `funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_1`)
AND w.phone IN (SELECT phone FROM `funnel-flowwow.Analyt_ChumichevN.DATA_2115_part_2`)
AND f.user_id NOT IN (SELECT user_id FROM a) -- исключить user_id из part_1
)
SELECT -- статистика по part_2
  COUNT(DISTINCT y.user_id) AS users,
  COUNT(DISTINCT y.purchase_id) AS cnt_purchases,
  SUM(y.product_price) AS revenue,  
  SUM(y.product_price)/COUNT(DISTINCT purchase_id) AS AOV
FROM a INNER JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON a.user_id = y.user_id AND a.order_id = y.purchase_id 
WHERE DATE(y.purchase_timestamp) BETWEEN '2023-09-01' AND '2023-09-30'