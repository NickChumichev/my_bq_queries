WITH a AS ( --получить количество транзакций по магазинам
SELECT
  DATE_TRUNC(purchase_date,MONTH) AS month,  
  e.shop_id,
  COUNT(DISTINCT id) AS transactions,
  SUM(margin) AS commission
FROM `funnel-flowwow.MYSQL_EXPORT.f_order_archive` e 
INNER JOIN `funnel-flowwow.CRM_DM_PRTND.crm_com` m ON e.id = m.purchase_id
WHERE e.status = 3 AND e.paid = 1 AND DATE(e.create_at) BETWEEN '2023-11-01' AND CURRENT_DATE()-1
GROUP BY shop_id,month
ORDER BY transactions DESC
)
, b AS (
SELECT -- магазины с категориями, бизнес формами,городами 
  p.id AS shop_id,
  p.name,
  p.city_id,
  l.city,
  t.range_group_id,
  up.name AS category_name,
  CASE 
    WHEN business_form = 6 THEN 'self_served'
    WHEN business_form IN (1,3,5) THEN 'indivudual'
    WHEN business_form IN (2,4) THEN 'organization'
    ELSE 'other'
    END business_form 
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop` p 
  LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_shop_main_assortment` t ON p.id = t.shop_id
  LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_range_group`up ON t.range_group_id = up.id 
  LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_city` l ON p.city_id = l.city_id
  LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.f_holding_legal_ru` u ON p.id = u.holding_id 
WHERE t.range_group_id IN (1,5,3,17)
AND l.city_id IN (2,49,60,151,104,37,73,147,119,99)-- категория цветы и подарки, кондитерка, живые растения, декор
)
SELECT
  b.city,
  b.category_name,
  b.business_form,
  COUNT(DISTINCT a.shop_id) AS shops,
  SUM(a.transactions) AS transactions,
  SUM(commission) AS commission,
  SUM(a.transactions) / COUNT(DISTINCT a.shop_id) AS avg_transactions, --среднее кол-во транзакций от одного магазина
  (SUM(commission)/4) / COUNT(DISTINCT a.shop_id) AS avg_commission -- средняя комиссия от одного магазина
FROM a INNER JOIN b ON a.shop_id = b.shop_id
GROUP BY 1,2,3
ORDER BY business_form,city
