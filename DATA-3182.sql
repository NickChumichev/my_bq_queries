WITH a AS (
SELECT DISTINCT
  promocode,  
  rrn,
  purchase_id as paid_not_canceled_crm_transaction,
  IF(is_first_purchase=1,purchase_id,NULL) as paid_not_canceled_crm_first_tranasction,
  IF(is_first_purchase=0,purchase_id,NULL) as paid_not_canceled_crm_repeat_transaction,
  SUM(product_price) as paid_not_canceled_crm_revenue,
  SUM(IF(is_first_purchase=1,product_price,0)) as paid_not_canceled_crm_first_revenue,
  SUM(IF(category_name!='Цветы и подарки',product_price,0)) as paid_not_canceled_crm_non_flowers_revenue
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
WHERE rrn IN (SELECT rrn FROM `funnel-flowwow.Analyt_KobozevY.rrns_table` 
WHERE rrn IS NOT NULL)
GROUP BY 1,2,3,4,5
)
SELECT 
  a.promocode,  
  b.rrn,
  a.paid_not_canceled_crm_transaction,
  a.paid_not_canceled_crm_first_tranasction,
  a.paid_not_canceled_crm_repeat_transaction,
  a.paid_not_canceled_crm_revenue,
  a.paid_not_canceled_crm_first_revenue,
  a.paid_not_canceled_crm_non_flowers_revenue
FROM a RIGHT JOIN  `funnel-flowwow.Analyt_KobozevY.rrns_table` b ON a.rrn=b.rrn