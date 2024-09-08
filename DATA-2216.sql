WITH a AS (
SELECT DISTINCT -- транзакции, где сработал ивент оплаченной покупки и таких транзакий или нет в СРМ, или paid=0 по ним
  _network_name_,
  _event_name_,
  IF(_event_name_ = 's2s_ecommerce_purchase_paid',_purchase_id_,NULL) AS adj_purchases, -- все заказы из adj по событию
  IF(_event_name_ = 's2s_ecommerce_purchase_paid',purchase_id,NULL) AS crm_paid_purchases, -- все заказы из crm по событию
  IF(_event_name_ = 's2s_first_ecommerce_purchase_paid' AND is_first_purchase = 1, purchase_id,NULL) AS crm_first_paid_purchases, -- все первые заказы из crm по событию
  _purchase_id_,
  IF(_event_name_ = 's2s_ecommerce_purchase_paid',product_price,NULL) AS revenue,
  IF(_event_name_ = 's2s_first_ecommerce_purchase_paid' AND is_first_purchase = 1,product_price,NULL) AS first_revenue
FROM `funnel-flowwow.ADJUST_RAW.clients_app` a
LEFT JOIN  `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` b ON a._purchase_id_ = b.purchase_id
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-08-28") AND TIMESTAMP("2023-09-20") 
  AND DATE(_created_at_) BETWEEN '2023-09-01' AND '2023-09-17'
  AND LOWER(_network_name_) IN ("vk_ads","vk apps") 
  AND _activity_kind_ = 'event'
  AND _purchase_id_ IS NOT NULL
  -- AND a._country_ NOT IN ('br')
)
, b AS ( --вывести отмененные покупки
SELECT DISTINCT
a._purchase_id_
FROM  a LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` b ON a._purchase_id_ = b.purchase_id
WHERE b.purchase_id IS NULL 
)
SELECT
  COUNT(DISTINCT adj_purchases) AS adj_purchases,
  COUNT(DISTINCT IF(_event_name_ = 's2s_ecommerce_purchase_paid' AND b._purchase_id_ IS NOT NULL, a._purchase_id_,NULL)) AS cancelled_purchases,
  COUNT(DISTINCT crm_paid_purchases) AS crm_paid_purchases,
  COUNT(DISTINCT crm_first_paid_purchases) AS crm_first_paid_purchases,
  COUNT(DISTINCT IF(_event_name_ = 's2s_first_ecommerce_purchase_paid' AND b._purchase_id_ IS NOT NULL, a._purchase_id_,NULL)) AS cancelled_first_purchases,
  SUM(revenue) AS revenue,
  SUM(first_revenue) AS first_revenue
FROM a LEFT JOIN b ON a._purchase_id_ = b._purchase_id_