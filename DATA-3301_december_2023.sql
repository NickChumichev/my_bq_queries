-- google uac, событие  s2s_ecommerce_purchase_non_flowers_paid
WITH a AS (
SELECT -- транзакции, где сработал ивент оплаченной покупки и таких транзакий или нет в СРМ, или paid=0 по ним
  _created_at_,
  IF(_event_name_ = 's2s_ecommerce_purchase_paid',_purchase_id_,NULL) AS  s2s_ecommerce_purchase_paid, -- все заказы из adj по событию
  IF(_event_name_ = 's2s_first_ecommerce_purchase_paid',_purchase_id_,NULL) AS  s2s_first_ecommerce_purchase_paid, -- первые заказы из adj по событию
FROM `funnel-flowwow.ADJUST_RAW.clients_app`
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-11-29") AND TIMESTAMP("2023-12-27") 
  AND DATE(_created_at_) BETWEEN '2023-12-01' AND '2023-12-26'  
  AND _activity_kind_ = 'event'
  AND _purchase_id_ IS NOT NULL
  AND _country_ NOT IN ('br')
  AND REGEXP_CONTAINS(LOWER(_campaign_name_),"ios|andr|app|uac")
  AND REGEXP_CONTAINS(LOWER(_network_name_),"google ads aci|google ads ace")
)
-- ,b AS ( 
  -- транзакции, которые нашлись в f_order
SELECT
  COUNT(a.s2s_ecommerce_purchase_paid) AS s2s_ecommerce_purchase_paid,-- покупки по adjust
  COUNT(a.s2s_first_ecommerce_purchase_paid) AS s2s_first_ecommerce_purchase_paid,-- первые покупки по adjust
  -- COUNT(b.id AS transactions_from_order_archive) AS transactions_from_order_archive, -- покупки по f_order_archive
  COUNT(IF(paid = 1 AND purchase_status = 'Завершён', b.purchase_id, NULL)) AS paid_transactions_from_crm, -- оплаченные покупки по f_order_archive
  COUNT(IF(paid = 1 AND is_first_purchase = 1  AND purchase_status = 'Завершён', b.purchase_id, NULL)) AS first_paid_transactions_from_crm, -- первые оплаченные покупки покупки по f_order_archive
  COUNT(IF(purchase_status = 'Отменён' , b.purchase_id, NULL)) AS refused_transactions_from_crm, -- отмененные покупки
  COUNT(IF(purchase_status = 'Отменён' AND is_first_purchase = 1, b.purchase_id, NULL)) AS refused_first_transactions_from_crm, -- отмененные первые покупки
FROM 
  a LEFT JOIN `funnel-flowwow.CRM_DM_PRTND.crm_com` b ON a.s2s_ecommerce_purchase_paid = b.purchase_id

