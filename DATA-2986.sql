-- vk ads без vk_ads retargeting за октябрь 2023
WITH a AS (
SELECT -- транзакции, где сработал ивент оплаченной покупки и таких транзакий или нет в СРМ, или paid=0 по ним
  IF(_event_name_ = 's2s_ecommerce_purchase_non_cis_paid',_purchase_id_,NULL) AS s2s_ecommerce_purchase_non_cis_paid, -- все заказы из adj по событию
  IF(_event_name_ = 's2s_first_ecommerce_purchase_paid',_purchase_id_,NULL) AS s2s_first_ecommerce_purchase_paid, -- транзакции по событию, которые нашлись в crm
  _event_name_,
  _reattributed_at_,
  _adid_,
  _user_id_
FROM `funnel-flowwow.ADJUST_RAW.clients_app` a
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-09-29") AND TIMESTAMP("2023-11-02") 
  AND DATE(_created_at_) BETWEEN '2023-10-01' AND '2023-10-31'  
  AND _activity_kind_ = 'event'
  AND _purchase_id_ IS NOT NULL
  AND _country_ NOT IN ('br')
  AND REGEXP_CONTAINS(LOWER(_campaign_name_),"ретарг|ремарк|retarg|remark|exist")=false
  AND REGEXP_CONTAINS(LOWER(_network_name_),"vk apps|vk_android")
)
,b AS ( -- транзакции, которые нашлись в f_order
SELECT
  a.s2s_first_ecommerce_purchase_paid,-- первые покупки по adjust
  a._event_name_,
  a._reattributed_at_,
  a._adid_,
  a._user_id_,
  b.id AS transactions_from_order_archive, -- покупки по f_order_archive
  IF(paid = 1 AND returning = 0, b.id, NULL) AS new_transactions_from_order_archive, -- оплаченные первые покупки покупки по f_order_archive
  IF(refuse != 0 , b.id, NULL) AS refused_transactions_from_order_archive,
  IF(returning = 1 , b.id, NULL) AS return_transactions_from_order_archive, -- повторные покупки
  IF(paid = 1 AND returning = 0 AND refuse = 0, b.id, NULL) AS target_transactions_from_order_archive, -- оплаченные первые неотмененные покупки покупки по f_order_archive
  status
FROM 
  a LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_order_archive` b ON a.s2s_first_ecommerce_purchase_paid = b.id
)
SELECT -- транзакции,которые нашлись в crm_com
  _event_name_,
  _reattributed_at_,
  _adid_,
  _user_id_,
  s2s_first_ecommerce_purchase_paid,
  transactions_from_order_archive,
  refused_transactions_from_order_archive,
  new_transactions_from_order_archive,
  return_transactions_from_order_archive,
  target_transactions_from_order_archive,
  purchase_id AS purchase_from_crm_com, --все покупки 
  IF(paid = 1 AND is_first_purchase = 1,purchase_id,NULL) AS paid_purchase_from_crm_com, --все первые оплаченные покупки
  IF(is_first_purchase = 0,purchase_id,NULL) AS repeat_purchase_from_crm_com, -- повторные покупки
  IF(purchase_status = 'Отменён',purchase_id,NULL) AS cancelled_purchase_from_crm_com, -- отмененные покупки
  IF(purchase_status != 'Отменён'AND is_first_purchase = 1 AND paid = 1,purchase_id,NULL) AS purchase_from_crm_com_first_paid_non_cancelled -- оплаченные первые неотмененные покупки покупки
FROM b LEFT JOIN funnel-flowwow.CRM_DM_PRTND.crm_com c ON b.transactions_from_order_archive = c.purchase_id
