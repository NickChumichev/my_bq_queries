WITH a AS (
SELECT -- транзакции, где сработал ивент оплаченной покупки и таких транзакий или нет в СРМ, или paid=0 по ним
  DATE(a._created_at_) AS date,
  IF(_event_name_ IN ('s2s_ecommerce_purchase_paid_GBR') AND paid= 0 OR _purchase_id_ NOT IN (SELECT purchase_id FROM `funnel-flowwow.CRM_DM_PRTND.crm_com`), a._purchase_id_,NULL) AS adj_canceled_purchase_id,

  IF(_event_name_ = 's2s_ecommerce_purchase_paid_GBR',a._purchase_id_,NULL) AS adj_purchases, -- все заказы из adj по событию

  IF(_event_name_ = 's2s_ecommerce_purchase_paid_GBR',c.purchase_id,NULL) AS crm_paid_purchases-- все заказы из crm по событию

FROM `funnel-flowwow.ADJUST_RAW.clients_app` a 
LEFT JOIN  `funnel-flowwow.CRM_DM_PRTND.crm_com` c ON a._purchase_id_ = c.purchase_id
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-09-01") AND TIMESTAMP("2023-09-19") 
  AND DATE(a._created_at_) = '2023-09-13'
  AND REGEXP_CONTAINS(LOWER (_network_name_),'unattributed')
  AND _activity_kind_ = 'event'
  AND _event_name_ = 's2s_ecommerce_purchase_paid_GBR'
  ORDER BY date DESC
)
, b AS ( -- поиск adj_canceled_purchase_id среди отмененных заказов
SELECT DISTINCT
  y.order_id,
  comment,
  reason,
  real_reason
FROM a INNER JOIN funnel-flowwow.MYSQL_EXPORT.f_order_refuse y ON a.adj_canceled_purchase_id = y.order_id
)
SELECT -- посмотреть статусы заказа
  comment,
  COUNT(comment) AS cnt_comment,
  ARRAY_AGG(reason) AS arr_reason
FROM b
GROUP BY comment
ORDER BY cnt_comment DESC
