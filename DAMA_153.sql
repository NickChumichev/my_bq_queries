SELECT DISTINCT -- атрибуция по кликам
  _network_name_,
  _campaign_name_,
  _activity_kind_,
  -- _event_name_, 
  _adid_,
  _installed_at_,
  -- _created_at_,
  _ip_address_,
  _country_,
  CAST(p._user_id_ AS STRING) AS _user_id_,
  r.last_city_id,
  r.currency,
  r.city_id,
  r.time_zone,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(r.email,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS email,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(r.phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone,
  e.message,
  e.address, --адрес доставки
  e.currency,
  e.currency_before
FROM `funnel-flowwow.ADJUST_RAW.clients_app` p
LEFT JOIN  `funnel-flowwow.MYSQL_EXPORT.f_user` r ON CAST(p._user_id_ AS STRING) = CAST(r.id AS STRING)
LEFT JOIN  `funnel-flowwow.MYSQL_EXPORT.f_order_archive` e ON CAST(p._user_id_ AS STRING) = CAST(e.user_id AS STRING)
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-12-29") AND TIMESTAMP("2024-02-01") 
AND DATE(_created_at_) BETWEEN '2024-01-01' AND '2024-01-31'
AND LOWER(_network_name_) IN ('tiktok','tiktok san') 
-- AND _event_name_ IN ('s2s_first_ecommerce_purchase_paid','s2s_ecommerce_purchase_paid', 's2s_first_delivered_purchase')