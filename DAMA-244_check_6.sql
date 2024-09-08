SELECT
  _network_name_,
  COUNT(DISTINCT(_click_time_)) AS clicks,
  COUNT(IF(_activity_kind_ = 'install',1,NULL)) AS installs,
  COUNT(IF(_activity_kind_ = 'install',1,NULL))/COUNT(DISTINCT(_click_time_)) AS CR,
  COUNT(IF(_event_name_ = 's2s_ecommerce_purchase_paid',1,NULL)) AS s2s_ecommerce_purchase_paid,
  COUNT(IF(_event_name_ = 's2s_first_ecommerce_purchase_paid',1,NULL)) AS s2s_first_ecommerce_purchase_paid,
FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-05-29") AND TIMESTAMP("2024-01-01") AND LOWER(_network_name_) IN ('reliz_mintegral')
GROUP BY 1
-- ORDER BY installs DESC