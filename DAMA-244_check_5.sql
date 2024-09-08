--reliz_mintegral
--на один adjust_device_id приходится одна установка.
SELECT
  _network_name_,
  _adid_,
  COUNT(DISTINCT(_click_time_)) AS clicks,
  COUNT(DISTINCT(IF(_activity_kind_ = 'install',1,NULL))) AS installs
FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-05-29") AND TIMESTAMP("2024-01-01") AND LOWER(_network_name_)IN ('reliz_mintegral')
-- AND _adid_ = '57e1a42f0fc774b3bd390bbd3ac1ad98'
GROUP BY 1,2
ORDER BY installs DESC