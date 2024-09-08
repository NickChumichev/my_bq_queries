--reliz_mintegral
--_adid_ без событий
SELECT 
  _network_name_,
  _adid_,
  COUNT(IF(_event_name_ IS NOT NULL,1,NULL)) AS cnt_events
FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-05-29") AND TIMESTAMP("2024-01-01") AND LOWER(_network_name_)IN ('reliz_mintegral')
-- AND _event_name_ IS NOT NUlL
GROUP BY 1,2
ORDER BY cnt_events DESC