--reliz_mintegral
--_country_ - код страны, в которой находится устройство пользователя, на один _adid_ может приходиться несколько стран
SELECT
  _network_name_,
  _country_,
  COUNT(DISTINCT _adid_) AS cnt_adid_
FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-05-29") AND TIMESTAMP("2024-01-01") AND LOWER(_network_name_)IN ('reliz_mintegral')
GROUP BY 1,2
ORDER BY cnt_adid_ DESC