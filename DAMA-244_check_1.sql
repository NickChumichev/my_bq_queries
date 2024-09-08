--reliz_mintegral
--время между кликом и инсталлом больше 60 мин.
WITH a AS (
SELECT
  _network_name_,
  _adid_,
  _click_time_ AS click_time, -- время клика по объявлению
  IF(_activity_kind_ = 'install',_installed_at_ ,NULL) AS installed_at, -- первое открытие приложения после установки
  TIMESTAMP_DIFF(IF(_activity_kind_ = 'install',_installed_at_ ,NULL), _click_time_, HOUR) AS hour_dif 
FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-05-29") AND TIMESTAMP("2024-01-01") AND LOWER(_network_name_)IN ('reliz_mintegral')
ORDER BY hour_dif DESC
)
SELECT
  _network_name_,
  _adid_,
  click_time,
  installed_at,
  hour_dif
FROM a 
WHERE 1=1
AND hour_dif >1 
AND installed_at IS NOT NULL