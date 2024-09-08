SELECT
  COUNT(id) AS cnt_id ,
  COUNT(IF (url NOT IN (''), id, NULL)) AS cnt_url,
  COUNT(IF (translit NOT IN (''), id, NULL)) AS translit,
  COUNT(IF (vkid != 0, id, NULL)) AS vkid,
  COUNT(IF (soc_ok NOT IN (''), id, NULL)) AS soc_ok,
  COUNT(IF (soc_fb NOT IN (''), id, NULL)) AS soc_fb,
  COUNT(IF (soc_vk NOT IN (''), id, NULL)) AS soc_vk,
  COUNT(IF (soc_tw NOT IN (''), id, NULL)) AS soc_tw,
  COUNT(IF (soc_in NOT IN (''), id, NULL)) AS soc_in,
  COUNT(IF (telegram_code != 0, id, NULL)) AS telegram_code,
  COUNT(IF (email NOT IN (''), id, NULL)) AS email,
  COUNT(IF (phone NOT IN (''), id, NULL)) AS phone,
FROM `funnel-flowwow.MYSQL_EXPORT.f_shop` 