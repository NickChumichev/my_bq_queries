WITH phone_num AS (
SELECT
  DATE(create_at) AS date,
  id AS user_id,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(phone,"jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=") AS phone
FROM `funnel-flowwow.MYSQL_EXPORT.f_user`
)
, country_by_phone_num AS ( --получить страны регистрации по номеру телефона
SELECT
  date,
  user_id,
  phone,
  CASE
    WHEN STARTS_WITH(phone,"7") AND NOT (STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77"))THEN "Россия"
    WHEN STARTS_WITH(phone,"76") OR STARTS_WITH(phone,"77") THEN "Казахстан"
    WHEN STARTS_WITH(phone,"41") THEN "Швейцария"
    WHEN STARTS_WITH(phone,"62") THEN "Индонезия"
    WHEN STARTS_WITH(phone,"62") THEN "Асеньон"
    WHEN STARTS_WITH(phone,"672") THEN "Рождественсткие о-ва"
    WHEN STARTS_WITH(phone,"21") THEN "Тунис"
    WHEN STARTS_WITH(phone,"47") THEN "Норвегия"
    WHEN STARTS_WITH(phone,"351") THEN "Португалия"
    WHEN STARTS_WITH(phone,"39") THEN "Италия"
    WHEN STARTS_WITH(phone,"243") THEN "ДРК"
    WHEN STARTS_WITH(phone,"382") THEN "Черногория"
    WHEN STARTS_WITH(phone,"383") OR STARTS_WITH(phone,"990") THEN 'other_countries'
    ELSE phone_codes.title_ru
  END AS country_by_phone, --страна регистрации пользователя  
FROM phone_num AS _phone_num LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_MGS_country_phone_codes` AS phone_codes ON STARTS_WITH(_phone_num.phone,phone_codes.phonecode)
WHERE 1=1
AND SAFE_CAST(phone AS NUMERIC) IS NOT NULL
AND phone = '3845418646f3eb2065ee'
)
SELECT
  DATE_TRUNC(date,MONTH) AS month,
  COUNT(DISTINCT user_id) AS users
FROM country_by_phone_num
GROUP BY 1