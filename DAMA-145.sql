WITH a AS ( -- получить покупателей с номерами телефонов
SELECT 
  CAST(r.id AS STRING) as user_id, -- id покупателя
  time_zone,
  `funnel-flowwow.MYSQL_EXPORT.PY_DECODE`(r.phone,'jov1Jo6E21kruRd0H6tG7bfvIeIOrlr03m6-bdIoJGQ=') AS phone,
  recipient_id, -- id получателя
  e.phone AS recipient_phone --номер телефона получателя
FROM `funnel-flowwow.MYSQL_EXPORT.f_user` r RIGHT JOIN `funnel-flowwow.MYSQL_EXPORT.f_order_archive` e ON r.id = e.user_id 
WHERE DATE(e.create_at) BETWEEN '2023-01-01' AND '2023-12-31' AND paid = 1 AND status = 3
)
, b AS (
 SELECT  --получить список заказов, страну из, страну в   
  CAST(user_id AS STRING) as user_id, 
  country_1.country as country_from, 
  country_2.country as country_to 
  FROM `funnel-flowwow.CRM_DM_PRTND.crm_com` crm 
  LEFT JOIN
    (SELECT city_id, city, country_id, country
    FROM `funnel-flowwow.MYSQL_EXPORT.f_city_all` 
    LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_country` USING(country_id)) as country_1
    ON crm.city_from_id = country_1.city_id
  LEFT JOIN
    (SELECT city_id, city, country_id, country
    FROM `funnel-flowwow.MYSQL_EXPORT.f_city_all` 
    LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_country` USING(country_id)) as country_2
    ON crm.city_id=country_2.city_id 
  WHERE paid = 1 AND purchase_status = 'Завершён' AND DATE(purchase_date) BETWEEN '2023-01-01' AND '2023-12-31'
)
, d AS (
SELECT DISTINCT
  b.user_id,
  a.time_zone,
  a.phone,
  CASE
    WHEN STARTS_WITH(phone, '49') THEN 'Germany'
    WHEN STARTS_WITH(phone, '1') THEN 'USA'
    WHEN STARTS_WITH(phone, '31') THEN 'Netherlands'
    WHEN STARTS_WITH(phone, '33') THEN 'France'
    WHEN STARTS_WITH(phone, '90') THEN 'Turkey'
    WHEN STARTS_WITH(phone, '44') THEN 'UK'
    WHEN STARTS_WITH(phone, '66') THEN 'Thailand'
    WHEN STARTS_WITH(phone, '381') THEN 'Serbia'
    WHEN STARTS_WITH(phone, '360') THEN 'Serbia'
    ELSE 'other country'
    END AS phone_country, 
  b.country_from,
  b.country_to,
  a.recipient_id,
  a.recipient_phone
FROM a RIGHT JOIN b USING (user_id) 
)
SELECT
  user_id,
  time_zone,
  phone,
  phone_country,
  country_from,
  country_to,
  recipient_id,
  recipient_phone
FROM d
WHERE phone_country NOT IN ('other country')

