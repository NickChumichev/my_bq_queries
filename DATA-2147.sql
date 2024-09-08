WITH a AS ( -- все уникальные поздравления в открытках
SELECT DISTINCT
  REGEXP_REPLACE(LOWER(message), 'днем','днём') AS message
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y
INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_order_archive` e ON y.purchase_id = e.id
WHERE 1=1 
AND DATE(purchase_timestamp) = '2022-11-26' AND message NOT IN ('') --исключить незаполненные открытки
)
, b AS ( -- все поздравления в открытках
SELECT
  purchase_id,
  REGEXP_REPLACE(LOWER(message), 'днем','днём') AS message
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y
INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_order_archive` e ON y.purchase_id = e.id
WHERE 1=1 
AND DATE(purchase_timestamp) = '2022-11-26' AND message NOT IN ('') --исключить незаполненные открытки
)
SELECT --количество поздравлений
  REGEXP_REPLACE(LOWER(a.message), r'\!|\?|\:|\)','') AS message, --заменить специальные символы
  COUNT(REGEXP_REPLACE(LOWER(b.message), r'\!|\?|\:|\)','')) AS rate_of_messages --сколько данное поздравление встречается
FROM a LEFT JOIN b ON LOWER(b.message) LIKE CONCAT('%', LOWER(a.message), '%') 
WHERE 1=1
AND REGEXP_CONTAINS(LOWER(a.message),' ') --исключить поздравления в одно слово
-- AND REGEXP_CONTAINS(LOWER(a.message),'с днём матери') 
GROUP BY 1
ORDER BY rate_of_messages DESC
