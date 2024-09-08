WITH promo_contains AS (
SELECT 
  LOWER(LEFT(promocode,4)) as partner_promo --взять промокоды из файлика без f_program
FROM `funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view`
WHERE promocode IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")=false
  )
, f_program AS (
SELECT 
  LOWER(LEFT(code,4)) as partner_promo --промокоды программы лояльности c f_program
FROM `funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view`
RIGHT JOIN (SELECT code,type FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes` UNION ALL
SELECT code,type FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes_log`) ON promocode=type
WHERE promocode IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")
  )
, setting_ids AS ( 
SELECT       
  code as partner_promo_setting
FROM (SELECT code,setting_id FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes` UNION ALL
SELECT code,setting_id FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes_log`)
WHERE CAST(setting_id AS STRING) IN (SELECT setting_id FROM `funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view`)
QUALIFY ROW_NUMBER() OVER(PARTITION BY partner_promo_setting)=1 --удалить дубли
)
, partner_promocodes AS ( --промокоды с маленькой буквы, состоят из 4 символов
  SELECT * FROM (
  SELECT * FROM promo_contains
  UNION ALL
  SELECT * FROM f_program)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY partner_promo)=1
)
SELECT -- объединить промокоды с данными по покупкам
  DATE_TRUNC(DATE(purchase_timestamp),WEEK(MONDAY)) AS week,
  CASE 
    WHEN partner_name = "TinkoffSuperApp" THEN 'Тинькофф супер апп'
    WHEN partner_promo IS NOT NULL AND partner_promo_setting IS NULL AND partner IS NOT NULL THEN  partner
    WHEN partner_promo_setting IS NOT NULL THEN  'partner_promo_setting'
    ELSE 'partner_promocodes' -- из partner_promocodes, но без названия партнера
    END AS partner,
  promocode,
  COUNT(DISTINCT purchase_id) AS purchases,
  COUNT(DISTINCT IF(product_category_id != 1,purchase_id, NULL)) AS non_flower_purchases,
  COUNT(DISTINCT IF(product_category_id != 1,purchase_id, NULL)) / COUNT(DISTINCT purchase_id) AS non_flower_purchases_share,  
  COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id, NULL)) AS first_purchases,
  COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id, NULL)) / COUNT(DISTINCT purchase_id) AS first_purchases_share, 
  COUNT(DISTINCT IF(is_first_purchase = 0, purchase_id, NULL)) AS repeated_purchases,
  COUNT(DISTINCT IF(is_first_purchase = 0, purchase_id, NULL)) / COUNT(DISTINCT purchase_id) AS repeated_purchases_share, 
  SUM(product_price) AS revenue,
  SUM(product_price) - (SUM(promo_sum_rub) + SUM(bonus_company)) AS revenue_without_promo_bonus,
  (SUM(product_price) - (SUM(promo_sum_rub) + SUM(bonus_company))) / SUM(product_price) AS revenue_without_promo_bonus_share
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
LEFT JOIN partner_promocodes ON LEFT(promocode,4)=LOWER(partner_promo)
LEFT JOIN setting_ids ON LOWER(promocode)=LOWER(partner_promo_setting)
LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view` USING(promocode)
WHERE 1=1
AND (partner_promo IS NOT NULL OR partner_promo_setting IS NOT NULL OR partner_name="TinkoffSuperApp")
AND DATE(purchase_timestamp) >= '2023-01-01'
GROUP BY partner,promocode,week
ORDER BY partner,week ASC