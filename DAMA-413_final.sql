WITH promo_contains AS (
SELECT 
  partner,
   '' AS type,
  CAST(setting_id AS STRING) AS setting_id,
  LOWER(LEFT(promocode,4)) as partner_promo --взять промокоды из файлика без f_program
FROM `funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view`
WHERE promocode IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")=false
  )
, f_program AS (
SELECT
  '' AS partner,
  type,
  CAST(setting_id AS STRING) AS setting_id,
  LOWER(LEFT(promocode,4)) as partner_promo --промокоды программы лояльности c f_program
FROM `funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view`
RIGHT JOIN (SELECT code,type FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes` UNION ALL
SELECT code,type FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes_log`) ON promocode=type
WHERE promocode IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")
  )
, partner_promocodes AS ( --промокоды с маленькой буквы, состоят из 4 символов
  SELECT * FROM (
  SELECT * FROM promo_contains
  UNION ALL
  SELECT * FROM f_program)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY partner_promo)=1
)
, purchases AS (
SELECT
  promocode,
  partner_name,
  COUNT(DISTINCT purchase_id) AS purchases,
  COUNT(DISTINCT IF(product_category_id != 1,purchase_id, NULL)) AS non_flower_purchases,
  SAFE_DIVIDE(COUNT(DISTINCT IF(product_category_id != 1,purchase_id, NULL)) , COUNT(DISTINCT purchase_id)) AS non_flower_purchases_share,  
  COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id, NULL)) AS first_purchases,
  SAFE_DIVIDE(COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id, NULL)) , COUNT(DISTINCT purchase_id)) AS first_purchases_share, 
  COUNT(DISTINCT IF(is_first_purchase = 0, purchase_id, NULL)) AS repeated_purchases,
  SAFE_DIVIDE(COUNT(DISTINCT IF(is_first_purchase = 0, purchase_id, NULL)) , COUNT(DISTINCT purchase_id)) AS repeated_purchases_share, 
  SUM(product_price) AS revenue,
  SUM(product_price) - (SUM(promo_sum_rub) + SUM(bonus_company)) AS revenue_without_promo_bonus,
  SAFE_DIVIDE((SUM(product_price) - (SUM(promo_sum_rub) + SUM(bonus_company))) , SUM(product_price)) AS revenue_without_promo_bonus_share
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
WHERE DATE(purchase_timestamp) >= '2024-01-01'
GROUP BY 1,2
)
-- , union_promocodes AS (
SELECT DISTINCT -- объединить промокоды с данными по покупкам
  -- partner,
  -- purchases
  -- partner_promo,
  -- promocode
  CASE
    WHEN es.type = 'sberbank' OR STARTS_WITH(LOWER(promocode),'drug22') THEN 'Сбербанк Друг'
    WHEN es.type = 'primezone' OR (STARTS_WITH(LOWER(promocode),'pz24') OR STARTS_WITH(LOWER(promocode),'pz25')) THEN 'Primezone'
    WHEN es.type = 'union'  THEN 'Профсоюзы'
    WHEN partner_name = 'TinkoffSuperApp' AND NOT STARTS_WITH(LOWER(promocode),'flowbestb12') THEN 'Тинькофф супер апп'
    ELSE partner
    END AS partner_new,
  setting_id,
  type,
  -- partner_name,
  -- promocode,
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
FROM  partner_promocodes es
LEFT JOIN partner_promocodes es ON LOWER(LEFT(promocode,4))=LOWER(partner_promo)
-- LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` ON LOWER(promocode) LIKE CONCAT(LOWER(partner_promo), '%')
WHERE 1=1
-- AND (partner_promo IS NOT NULL OR partner_name="TinkoffSuperApp")
-- AND DATE(purchase_timestamp) >= '2023-01-01'
-- AND promocode_new = 'primezone'
-- AND LOWER(promocode) LIKE LOWER(partner_promo)
GROUP BY 1,2,3