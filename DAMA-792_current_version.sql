WITH promo_contains AS (
SELECT 
  partner,
   '' AS type,
  CAST(setting_id AS STRING) AS setting_id,
  LOWER(LEFT(promocode,16)) as partner_promo --взять промокоды из файлика без f_program
FROM `funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view`
WHERE promocode IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")=false
  )
, f_program AS (
SELECT 
  '' AS partner,
  type,
  CAST(setting_id AS STRING) AS setting_id,
  LOWER(code) as partner_promo --промокоды программы лояльности c f_program
FROM `funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view`
RIGHT JOIN (SELECT code,type FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes` UNION ALL
SELECT code,type FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes_log`) ON promocode=type
WHERE promocode IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")
  )
 , union_tables AS ( 
SELECT DISTINCT -- объединить промокоды с данными по покупкам
  DATE_TRUNC(purchase_timestamp,WEEK(MONDAY)) AS week,
  CASE    
    WHEN am.type = 'sberbank' OR STARTS_WITH(LOWER(promocode),'drug22') THEN 'Сбербанк Друг'
    WHEN am.type = 'primezone' OR (STARTS_WITH(LOWER(promocode),'pz24') OR STARTS_WITH(LOWER(promocode),'pz25')) THEN 'Primezone'
    WHEN am.type = 'union'  THEN 'Профсоюзы'
    WHEN partner_name = 'TinkoffSuperApp' AND NOT STARTS_WITH(LOWER(promocode),'flowbestb12') THEN 'Тинькофф супер апп'
    ELSE r.partner
    END AS partner,
    r.type,
    am.type,
    promocode,
    purchase_id,
    IF(product_category_id != 1,purchase_id, NULL) AS non_flower_purchases, 
    IF(is_first_purchase = 1, purchase_id, NULL) AS first_purchases,
    IF(is_first_purchase = 0, purchase_id, NULL) AS repeated_purchases,
    product_price AS revenue,
    promo_sum_rub,
    bonus_company
FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
LEFT JOIN promo_contains r ON (STARTS_WITH(LOWER(promocode),LOWER(r.partner_promo)))
LEFT JOIN f_program am ON LOWER(promocode) = LOWER(am.partner_promo)
WHERE 1=1
AND (r.partner_promo IS NOT NULL OR am.partner_promo IS NOT NULL OR partner_name="TinkoffSuperApp")
AND DATE(purchase_timestamp) >= '2023-06-01'
ORDER BY partner
 )
SELECT
  week,
  partner,
  promocode,
  COUNT(DISTINCT purchase_id) AS purchases,
  COUNT(DISTINCT non_flower_purchases) AS non_flower_purchases,
  SAFE_DIVIDE(COUNT(DISTINCT non_flower_purchases), COUNT(DISTINCT purchase_id)) AS non_flower_purchases_share,
  COUNT(DISTINCT first_purchases) AS first_purchases,
  SAFE_DIVIDE(COUNT(DISTINCT first_purchases), COUNT(DISTINCT purchase_id)) AS first_purchases_share,
  COUNT(DISTINCT repeated_purchases) AS repeated_purchases,
  SAFE_DIVIDE(COUNT(DISTINCT repeated_purchases),COUNT(DISTINCT purchase_id)) AS repeated_purchases_share,
  SUM(revenue) AS revenue,
  SUM(promo_sum_rub) AS promo_cost,
  SUM(bonus_company) AS bonus_cost,
  SUM(revenue) - (SUM(promo_sum_rub) + SUM(bonus_company)) AS revenue_without_promo_bonus
FROM union_tables
WHERE promocode = '2GISFWTQ' AND partner = '2gis'
GROUP BY 1,2,3
ORDER BY partner,week ASC