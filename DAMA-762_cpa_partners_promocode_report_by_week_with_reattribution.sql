--partners_promocode_report_by_week_with_reattribution (версия c реатрибуцией)
WITH promo_contains AS (
SELECT
  partner,
   '' AS type,
  CAST(setting_id AS STRING) AS setting_id,
  LOWER(LEFT(promocode,16)) as partner_promo --взять промокоды из файлика без f_program
FROM funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view
WHERE promocode IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")=false
  )
, f_program AS (
SELECT
  '' AS partner,
  type,
  CAST(setting_id AS STRING) AS setting_id,
  LOWER(code) as partner_promo --промокоды программы лояльности c f_program
FROM funnel-flowwow.Analyt_ChumichevN.partner_promocodes_gs_view
RIGHT JOIN (SELECT code,type FROM funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes UNION ALL
SELECT code,type FROM funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes_log) ON promocode=type
WHERE promocode IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")
  )
 , union_tables AS (
SELECT DISTINCT -- объединить промокоды с данными по покупкам
  DATE_TRUNC(DATE(purchase_timestamp),WEEK(MONDAY)) AS week,
  CASE
    WHEN r.setting_id IS NOT NULL THEN r.setting_id
    WHEN am.type = 'sberbank' OR STARTS_WITH(LOWER(y.promocode),'drug22') THEN '3'
    WHEN am.type = 'union' THEN '4'
    WHEN am.type = 'primezone' OR (STARTS_WITH(LOWER(y.promocode),'pz24') OR STARTS_WITH(LOWER(y.promocode),'pz25')) THEN '88'
    ELSE NULL
  END AS setting_id,
  CASE    
    WHEN am.type = 'sberbank' OR STARTS_WITH(LOWER(y.promocode),'drug22') THEN 'Сбербанк Друг'
    WHEN am.type = 'primezone' OR (STARTS_WITH(LOWER(y.promocode),'pz24') OR STARTS_WITH(LOWER(y.promocode),'pz25')) THEN 'Primezone'
    WHEN am.type = 'union'  THEN 'Профсоюзы'
    WHEN partner_name = 'TinkoffSuperApp' AND NOT STARTS_WITH(LOWER(y.promocode),'flowbestb12') THEN 'Тинькофф супер апп'
    ELSE r.partner
    END AS partner,
    r.type,
    am.type,
    y.promocode,
    purchase_id,
    IF(acquisition in ('aso_organic', 'seo', 'sms','direct', 'flowwow_com') or acquisition is null, y.purchase_id, NULL) AS purchase_id_reattrib,
    IF(product_category_id != 1,purchase_id, NULL) AS non_flower_purchases,
    IF(y.is_first_purchase = 1, purchase_id, NULL) AS first_purchases,
    IF(y.is_first_purchase = 0, purchase_id, NULL) AS repeated_purchases,
    product_price AS revenue,
    y.promo_sum_rub,
    y.bonus_company
FROM funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company y
LEFT JOIN promo_contains r ON (STARTS_WITH(LOWER(promocode),LOWER(r.partner_promo)))
LEFT JOIN f_program am ON LOWER(promocode) = LOWER(am.partner_promo)
LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s ON purchase_id = transactionid AND  DATE_TRUNC(DATE(purchase_timestamp),WEEK(MONDAY)) = DATE_TRUNC(DATE(date),WEEK(MONDAY))
WHERE 1=1
AND (r.partner_promo IS NOT NULL OR am.partner_promo IS NOT NULL OR partner_name="TinkoffSuperApp")
AND DATE(purchase_timestamp) >= '2023-01-01'
ORDER BY partner
 )
SELECT
  week,
  partner,
  promocode,
  COUNT(DISTINCT purchase_id) AS purchases,
  COUNT(DISTINCT purchase_id_reattrib) AS purchases_reattrib,
  COUNT(DISTINCT non_flower_purchases) AS non_flower_purchases,
  COUNT(DISTINCT IF(purchase_id_reattrib IS NOT NULL,non_flower_purchases,NULL)) AS non_flower_purchases_reattrib,
  SAFE_DIVIDE(COUNT(DISTINCT non_flower_purchases), COUNT(DISTINCT purchase_id)) AS non_flower_purchases_share,
  SAFE_DIVIDE(COUNT(DISTINCT IF(purchase_id_reattrib IS NOT NULL,non_flower_purchases,NULL)), COUNT(DISTINCT purchase_id_reattrib)) AS non_flower_purchases_share_reattrib,
  COUNT(DISTINCT first_purchases) AS first_purchases,
  COUNT(DISTINCT IF(purchase_id_reattrib IS NOT NULL, first_purchases,NULL)) AS first_purchases_reattrib,
  SAFE_DIVIDE(COUNT(DISTINCT first_purchases), COUNT(DISTINCT purchase_id)) AS first_purchases_share,
  SAFE_DIVIDE(COUNT(DISTINCT IF(purchase_id_reattrib IS NOT NULL,first_purchases,NULL)), COUNT(DISTINCT purchase_id_reattrib)) AS first_purchases_share_reattrib,
  COUNT(DISTINCT repeated_purchases) AS repeated_purchases,
  COUNT(DISTINCT IF(purchase_id_reattrib IS NOT NULL, repeated_purchases,NULL)) AS repeated_purchases_reattrib,
  SAFE_DIVIDE(COUNT(DISTINCT repeated_purchases),COUNT(DISTINCT purchase_id)) AS repeated_purchases_share,
  SAFE_DIVIDE(COUNT(DISTINCT IF(purchase_id_reattrib IS NOT NULL,repeated_purchases,NULL)), COUNT(DISTINCT purchase_id_reattrib)) AS repeated_purchases_share_reattrib,
  SUM(revenue) AS revenue,
  SUM(IF(purchase_id_reattrib IS NOT NULL, revenue,NULL)) AS revenue_reattrib,
  SUM(promo_sum_rub) AS promo_cost,
  SUM(IF(purchase_id_reattrib IS NOT NULL, promo_sum_rub,NULL)) AS promo_cost_reattrib,
  SUM(bonus_company) AS bonus_cost,
  SUM(IF(purchase_id_reattrib IS NOT NULL, bonus_company,NULL)) AS bonus_cost_reattrib,
  SUM(revenue) - (SUM(promo_sum_rub) + SUM(bonus_company)) AS revenue_without_promo_bonus,
  SUM(IF(purchase_id_reattrib IS NOT NULL, revenue,NULL)) - (SUM(IF(purchase_id_reattrib IS NOT NULL, promo_sum_rub,NULL)) + SUM(IF(purchase_id_reattrib IS NOT NULL, bonus_company,NULL))) AS revenue_without_promo_bonus_reattrib
FROM union_tables
LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.f_partnership_promocode_report` ON LOWER(CAST(setting_id AS STRING)) = LOWER(CAST(id AS STRING))
WHERE category = 4
GROUP BY 1,2,3
ORDER BY partner,week ASC