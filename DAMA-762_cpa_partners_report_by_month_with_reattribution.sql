--cpa_partners_report_by_month_with_reattribution (версия c реатрибуцией)
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
, join_tables AS (
SELECT DISTINCT -- объединить промокоды с данными по покупкам
  DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS month,
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
LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s ON purchase_id = transactionid AND  DATE_TRUNC(DATE(purchase_timestamp),MONTH) = DATE_TRUNC(DATE(date),MONTH) 
WHERE 1=1
AND (r.partner_promo IS NOT NULL OR am.partner_promo IS NOT NULL OR partner_name="TinkoffSuperApp")
AND DATE(purchase_timestamp) BETWEEN '2023-01-01' AND CURRENT_DATE()-1
ORDER BY partner
 )
 , list_of_partners AS ( -- получить партнера и промокод на каждую дату
SELECT DISTINCT
  DATE_TRUNC(date_generate,MONTH) AS month,
  setting_id,
  partner,
  promocode,
  0 AS purchases,
  0 AS purchases_reattrib,
  0 AS non_flower_purchases,
  0 AS non_flower_purchases_reattrib,
  0 AS non_flower_purchases_share,
  0 AS non_flower_purchases_share_reattrib,
  0 AS first_purchases,
  0 AS first_purchases_reattrib,
  0 AS first_purchases_share,
  0 AS first_purchases_share_reattrib,
  0 AS repeated_purchases,
  0 AS repeated_purchases_reattrib,
  0 AS repeated_purchases_share,
  0 AS repeated_purchases_share_reattrib,
  0 AS revenue,
  0 AS revenue_reattrib,
  0 AS promo_sum_rub,
  0 AS promo_sum_rub_reattrib,
  0 AS bonus_company,
  0 AS bonus_company_reattrib,
  0 AS revenue_without_promo_bonus,
  0 AS revenue_without_promo_bonus_reattrib
FROM UNNEST(GENERATE_DATE_ARRAY('2023-01-01', CURRENT_DATE()-1)) AS date_generate
CROSS JOIN join_tables
ORDER BY partner,promocode,month ASC
 )
 , union_tables AS ( --соединить данные по партнерам и промокодам
SELECT
  month,
  setting_id,
  partner,
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
FROM join_tables
GROUP BY 1,2,3

UNION ALL

SELECT
  month,
  setting_id,
  partner,
  purchases,
  purchases_reattrib,
  non_flower_purchases,
  non_flower_purchases_reattrib,
  non_flower_purchases_share,
  non_flower_purchases_share_reattrib,
  first_purchases,
  first_purchases_reattrib,
  first_purchases_share,
  first_purchases_share_reattrib,
  repeated_purchases,
  repeated_purchases_reattrib,
  repeated_purchases_share,
  repeated_purchases_share_reattrib,
  revenue,
  revenue_reattrib,
  promo_sum_rub AS promo_cost,
  promo_sum_rub_reattrib AS promo_cost_reattrib,
  bonus_company AS bonus_cost,
  bonus_company_reattrib AS bonus_cost_reattrib,
  revenue_without_promo_bonus,
  revenue_without_promo_bonus_reattrib
FROM list_of_partners
ORDER BY partner,month ASC
 )
 , delete_duplicate_rows AS (
SELECT -- удалить дубли
  month,
  setting_id,
  partner,
  purchases,
  purchases_reattrib,
  non_flower_purchases,
  non_flower_purchases_reattrib,
  non_flower_purchases_share,
  non_flower_purchases_share_reattrib,
  first_purchases,
  first_purchases_reattrib,
  first_purchases_share,
  first_purchases_share_reattrib,
  repeated_purchases,
  repeated_purchases_reattrib,
  repeated_purchases_share,
  repeated_purchases_share_reattrib,
  revenue,
  revenue_reattrib,
  promo_cost,
  promo_cost_reattrib,
  bonus_cost,
  bonus_cost_reattrib,
  revenue_without_promo_bonus,
  revenue_without_promo_bonus_reattrib
FROM union_tables
QUALIFY ROW_NUMBER() OVER (PARTITION BY month, partner ORDER BY purchases DESC) = 1
ORDER BY partner,month DESC
)
, real_begin_month AS (
SELECT -- получить неделю начала покупок
  month,
  setting_id,
  partner,
  purchases,
  FIRST_VALUE(month) OVER (PARTITION BY partner ORDER BY month rows between unbounded preceding and unbounded following) AS real_begin_month
FROM union_tables
WHERE 1=1
AND purchases != 0
ORDER BY partner,month DESC
)
SELECT -- убрать NULL
  s.month,
  s.partner,
  s.purchases,
  s.purchases_reattrib,
  s.non_flower_purchases,
  s.non_flower_purchases_reattrib,
  s.non_flower_purchases_share,
  s.non_flower_purchases_share_reattrib,
  s.first_purchases,
  s.first_purchases_reattrib,
  s.first_purchases_share,
  s.first_purchases_share_reattrib,
  s.repeated_purchases,
  s.repeated_purchases_reattrib,
  s.repeated_purchases_share,
  s.repeated_purchases_share_reattrib,
  s.revenue,
  s.revenue_reattrib,
  s.promo_cost,
  s.promo_cost_reattrib,
  s.bonus_cost,
  s.bonus_cost_reattrib,
  s.revenue_without_promo_bonus,
  s.revenue_without_promo_bonus_reattrib
FROM delete_duplicate_rows s
LEFT JOIN real_begin_month k ON s.month = k.month AND s.partner = k.partner
LEFT JOIN `funnel-flowwow.Analyt_ChumichevN.f_partnership_promocode_report` ON LOWER(CAST(s.setting_id AS STRING)) = LOWER(CAST(id AS STRING))
WHERE category = 4
QUALIFY s.month >= MAX(k.real_begin_month) OVER (PARTITION BY s.partner)
ORDER BY partner, month DESC