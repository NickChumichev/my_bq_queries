WITH a AS ( -- фиксированный расход по интеграции
  SELECT
    LOWER(campaign) AS campaign, 
    SUM(CAST(cost_rub AS numeric)) AS cost
  FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`
  WHERE 1=1
  AND cost_rub IS NOT NULL -- у некоторых инфлюенсеров нет фиксированного расхода
  AND CAST(cost_rub AS NUMERIC) != 0
  GROUP BY 1
  ORDER BY cost ASC
  )
  , b AS (
  SELECT
    LOWER(s.campaign) AS campaign,
    COUNT(DISTINCT y.purchase_id) AS purchases,
    COUNT(DISTINCT IF(y.is_first_purchase = 1, y.purchase_id,NULL)) AS first_purchases,
    COUNT(DISTINCT y.user_id) AS users,
    COUNT(DISTINCT (IF(y.is_first_purchase = 1, y.user_id, NULL))) AS new_users, 
    SUM(y.product_price) AS revenue,
    SUM(y.promo_sum_rub) AS promo_cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s ON s.transactionid = y.purchase_id
  WHERE 1=1
  AND LOWER(s.campaign) IN (SELECT LOWER(campaign) FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`)
  GROUP BY 1
  UNION ALL
  SELECT
    LOWER(s.campaign) AS campaign,
    COUNT(DISTINCT y.purchase_id) AS purchases,
    COUNT(DISTINCT IF(y.is_first_purchase = 1, y.purchase_id,NULL)) AS first_purchases,
    COUNT(DISTINCT y.user_id) AS users,
    COUNT(DISTINCT (IF(y.is_first_purchase = 1, y.user_id, NULL))) AS new_users, 
    SUM(y.product_price) AS revenue,
    SUM(y.promo_sum_rub) AS promo_cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s ON s.transactionid = y.purchase_id
  WHERE 1=1
  AND LOWER(s.campaign) IN (SELECT LOWER(campaign) FROM `funnel-flowwow.BUSINESS_DM.influencers_gs_view`)
  AND s.country_from != 'Россия'
  GROUP BY 1
  )
  , c AS (
  SELECT -- соединить данные по промокодам и данные по интеграциям
    campaign,
    0 AS users,
    0 AS new_users,
    0 AS purchases,
    0 AS first_purchases,
    0 AS revenue,
    cost
  FROM a
    UNION ALL
  SELECT 
    campaign,
    users,
    new_users,
    purchases,
    first_purchases,
    revenue,
    promo_cost AS cost
  FROM b
  )
  SELECT
    campaign,
    SUM(users) AS users,
    SUM(new_users) AS new_users,
    SUM(purchases) AS purchases,
    SUM(first_purchases) AS first_purchases,
    SUM(revenue) AS revenue,
    SUM(cost) AS cost,
    SAFE_DIVIDE(SUM(cost) , SUM(new_users)) AS CAC_new_users,
    SAFE_DIVIDE(SUM(revenue) , SUM(new_users)) AS LTV_new_users
  FROM c
  WHERE campaign IS NOT NULL AND campaign NOT IN ('-')
  GROUP BY 1