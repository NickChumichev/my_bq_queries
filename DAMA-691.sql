WITH a AS ( -- фиксированный расход по интеграции
  SELECT
    DATE_TRUNC(PARSE_DATE('%m/%d/%Y', date),MONTH) AS month, 
    country, --отделяет интеграции и расход друг от друга
    SUM(CAST(cost_rub AS numeric)) AS cost
  FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`
  WHERE 1=1
  AND cost_rub IS NOT NULL -- у некоторых инфлюенсеров нет расхода
  AND CAST(cost_rub AS NUMERIC) != 0
  GROUP BY 1,2
  ORDER BY month ASC
  )
  , b AS (
  SELECT --расходы по промокодам
    DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS month,
    country_from, --страна расхода по промокоду
    COUNT(DISTINCT purchase_id) AS purchases,
    COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id,NULL)) as first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(is_first_purchase = 1, user_id, NULL))) AS new_users, 
    SUM(product_price) as revenue,
    SUM(promo_sum_rub) AS promo_cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  WHERE LOWER(promocode) IN (SELECT LOWER(personal_promocode) FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`)
  GROUP BY 1,2
  UNION ALL
  SELECT --расходы по промокодам от российских блогеров
    DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS month,
    country_from, --страна расхода по промокоду
    COUNT(DISTINCT purchase_id) AS purchases,
    COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id,NULL)) as first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(is_first_purchase = 1, user_id, NULL))) AS new_users, 
    SUM(product_price) as revenue,
    SUM(promo_sum_rub) AS promo_cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  WHERE LOWER(promocode) IN (SELECT LOWER(personal_promocode) FROM `funnel-flowwow.BUSINESS_DM.influencers_gs_view`)
  AND country_from != 'Россия'
  GROUP BY 1,2
  UNION ALL
  SELECT -- транзакции по интеграциям без промокодов
    DATE_TRUNC(DATE(s.date),MONTH) AS month,
    s.country_from,
    COUNT(DISTINCT s.transactionid) AS purchases,
    COUNT(DISTINCT IF(s.is_first_purchase = 1, s.transactionid,NULL)) AS first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(s.is_first_purchase = 1, user_id, NULL))) AS new_users,
    SUM(s.purchase_sum_rub) AS revenue,
    0 AS cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON s.transactionid = y.purchase_id 
  WHERE LOWER(campaign) IN (SELECT LOWER(campaign) FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`)
  AND s.promocode IN ('')
  GROUP BY 1,2
  UNION ALL
  SELECT -- транзакции по интеграциям зарубежных блогеров без промокодов
    DATE_TRUNC(DATE(s.date),MONTH) AS month,
    s.country_from,
    COUNT(DISTINCT s.transactionid) AS purchases,
    COUNT(DISTINCT IF(s.is_first_purchase = 1, s.transactionid,NULL)) AS first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(s.is_first_purchase = 1, user_id, NULL))) AS new_users,
    SUM(s.purchase_sum_rub) AS revenue,
    0 AS cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON s.transactionid = y.purchase_id 
  WHERE LOWER(campaign) IN (SELECT LOWER(campaign) FROM `funnel-flowwow.BUSINESS_DM.influencers_gs_view`)
  AND s.promocode IN ('') AND s.country_from != 'Россия'
  GROUP BY 1,2
  )
  , c AS (
  SELECT -- соединить данные по промокодам и данные по интеграциям
    month,
    country,
    0 AS users,
    0 AS new_users,
    0 AS purchases,
    0 AS first_purchases,
    0 AS revenue,
    cost
  FROM a
    UNION ALL
  SELECT 
    month,
    country_from,
    users,
    new_users,
    purchases,
    first_purchases,
    revenue,
    promo_cost AS cost
  FROM b
  )
  SELECT
    month,
    country,
    SUM(users) AS users,
    SUM(new_users) AS new_users,
    SUM(purchases) AS purchases,
    SUM(first_purchases) AS first_purchases,
    SUM(revenue) AS revenue,
    SUM(cost) AS cost,
    SAFE_DIVIDE(SUM(cost) , SUM(new_users)) AS CAC_new_users,
    SAFE_DIVIDE(SUM(revenue) , SUM(new_users)) AS LTV_new_users
  FROM c
  GROUP BY 1,2
  ORDER BY country, month ASC