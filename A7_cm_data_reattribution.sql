
create or replace  table
funnel-flowwow.Analyt_KosarevS.A7_cm_data_reattribution
partition by date 

as 
(WITH
cur_date AS (SELECT DATE(CURRENT_DATE()) as date),

/*
prediction AS
  (
  SELECT 
  date,subdivision,channel,segment,category,city_to,CAST(null AS STRING) as city_from, CAST(null AS STRING) as subcategory, CAST(null AS STRING) as platform, CAST(null AS STRING) as country_from, CAST(null AS STRING) as country_to, CAST(null AS STRING) as region, SUM(segment_revenue_prediction) as segment_revenue_prediction
--  ////////////
  FROM `funnel-flowwow.BUSINESS_DM.date_v_3_revenue_prediction`
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  ),
*/

ads_cost_by_traffic_actual AS
  (
  SELECT 
  date,subdivision,channel,segment,category,subcategory,city_to,country_to,platform, CAST(null AS STRING) as city_from, CAST(null AS  STRING) as country_from, region, SUM(ads_cost_by_traffic) as ads_cost_by_traffic,
  SUM(not_ads_cost_by_traffic) as not_ads_cost_by_traffic, SUM(service_cost_by_traffic) as service_cost_by_traffic,
-- ////////////////
  FROM `funnel-flowwow.Analyt_KosarevS.A6_ads_cost_by_traffic`
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  ),
ads_cost_m AS
  (
  SELECT
  DATE_ADD(date, INTERVAL 1 MONTH) as date,subdivision,channel,segment,category,subcategory,city_to,country_to,platform, CAST(null AS STRING) as city_from, CAST(null AS  STRING) as country_from, region, SUM(ads_cost_by_traffic) as ads_cost_by_traffic_30_days_ago,
  SUM(not_ads_cost_by_traffic) as not_ads_cost_by_traffic_30_days_ago, SUM(service_cost_by_traffic) as service_cost_by_traffic_30_days_ago,
  FROM ads_cost_by_traffic_actual
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  HAVING date<=(SELECT date FROM cur_date)
  ),
ads_cost_y AS
  (
  SELECT
  DATE_ADD(date, INTERVAL 1 YEAR) as date,subdivision,channel,segment,category,subcategory,city_to,country_to, region, platform, CAST(null AS STRING) as city_from, CAST(null AS  STRING) as country_from, SUM(ads_cost_by_traffic) as ads_cost_by_traffic_year_ago,
  SUM(not_ads_cost_by_traffic) as not_ads_cost_by_traffic_year_ago, SUM(service_cost_by_traffic) as service_cost_by_traffic_year_ago
  FROM ads_cost_by_traffic_actual
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  HAVING date<=(SELECT date FROM cur_date)
  ),
date_v_1_table AS 
  (
  SELECT 
  date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region, category, subcategory, platform,
  SUM(ads_cost) as ads_cost, SUM(not_ads_cost) as not_ads_cost, SUM(service_cost) as service_cost, 
  SUM(clicks) as clicks, SUM(impressions) as impressions,
  SUM(transactions) as transactions, SUM(first_transactions) as first_transactions, SUM(revenue) as revenue, SUM(revenue_first_transactions) as revenue_first_transactions, SUM(promo_cost) as promo_cost , SUM(bonus_company) as bonus_company, SUM(first_traffic) as first_traffic, SUM(traffic) as traffic, SUM(purchases) as purchases, SUM(first_purchases) as first_purchases,
  SUM(revenue_from_first_365) as revenue_from_first_365
  FROM `funnel-flowwow.Analyt_KosarevS.A5_date_v_3_a`
  --WHERE date>=DATE_ADD(DATE_TRUNC(DATE((SELECT date FROM cur_date)), MONTH), INTERVAL -24 MONTH)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  ),
actual AS
  (
  SELECT  
  date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region, category, subcategory, platform, SUM(ads_cost) as ads_cost, SUM(not_ads_cost) as not_ads_cost, SUM(service_cost) as service_cost,
    SUM(clicks) as clicks, SUM(impressions) as impressions,
   SUM(transactions) as transactions, SUM(first_transactions) as first_transactions, SUM(revenue) as revenue, SUM(revenue_first_transactions) as revenue_first_transactions, SUM(promo_cost) as promo_cost , SUM(bonus_company) as bonus_company, SUM(first_traffic) as first_traffic, SUM(traffic) as traffic, SUM(purchases) as purchases, SUM(first_purchases) as first_purchases
  FROM date_v_1_table
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  ),
m AS
  (
  SELECT  
  DATE_ADD(date, INTERVAL 1 MONTH) as date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region, category, subcategory, platform, 
  
  SUM(ads_cost) as ads_cost_30_days_ago, 
  SUM(not_ads_cost) as not_ads_cost_30_days_ago, 
  SUM(service_cost) as service_cost_30_days_ago,
  SUM(clicks) as clicks_30_days_ago,
  SUM(impressions) as impressions_30_days_ago,
  SUM(transactions) as transactions_30_days_ago, 
  SUM(first_transactions) as first_transactions_30_days_ago,
  SUM(revenue) as revenue_30_days_ago,  
  SUM(revenue_first_transactions) as revenue_first_transactions_30_days_ago,
  SUM(promo_cost) as promo_cost_30_days_ago,
  SUM(bonus_company) as bonus_company_30_days_ago,
  SUM(first_traffic) as first_traffic_30_days_ago,
  SUM(traffic) as traffic_30_days_ago,
  SUM(purchases) as purchases_30_days_ago, 
  SUM(first_purchases) as first_purchases_30_days_ago,


  FROM date_v_1_table
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  HAVING date<=(SELECT date FROM cur_date)
  ),
y AS
  (
  SELECT  
  DATE_ADD(date, INTERVAL 1 YEAR) as date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region, category, subcategory, platform, 
  SUM(ads_cost) as ads_cost_year_ago, 
  SUM(not_ads_cost) as not_ads_cost_year_ago, 
  SUM(service_cost) as service_cost_year_ago,
  SUM(clicks) as clicks_year_ago,
  SUM(impressions) as impressions_year_ago,
  SUM(transactions) as transactions_year_ago, 
  SUM(first_transactions) as first_transactions_year_ago,
  SUM(revenue) as revenue_year_ago, 
  SUM(revenue_first_transactions) as revenue_first_transactions_year_ago,
  SUM(promo_cost) as promo_cost_year_ago,
  SUM(bonus_company) as bonus_company_year_ago,
  SUM(first_traffic) as first_traffic_year_ago,
  SUM(traffic) as traffic_year_ago,
  SUM(purchases) as purchases_year_ago, 
  SUM(first_purchases) as first_purchases_year_ago,
  SUM(revenue_from_first_365) as revenue_from_first_365_year_ago

  FROM date_v_1_table
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  HAVING date<=(SELECT date FROM cur_date)
  )

SELECT 
DATE(date) as date,subdivision,channel,segment,city_from,city_to,country_from,country_to, region, category,subcategory,platform,


IFNULL(ads_cost,0) as ads_cost, 
IFNULL(not_ads_cost,0) as not_ads_cost, 
IFNULL(service_cost,0) as service_cost, 
IFNULL(clicks,0) as clicks,
IFNULL(impressions,0) as impressions,
IFNULL(transactions,0) as transactions, 
IFNULL(first_transactions,0) as first_transactions, 
IFNULL(revenue,0) as revenue, 
IFNULL(revenue_first_transactions,0) as revenue_first_transactions, 
IFNULL(promo_cost,0) as promo_cost, 
IFNULL(bonus_company,0) as bonus_company, 
IFNULL(first_traffic,0) as first_traffic,
IFNULL(traffic,0) as traffic,
IFNULL(purchases,0) as purchases,
IFNULL(first_purchases,0) as first_purchases,

-- IFNULL(segment_revenue_prediction,0) as segment_revenue_prediction,
IFNULL(ads_cost_by_traffic,0) as ads_cost_by_traffic,
IFNULL(not_ads_cost_by_traffic,0) as not_ads_cost_by_traffic,
IFNULL(service_cost_by_traffic,0) as service_cost_by_traffic,


IFNULL(ads_cost_year_ago,0) as ads_cost_year_ago,
IFNULL(not_ads_cost_year_ago,0) as  not_ads_cost_year_ago,
IFNULL(service_cost_year_ago,0) as service_cost_year_ago,
IFNULL(transactions_year_ago,0) as transactions_year_ago,
IFNULL(first_transactions_year_ago,0) as first_transactions_year_ago,
IFNULL(revenue_year_ago,  0) as revenue_year_ago,
IFNULL(revenue_first_transactions_year_ago, 0) as revenue_first_transactions_year_ago,
IFNULL(promo_cost_year_ago, 0) as   promo_cost_year_ago,
IFNULL(bonus_company_year_ago,0) as bonus_company_year_ago,
IFNULL(traffic_year_ago, 0) as  traffic_year_ago,
IFNULL(first_traffic_year_ago,0) as first_traffic_year_ago,
IFNULL(ads_cost_by_traffic_year_ago,0) as ads_cost_by_traffic_year_ago,
IFNULL(not_ads_cost_by_traffic_year_ago,0) as not_ads_cost_by_traffic_year_ago,
IFNULL(service_cost_by_traffic_year_ago,0) as service_cost_by_traffic_year_ago,

IFNULL(ads_cost_30_days_ago, 0) as ads_cost_30_days_ago,
IFNULL(not_ads_cost_30_days_ago, 0) as not_ads_cost_30_days_ago,
IFNULL(service_cost_30_days_ago,0) as service_cost_30_days_ago,
IFNULL(transactions_30_days_ago, 0) as transactions_30_days_ago,
IFNULL(first_transactions_30_days_ago,0) as first_transactions_30_days_ago,
IFNULL(revenue_30_days_ago, 0) as revenue_30_days_ago,
IFNULL(revenue_first_transactions_30_days_ago,  0) as revenue_first_transactions_30_days_ago,
IFNULL(promo_cost_30_days_ago,0) as promo_cost_30_days_ago,
IFNULL(bonus_company_30_days_ago,0) as bonus_company_30_days_ago,
IFNULL(traffic_30_days_ago, 0) as   traffic_30_days_ago,
IFNULL(first_traffic_30_days_ago,0) as first_traffic_30_days_ago,
IFNULL(ads_cost_by_traffic_30_days_ago,0) as ads_cost_by_traffic_30_days_ago,
IFNULL(not_ads_cost_by_traffic_30_days_ago,0) as not_ads_cost_by_traffic_30_days_ago,
IFNULL(service_cost_by_traffic_30_days_ago,0) as service_cost_by_traffic_30_days_ago,
IFNULL(purchases_30_days_ago,0) as purchases_30_days_ago,
IFNULL(first_purchases_30_days_ago,0) as first_purchases_30_days_ago,
IFNULL(purchases_year_ago,0) as purchases_year_ago,
IFNULL(first_purchases_year_ago,0) as first_purchases_year_ago,
IFNULL(revenue_from_first_365_year_ago,0) as revenue_from_first_365_year_ago


FROM actual
FULL JOIN y USING(date, subdivision, channel, segment, city_from, city_to, country_from, region, country_to, category, subcategory, platform)
FULL JOIN m USING(date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region, category, subcategory, platform)
-- FULL JOIN prediction USING(date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region,category, subcategory, platform)
FULL JOIN ads_cost_by_traffic_actual USING(date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region,category, subcategory, platform)
FULL JOIN ads_cost_m USING(date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region,category, subcategory, platform)
FULL JOIN ads_cost_y USING(date, subdivision, channel, segment, city_from, city_to, country_from, country_to, region, category, subcategory, platform));