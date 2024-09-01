create or replace  table
funnel-flowwow.Analyt_KosarevS.A5_date_v_3_a
as 


(WITH
--------start of uac attribution
-- считаем показатели в разрезе даты и сегментов, платформы, заменяем юак на органику, чтобы не задваивалось
date_v_2_full AS
  (
  SELECT
  date,
  IF(segment="google_ads_uac" AND platform="ios app","organic_apps",subdivision) as subdivision,
  IF(segment="google_ads_uac" AND platform="ios app","aso_organic",channel) as channel,
  IF(segment="google_ads_uac" AND platform="ios app","aso_organic",segment) as segment,
  platform,
  category, subcategory, city_from, city_to, country_from, country_to, region,
  SUM(revenue) as revenue,
  SUM(revenue_first_transactions) as revenue_first_transactions,
  SUM(transactions) as transactions,
  SUM(first_transactions) as first_transactions,
  SUM(purchases) as purchases,
  SUM(first_purchases) as first_purchases,
  SUM(promo_cost) as promo_cost,
  SUM(bonus_company) as bonus_company,
  SUM(ads_cost) as ads_cost,
  SUM(not_ads_cost) as not_ads_cost,
  SUM(service_cost) as service_cost,
  SUM(clicks) as clicks,
  SUM(impressions) as impressions,
  SUM(traffic) as traffic,
  SUM(first_traffic) as first_traffic,
  SUM(revenue_from_first_365) as revenue_from_first_365
  FROM `funnel-flowwow.Analyt_KosarevS.A4_cm_data_by_campaigns`
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  ),
-- считаем первые 100 самых частых разреза
shares AS
  (
  SELECT date, category, subcategory, city_from, city_to, country_from, country_to, region, SAFE_DIVIDE(revenue, SUM(revenue) OVER(PARTITION BY date)) as revenue_share
  FROM (
  SELECT
  date,
  category, subcategory, city_from, city_to, country_from, country_to, region,
  ROW_NUMBER() OVER(PARTITION BY date ORDER BY SUM(revenue) DESC) as rn,
  SUM(revenue) as revenue
  FROM date_v_2_full
  WHERE segment="aso_organic" AND platform="ios app"
  GROUP BY 1,2,3,4,5,6,7,8
  QUALIFY ROW_NUMBER() OVER(PARTITION BY date ORDER BY revenue DESC)<=5)
  ),
uac_dates AS
  (
  SELECT  
  Day as date,
  SUM(s2s_ecoomerce_purchase_paid_value_by_t)*AVG(IF(usd_rub IS NULL,70,usd_rub)) as revenue,
  SUM(First_ecommerce_purchase_paid_by_time) as first_transactions,
  SUM(s2s_ecommerce_purchase_paid_by_time) as transactions,
  SAFE_DIVIDE(SUM(s2s_ecoomerce_purchase_paid_value_by_t)*AVG(IF(usd_rub IS NULL,70,usd_rub)),SUM(s2s_ecommerce_purchase_paid_by_time))*SUM(First_ecommerce_purchase_paid_by_time) as revenue_first_transactions
  FROM `funnel-flowwow.UAC_RAW.ios_conversion`
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON Day=date
  WHERE Day>="2023-01-01" AND REGEXP_CONTAINS(LOWER(campaign),"ретарг|ремарк|retarg|remark|exist")=false
  GROUP BY 1
  ),
uac_dates_retargeting AS
  (
  SELECT Day as date,
  SUM(s2s_ecoomerce_purchase_paid_value_by_t)*AVG(IF(usd_rub IS NULL,70,usd_rub)) as revenue,
  SUM(First_ecommerce_purchase_paid_by_time) as first_transactions,
  SUM(s2s_ecommerce_purchase_paid_by_time) as transactions,
  SAFE_DIVIDE(SUM(s2s_ecoomerce_purchase_paid_value_by_t)*AVG(IF(usd_rub IS NULL,70,usd_rub)),SUM(s2s_ecommerce_purchase_paid_by_time))*SUM(First_ecommerce_purchase_paid_by_time) as revenue_first_transactions
  FROM `funnel-flowwow.UAC_RAW.ios_conversion`
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON Day=date
  WHERE Day>="2023-01-01" AND REGEXP_CONTAINS(LOWER(campaign),"ретарг|ремарк|retarg|remark|exist")
  GROUP BY 1
  ),
uac_data_ready AS
  (
  SELECT
  date,
  "paid_apps" as subdivision,
  "google_ads_uac" as channel,
  "google_ads_uac" as segment,
  "ios app" as platform,
  category, subcategory, city_from, city_to,  country_from, country_to, region,
  revenue*revenue_share as revenue,
  revenue_first_transactions*revenue_share as revenue_first_transactions,
  transactions*revenue_share as transactions,
  first_transactions*revenue_share as first_transactions,
  0 as purchases,
  0 as first_purchases,
  0 as promo_cost,
  0 as bonus_company,
  0 as ads_cost,
  0 as not_ads_cost,
  0 as service_cost,
  0 as clicks,
  0 as impressions,
  0 as traffic,
  0 as first_traffic,
  0 as revenue_from_first_365
  FROM uac_dates
  FULL JOIN shares USING(date)
  ),
uac_data_ready_retargeting AS
  (
  SELECT
  date,
  "paid_apps" as subdivision,
  "retargeting" as channel,
  "google_ads_uac" as segment,
  "ios app" as platform,
  category, subcategory, city_from, city_to,  country_from, country_to, region,
  revenue*revenue_share as revenue,
  revenue_first_transactions*revenue_share as revenue_first_transactions,
  transactions*revenue_share as transactions,
  first_transactions*revenue_share as first_transactions,
  0 as purchases,
  0 as first_purchases,
  0 as promo_cost,
  0 as bonus_company,
  0 as ads_cost,
  0 as not_ads_cost,
  0 as service_cost,
  0 as clicks,
  0 as impressions,
  0 as traffic,
  0 as first_traffic,
  0 as revenue_from_first_365
  FROM uac_dates_retargeting
  FULL JOIN shares USING(date)
  ),
date_v_2_full_union_uac_data_ready AS
  (
  SELECT * FROM date_v_2_full
  UNION ALL
  SELECT * FROM uac_data_ready
  UNION ALL
  SELECT * FROM uac_data_ready_retargeting
  ),
uac_data_to_substruct_from_organic AS
  (
  SELECT * EXCEPT(subdivision, channel, segment), "organic_apps" as subdivision, "aso_organic" as channel,
  "aso_organic" as segment
  FROM uac_data_ready
  ),
uac_data_to_substruct_from_organic_retargeting AS
  (
  SELECT * EXCEPT(subdivision, channel, segment), "organic_apps" as subdivision, "aso_organic" as channel,
  "aso_organic" as segment
  FROM uac_data_ready_retargeting
  ),
ready_table AS
  (
  SELECT
  date_v_2_full_union_uac_data_ready.date,
  subdivision, channel, segment, platform, category, subcategory, city_from, city_to, country_from, country_to, region,
  IFNULL(date_v_2_full_union_uac_data_ready.revenue-IF(uac_data_to_substruct_from_organic.revenue IS NULL,0,uac_data_to_substruct_from_organic.revenue)-IF(uac_data_to_substruct_from_organic_retargeting.revenue IS NULL,0,uac_data_to_substruct_from_organic_retargeting.revenue),0) as revenue,
  IFNULL(date_v_2_full_union_uac_data_ready.revenue_first_transactions-IF(uac_data_to_substruct_from_organic.revenue_first_transactions IS NULL,0,uac_data_to_substruct_from_organic.revenue_first_transactions)-IF(uac_data_to_substruct_from_organic_retargeting.revenue_first_transactions IS NULL,0,uac_data_to_substruct_from_organic_retargeting.revenue_first_transactions),0) as revenue_first_transactions,
  IFNULL(date_v_2_full_union_uac_data_ready.transactions-IF(uac_data_to_substruct_from_organic.transactions IS NULL,0,uac_data_to_substruct_from_organic.transactions)-IF(uac_data_to_substruct_from_organic_retargeting.transactions IS NULL,0,uac_data_to_substruct_from_organic_retargeting.transactions),0) as transactions,
  IFNULL(date_v_2_full_union_uac_data_ready.first_transactions-IF(uac_data_to_substruct_from_organic.first_transactions IS NULL,0,uac_data_to_substruct_from_organic.first_transactions)-IF(uac_data_to_substruct_from_organic_retargeting.first_transactions IS NULL,0,uac_data_to_substruct_from_organic_retargeting.first_transactions),0) as first_transactions,
  IFNULL(date_v_2_full_union_uac_data_ready.purchases,0) as purchases,
  IFNULL(date_v_2_full_union_uac_data_ready.first_purchases,0) as first_purchases,
  IFNULL(date_v_2_full_union_uac_data_ready.promo_cost,0) as promo_cost,
  IFNULL(date_v_2_full_union_uac_data_ready.bonus_company,0) as bonus_company,
  IFNULL(date_v_2_full_union_uac_data_ready.ads_cost,0) as ads_cost,
  IFNULL(date_v_2_full_union_uac_data_ready.not_ads_cost,0) as not_ads_cost,
  IFNULL(date_v_2_full_union_uac_data_ready.service_cost,0) as service_cost,
  IFNULL(date_v_2_full_union_uac_data_ready.clicks,0) as clicks,
  IFNULL(date_v_2_full_union_uac_data_ready.impressions,0) as impressions,
  IFNULL(date_v_2_full_union_uac_data_ready.traffic,0) as traffic,
  IFNULL(date_v_2_full_union_uac_data_ready.first_traffic,0) as first_traffic,
  IFNULL(date_v_2_full_union_uac_data_ready.revenue_from_first_365,0) as revenue_from_first_365
  FROM date_v_2_full_union_uac_data_ready
  LEFT JOIN uac_data_to_substruct_from_organic
  USING(date,subdivision, channel, segment, platform, category, subcategory, city_from, city_to, country_from, country_to, region)
  LEFT JOIN uac_data_to_substruct_from_organic_retargeting
  USING(date,subdivision, channel, segment, platform, category, subcategory, city_from, city_to, country_from, country_to, region)
  ),
------------- end of uac_attribution
inf_data_ready AS
  (
  SELECT
  date,
  "brand" as subdivision,
  "influencers" as channel,
  "influencers" as segment,
  "ios app" as platform,
  category, subcategory, city_from, city_to,  country_from, country_to, region,
  revenue*revenue_share as revenue,
  revenue_first_transactions*revenue_share as revenue_first_transactions,
  transactions*revenue_share as transactions,
  first_transactions*revenue_share as first_transactions,
  0 as purchases,
  0 as first_purchases,
  0 as promo_cost,
  0 as bonus_company,
  0 as ads_cost,
  0 as not_ads_cost,
  0 as service_cost,
  0 as clicks,
  0 as impressions,
  0 as traffic,
  0 as first_traffic,
  0 as revenue_from_first_365
  FROM `funnel-flowwow.BUSINESS_DM.cm_influencers_extra_attribution` 
  FULL JOIN shares USING(date)
  ),
date_v_2_full_union_inf_data_ready AS
  (
  SELECT * FROM ready_table
  UNION ALL
  SELECT * FROM inf_data_ready
  ),
inf_data_to_substruct_from_organic AS
  (
  SELECT * EXCEPT(subdivision, channel, segment), "organic_apps" as subdivision, "aso_organic" as channel,
  "aso_organic" as segment
  FROM inf_data_ready
  )

  SELECT
  date_v_2_full_union_inf_data_ready.date,
  subdivision, channel, segment, platform, category, subcategory, city_from, city_to, country_from, country_to, region,
  IFNULL(date_v_2_full_union_inf_data_ready.revenue-IF(inf_data_to_substruct_from_organic.revenue IS NULL,0,inf_data_to_substruct_from_organic.revenue),0) as revenue,
  IFNULL(date_v_2_full_union_inf_data_ready.revenue_first_transactions-IF(inf_data_to_substruct_from_organic.revenue_first_transactions IS NULL,0,inf_data_to_substruct_from_organic.revenue_first_transactions),0) as revenue_first_transactions,
  IFNULL(date_v_2_full_union_inf_data_ready.transactions-IF(inf_data_to_substruct_from_organic.transactions IS NULL,0,inf_data_to_substruct_from_organic.transactions),0) as transactions,
  IFNULL(date_v_2_full_union_inf_data_ready.first_transactions-IF(inf_data_to_substruct_from_organic.first_transactions IS NULL,0,inf_data_to_substruct_from_organic.first_transactions),0) as first_transactions,
  IFNULL(date_v_2_full_union_inf_data_ready.purchases,0) as purchases,
  IFNULL(date_v_2_full_union_inf_data_ready.first_purchases,0) as first_purchases,
  IFNULL(date_v_2_full_union_inf_data_ready.promo_cost,0) as promo_cost,
  IFNULL(date_v_2_full_union_inf_data_ready.bonus_company,0) as bonus_company,
  IFNULL(date_v_2_full_union_inf_data_ready.ads_cost,0) as ads_cost,
  IFNULL(date_v_2_full_union_inf_data_ready.not_ads_cost,0) as not_ads_cost,
  IFNULL(date_v_2_full_union_inf_data_ready.service_cost,0) as service_cost,
  IFNULL(date_v_2_full_union_inf_data_ready.clicks,0) as clicks,
  IFNULL(date_v_2_full_union_inf_data_ready.impressions,0) as impressions,
  IFNULL(date_v_2_full_union_inf_data_ready.traffic,0) as traffic,
  IFNULL(date_v_2_full_union_inf_data_ready.first_traffic,0) as first_traffic,
  IFNULL(date_v_2_full_union_inf_data_ready.revenue_from_first_365,0) as revenue_from_first_365
  FROM date_v_2_full_union_inf_data_ready
  LEFT JOIN inf_data_to_substruct_from_organic
  USING(date,subdivision, channel, segment, platform, category, subcategory, city_from, city_to, country_from, country_to, region)
)
  ;