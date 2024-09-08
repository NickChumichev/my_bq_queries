WITH a AS (         
SELECT -- данные по vk_ads
  date,
  CONCAT('vk_ads') AS source,
  SUM(cost) as cost,
  SUM(clicks) as clicks,
  SUM(impressions) as impressions,
FROM (
SELECT 
  date,   
  1.2*cost as cost, -- расход в рублях с НДС
  clicks, 
  impressions,
FROM `funnel-flowwow.VK_ADS_RAW.vk_ads_cost_data`
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
GROUP BY 1

UNION ALL 

SELECT -- данные по apple_search_ads
  date,
  CONCAT('apple_search_ads'),
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT 
  date, 
  campaign_name as campaign, 
  1.2*local_spend_amount as cost, -- расход в долларах с НДС
  taps as clicks, 
  impressions
FROM `funnel-flowwow.APPLE_SEARCH_ADS.ad_campaign_reports`
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
GROUP BY 1

UNION ALL

SELECT -- данные по всему yandex_direct
  date,
  CONCAT('yandex_direct'),
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,    
  CAST(Cost AS INT64)/1000000 AS cost, -- расход в рублях с НДС
  Clicks AS clicks, 
  SAFE_CAST(Impressions AS INT64) AS impressions
FROM `funnel-flowwow.YA_DIRECT_RAW.ya_direct_cost_data` 
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
GROUP BY 1

UNION ALL

SELECT -- данные по всему google_ads
  segments_date as date,
  CONCAT('google_ads'),
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  segments_date,
  "google" as source,
  1.2*metrics_cost_micros/1000000 as cost,-- расход в долларах с НДС
  metrics_clicks as clicks, 
  metrics_impressions as impressions
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*`
WHERE segments_date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))

UNION ALL

SELECT
segments_date,
"google" as source,
1.2*metrics_cost_micros/1000000 as cost, -- расход в долларах с НДС
metrics_clicks as clicks, 
metrics_impressions as impressions
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_CampaignBasicStats_*`
WHERE segments_date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
AND campaign_id NOT IN (SELECT DISTINCT campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*` ) 
)
GROUP BY 1

UNION ALL

SELECT -- данные по ya_promo
  date,
  CONCAT('ya_promo'),
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,    
  CAST(Cost AS INT64)/1000000 AS cost, -- расход в рублях с НДС
  Clicks AS clicks, 
  SAFE_CAST(Impressions AS INT64) AS impressions
FROM `funnel-flowwow.YA_DIRECT_RAW.ya_direct_cost_data` 
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
AND REGEXP_CONTAINS(LOWER(campaignname),"yapromo")
)
GROUP BY 1

UNION ALL

SELECT -- данные по vkontakte
  date,
  CONCAT('vkontakte'),
  SUM(cost) as cost,
  SUM(clicks) as clicks,
  SUM(impressions) as impressions,
FROM (
SELECT
  date,  
  1.2*CAST(adCost AS INT64) as cost, -- расход в рублях с НДС 
  adClicks as clicks, 
  impressions
FROM `funnel-flowwow.VK_ADS_INTERNAL_RAW.vk_internal_cost_data` 
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
GROUP BY 1

UNION ALL

SELECT -- данные по тикток
  date,
  CONCAT('tiktok'),
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT 
  date,
  1.2*CAST(spend AS INT64) as cost,
  clicks, 
  impressions
FROM `funnel-flowwow.TIKTOK_REPORTS.campaigns_report`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON EXTRACT(DATE FROM stat_time_day)=date
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
GROUP BY 1

UNION ALL

SELECT -- данные по facebook_ads
date,
  CONCAT('facebook_ads'),
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,  
  1.2*cost AS cost, 
  clicks, 
  impressions
FROM `funnel-flowwow.FACEBOOK_ADS_RAW.facebook_cost_data` 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
GROUP BY 1

UNION ALL

SELECT -- данные по smm_instagram_global
  date,
  smm_instagram_global,
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions
FROM (
SELECT 
  date,
  CONCAT('smm_instagram_global') AS smm_instagram_global,
  campaign_info,
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,
  ARRAY_TO_STRING([ad_account_name,campaign_name, adset_name,ad_name],"") AS campaign_info,  
  1.2*cost AS cost, 
  clicks, 
  impressions
FROM `funnel-flowwow.FACEBOOK_ADS_RAW.facebook_cost_data` 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
WHERE REGEXP_CONTAINS(LOWER(campaign_info),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign_info),"wrld|world")
GROUP BY 1,2,3
)
GROUP BY 1,2

UNION ALL

SELECT -- данные по smm_instagram
  date,
  smm_instagram,
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions
FROM (
SELECT 
  date,
  CONCAT('smm_instagram') AS smm_instagram,
  campaign_info,
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,
  ARRAY_TO_STRING([ad_account_name,campaign_name, adset_name,ad_name],"") AS campaign_info,  
  1.2*cost AS cost, 
  clicks, 
  impressions
FROM `funnel-flowwow.FACEBOOK_ADS_RAW.facebook_cost_data` 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
WHERE REGEXP_CONTAINS(LOWER(campaign_info),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign_info),"wrld|world")=false
GROUP BY 1,2,3
)
GROUP BY 1,2

UNION ALL

SELECT -- фиксированные ads_cost
  date,
  segment,
  SUM(IF(channel NOT IN ("analytics","retention") AND segment NOT IN ("creatives","offline_merch","serm","all_cashback","all_promocode_integration","pr_rus","pr_international","donate","incentive_traffic"),ads_cost,0)) AS cost,
  0 AS clicks,
  0 AS impressions
FROM  `funnel-flowwow.PRODUCTION_DM.cm_data_by_campaigns`
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
AND  REGEXP_CONTAINS(LOWER(segment),"vk_ads|smm_vk|facebook_ads|other|google_ads|regular_online|yandex_direct|tiktok|asa|ya_promo|smm_instagram_global|smm_instagram")=false
GROUP BY 1,2

UNION ALL 

SELECT -- фиксированные service_cost 
  date,
  segment,
  SUM(IF(channel IN ("analytics","retention") OR segment="incentive_traffic",service_cost,0)) AS cost,
  0 AS clicks,
  0 AS impressions
FROM  `funnel-flowwow.PRODUCTION_DM.cm_data_by_campaigns`
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
AND  REGEXP_CONTAINS(LOWER(segment),"vk_ads|smm_vk|facebook_ads|other|google_ads|regular_online|yandex_direct|tiktok|asa|ya_promo|smm_instagram_global|smm_instagram")=false
GROUP BY 1,2

UNION ALL 

SELECT --фиксированные not_ads_cost 
  date,
  segment,
  SUM(IF(segment IN ("creatives","offline_merch","serm","all_cashback","all_promocode_integration","pr_rus","pr_international","donate"),not_ads_cost,0)) AS cost,
  0 AS clicks,
  0 AS impressions
FROM  `funnel-flowwow.PRODUCTION_DM.cm_data_by_campaigns`
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
AND  REGEXP_CONTAINS(LOWER(segment),"vk_ads|smm_vk|facebook_ads|other|google_ads|regular_online|yandex_direct|tiktok|asa|ya_promo|smm_instagram_global|smm_instagram")=false
GROUP BY 1,2
)
SELECT
  source,
  SUM(cost) AS cost,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM a
WHERE date BETWEEN '2024-02-02' AND '2024-02-09' 
GROUP BY 1