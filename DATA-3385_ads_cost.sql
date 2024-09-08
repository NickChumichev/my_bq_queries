WITH a AS (
SELECT 
campaign_id,
segments_date as date,
"google" as source,
"cpc" as medium,
campaign_name as campaign,
1.2*IF(usd_rub IS NULL,70,usd_rub)*metrics_cost_micros/1000000 as cost,
metrics_clicks as clicks, metrics_impressions as impressions,
ARRAY_TO_STRING([campaign_name,ad_group_name],"") as campaign_info
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*`
LEFT JOIN funnel-flowwow.BUSINESS_DM.ii_exchange_rates ON segments_date=date
LEFT JOIN (SELECT ad_group_name, ad_group_id, campaign_id,campaign_name
FROM 
(SELECT ad_group_name, ad_group_id, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroup_*` GROUP BY 1,2,3) 
LEFT JOIN
(SELECT campaign_name, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*` GROUP BY 1,2)
USING(campaign_id) QUALIFY ROW_NUMBER() OVER(PARTITION BY ad_group_id, campaign_id)=1) USING(ad_group_id,campaign_id)
WHERE date>="2023-05-01" 
AND customer_id IN(7836885233,5909223199,8228670821,3794438144,5342632145,9286752693,3322772858)
AND date>="2023-05-01"

-- UNION ALL 

-- SELECT
-- segments_date as date,
-- "google" as source,
-- "cpc" as medium,
-- campaign_name as campaign,
-- 1.2*IF(usd_rub IS NULL,70,usd_rub)*metrics_cost_micros/1000000 as cost,
-- metrics_clicks as clicks, metrics_impressions as impressions,
-- campaign_name as campaign_info
-- FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_CampaignBasicStats_*`
-- LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON segments_date=date
-- LEFT JOIN (SELECT campaign_name, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*` GROUP BY 1,2
-- QUALIFY ROW_NUMBER() OVER(PARTITION BY campaign_id ORDER BY LENGTH(campaign_name) DESC)=1)
-- USING(campaign_id)
-- WHERE segments_date>="2023-05-01" AND campaign_id NOT IN (SELECT DISTINCT campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*` ) AND customer_id IN(7836885233,5909223199,8228670821,3794438144,5342632145,9286752693,3322772858)
-- AND segments_date>="2023-05-01"

-- UNION ALL

-- SELECT
-- segments_date as date, "google" as source, null as medium, 

-- CASE
-- WHEN customer_id=7965936301 OR REGEXP_CONTAINS(LOWER(campaign_name),'uac|ios|android|app') THEN CONCAT(campaign_name," ","(",a.campaign_id,")")
-- ELSE
-- campaign_name END as campaign, 

-- 1.2*metrics_cost_micros*IF(usd_rub IS NULL,70,usd_rub)/1000000 as cost, 
-- metrics_clicks as clicks, metrics_impressions as impressions,
-- campaign_name as campaign_info
-- FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_CampaignBasicStats_*` as a
-- LEFT JOIN
-- (SELECT  
-- campaign_id, campaign_name
-- FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*`
-- GROUP BY campaign_id, campaign_name) as b
-- ON a.campaign_id=b.campaign_id
-- LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON segments_date=date
-- WHERE segments_date>="2023-05-01" AND customer_id IN(2771316133,9347526252,1847327805,7965936301)
)
,
old_new AS
  (
  SELECT LOWER(string_field_0) as old_rk, LOWER(string_field_1) as new_rk
  FROM funnel-flowwow.BUSINESS_DM.old_new_rk_names_google_gs_view 
  WHERE string_field_0!="old_name"
  )
, b AS (
SELECT 
date,
campaign_id,
source,
medium,
-- LOWER(campaign) AS campaign,
LOWER(IF(source="google" AND medium="cpc"AND new_rk IS NOT NULL, new_rk,campaign)) as campaign,
LOWER(campaign_info) as campaign_info,
SUM(cost) as cost,
SUM(clicks) as clicks,
SUM(impressions) as impressions
FROM a
LEFT JOIN old_new ON campaign=old_rk
GROUP BY 1,2,3,4,5,6
)
SELECT
  -- customer_id,
  -- date,
  campaign_id,
  campaign,
  source,
  medium,
  SUM(cost)
FROM b
WHERE 1=1
AND date >= '2024-01-01' 
-- AND REGEXP_CONTAINS(LOWER(campaign),r'wrld-other_src_flowers_general-belgrade_en_geo') 
AND campaign_info LIKE '%cityid-1950030%'
GROUP BY 1,2,3,4