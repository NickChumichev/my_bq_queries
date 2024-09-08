WITH
costs AS
  (
  SELECT
date,
LOWER(source) as source,
LOWER(medium) as medium,
LOWER(IF(campaign IS NULL,"none", campaign)) as campaign,
campaign_id,
SUM(cost) as cost,
SUM(clicks) as clicks,
SUM(impressions) as impressions,
LOWER(IF(campaign_info IS NULL, "none", campaign_info)) as campaign_info
FROM (
SELECT 
date, source, medium, CAST(campaignId AS INT64) as campaign_id,campaignName as campaign, 1.2*CAST(adCost AS INT64) as cost, adClicks as clicks, impressions,
ARRAY_TO_STRING([campaign,adContent,campaignName,"smm_vk"],"") as campaign_info
FROM `funnel-flowwow.VK_ADS_INTERNAL_RAW.vk_internal_cost_data` 
WHERE date>="2022-01-01"

UNION ALL

SELECT 
date, source, medium, CAST(campaignId AS INT64) as campaign_id, campaignName as campaign, 1.2*CAST(adCost AS INT64) as cost, 
0 as clicks, 0 as impressions,
ARRAY_TO_STRING([campaign,adContent,campaignName,"smm_vk"],"") as campaign_info
FROM `funnel-flowwow.OWOXBI_vk.vk_OWOXAdCostData` 
WHERE date<"2022-01-01"


UNION ALL

SELECT date, "vk_ads" as source, null as medium, CAST(group_id AS INT64) as campaign_id, group_name as campaign, 1.2*cost as cost,
clicks, impressions,
ARRAY_TO_STRING([campaign_name,group_name,banner_name],"") as campaign_info FROM `funnel-flowwow.VK_ADS_RAW.vk_ads_cost_data` 


UNION ALL

SELECT 
EXTRACT(DATE FROM stat_time_day) as date, "tiktok" as source, null as medium, CAST(campaign_id AS INT64), CONCAT(campaign_name," ","(",campaign_id,")") as campaign, 1.2*IF(usd_rub IS NULL,70,usd_rub)*CAST(spend AS INT64) as cost,
clicks, impressions,
campaign_name as campaign_info
FROM `funnel-flowwow.TIKTOK_REPORTS.campaigns_report`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON EXTRACT(DATE FROM stat_time_day)=date


UNION ALL

SELECT 
date, "apple_search_ads" as source, null as medium, CAST(campaign_id AS INT64), CONCAT(campaign_name," ","(",campaign_id,")") as campaign, local_spend_amount*IF(local_spend_currency="USD",IF(usd_rub IS NULL,70,usd_rub),1) as cost, 
taps as clicks, impressions,
campaign_name as campaign_info
FROM `funnel-flowwow.APPLE_SEARCH_ADS.ad_campaign_reports`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)


UNION ALL

SELECT 
PARSE_DATE("%Y%m%d", date) as date,
source, medium, CAST(campaignId AS INT64) as campaign_id, campaign, 1.2*CAST(adCost AS INT64) as cost, 
0 as clicks, 0 as impressions,
ARRAY_TO_STRING([campaign,adContent,adGroup,campaignName,adGroupName],"") as campaign_info
FROM `funnel-flowwow.OWOXBI_CostData.CostData_*` 
WHERE source IN ("google") AND PARSE_DATE("%Y%m%d", date)<"2023-01-01"

UNION ALL

SELECT 
Date as date,
"google" as source,
"cpc" as medium,
CAST(CampaignId AS INT64) as campaign_id, 
CampaignName as campaign,
1.2*IF(usd_rub IS NULL,70,usd_rub)*Cost/1000000 as cost,
Clicks as clicks, Impressions as impressions,
ARRAY_TO_STRING([CampaignName,AdGroupName],"") as campaign_info
FROM `funnel-flowwow.GoogleAds.p_AdGroupBasicStats_*`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)

LEFT JOIN (SELECT AdGroupName, AdGroupId, CampaignId,CampaignName
FROM 
(SELECT AdGroupName, AdGroupId, CampaignId FROM `funnel-flowwow.GoogleAds.p_AdGroup_*` GROUP BY 1,2,3) 
LEFT JOIN
(SELECT CampaignName, CampaignId FROM `funnel-flowwow.GoogleAds.p_Campaign_*` GROUP BY 1,2)
USING(CampaignId) QUALIFY ROW_NUMBER() OVER(PARTITION BY AdGroupId, CampaignId)=1) USING(AdGroupId,CampaignId)
WHERE date>="2023-01-01" AND ExternalCustomerId IN(7836885233,5909223199)
AND date<"2023-05-01"

UNION ALL 

SELECT 
Date as date,
"google" as source,
"cpc" as medium,
CAST(CampaignId AS INT64) as campaign_id,
CampaignName as campaign,
1.2*IF(usd_rub IS NULL,70,usd_rub)*Cost/1000000 as cost,
Clicks as clicks, Impressions as impressions,
CampaignName as campaign_info
FROM `funnel-flowwow.GoogleAds.p_CampaignBasicStats_*` 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)


LEFT JOIN (SELECT CampaignName, CampaignId FROM `funnel-flowwow.GoogleAds.p_Campaign_*` GROUP BY 1,2
QUALIFY ROW_NUMBER() OVER(PARTITION BY CampaignId ORDER BY LENGTH(CampaignName) DESC)=1)
USING(CampaignId)


WHERE date>="2023-01-01" AND CampaignId NOT IN (SELECT DISTINCT CampaignId FROM `funnel-flowwow.GoogleAds.p_AdGroupBasicStats_*` ) AND ExternalCustomerId IN(7836885233,5909223199)
AND date<"2023-05-01"

UNION ALL

SELECT 
segments_date as date,
"google" as source,
"cpc" as medium,
CAST(campaign_id AS INT64) as campaign_id, 
campaign_name as campaign,
1.2*IF(usd_rub IS NULL,70,usd_rub)*metrics_cost_micros/1000000 as cost,
metrics_clicks as clicks, metrics_impressions as impressions,
ARRAY_TO_STRING([campaign_name,ad_group_name],"") as campaign_info
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON segments_date=date
LEFT JOIN (SELECT ad_group_name, ad_group_id, campaign_id,campaign_name
FROM 
(SELECT ad_group_name, ad_group_id, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroup_*` GROUP BY 1,2,3) 
LEFT JOIN
(SELECT campaign_name, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*` GROUP BY 1,2)
USING(campaign_id) QUALIFY ROW_NUMBER() OVER(PARTITION BY ad_group_id, campaign_id)=1) USING(ad_group_id,campaign_id)
WHERE date>="2023-05-01" AND customer_id IN(7836885233,5909223199,8228670821,3794438144,5342632145,9286752693,3322772858)
AND date>="2023-05-01"

UNION ALL 

SELECT
segments_date as date,
"google" as source,
"cpc" as medium,
CAST(campaign_id AS INT64) as campaign_id,
campaign_name as campaign,
1.2*IF(usd_rub IS NULL,70,usd_rub)*metrics_cost_micros/1000000 as cost,
metrics_clicks as clicks, metrics_impressions as impressions,
campaign_name as campaign_info
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_CampaignBasicStats_*`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON segments_date=date
LEFT JOIN (SELECT campaign_name, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*` GROUP BY 1,2
QUALIFY ROW_NUMBER() OVER(PARTITION BY campaign_id ORDER BY LENGTH(campaign_name) DESC)=1)
USING(campaign_id)
WHERE segments_date>="2023-05-01" AND campaign_id NOT IN (SELECT DISTINCT campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*` ) AND customer_id IN(7836885233,5909223199,8228670821,3794438144,5342632145,9286752693,3322772858)
AND segments_date>="2023-05-01"



UNION ALL

SELECT Date as date, "yandex" as source, "cpc" as medium, CAST(CampaignId AS INT64) as campaign_id,
CASE 
WHEN REGEXP_CONTAINS(LOWER(CampaignName),'app|uac|android|ios') THEN CONCAT(CampaignId,"_",REPLACE(REPLACE(CampaignName,".","_"),"-","_")) 
ELSE CONCAT(CampaignName, "|id-", CampaignId, "|pl-", LOWER(Device))
END as campaign, 

CAST(Cost AS INT64)/1000000 as cost,
Clicks as clicks, SAFE_CAST(Impressions AS INT64) as impressions,
ARRAY_TO_STRING([CONCAT(CampaignName, "|id-", CampaignId, "|pl-", LOWER(Device)), AdGroupName],"") as campaign_info
FROM `funnel-flowwow.YA_DIRECT_RAW.ya_direct_cost_data` 
WHERE Date>="2023-01-01"
UNION ALL
SELECT Date as date, "yandex" as source, "cpc" as medium, CAST(CampaignId AS INT64) as campaign_id, CampaignName as campaign, CAST(Cost AS INT64) as cost, 
0 as clicks, 0 as impressions,
ARRAY_TO_STRING([CampaignName, AdGroupName],"") as campaign_info
FROM `funnel-flowwow.OWOX_AdCostData_Yandex_Direct.yandex_direct_AdCostData` 
WHERE Date<"2023-01-01"




UNION ALL

SELECT date, IF(source IS NULL,"mycom",source) as source, IF(medium IS NULL,"cpc",medium) as medium, campaignId as campaign_id, campaign, 1.2*CAST(adCost AS INT64) as cost, 
adClicks as clicks, impressions,
ARRAY_TO_STRING([campaign, adContent],"") as campaign_info
FROM `funnel-flowwow.MYTARGET_RAW.mytarget_cost_data` 
--`funnel-flowwow.OWOXBI_my_target.my_target_OWOXAdCostData` 






UNION ALL

SELECT date, "facebook" as source, "null" as medium, campaign_id, campaign_name, IF(usd_rub IS NULL,70,usd_rub)*IF(account_id=509197767245431,1.2,1)*cost as cost, 
clicks, impressions,
ARRAY_TO_STRING([ad_account_name,campaign_name, adset_name,ad_name],"") as  campaign_info
FROM `funnel-flowwow.FACEBOOK_ADS_RAW.facebook_cost_data` 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)

UNION ALL

SELECT 
Date as date, "google" as source, null as medium, CAST(b.CampaignId AS INT64) as campaign_id, CampaignName as campaign, 1.2*CAST(Cost AS INT64)*IF(usd_rub IS NULL,70,usd_rub)/1000000 as cost, 
Clicks as clicks, Impressions as impressions,
CampaignName as campaign_info
FROM `funnel-flowwow.GoogleAds.p_CampaignBasicStats_2771316133` as a
LEFT JOIN 
(SELECT  
CampaignId, CampaignName
FROM `funnel-flowwow.GoogleAds.p_Campaign_2771316133` 
GROUP BY CampaignId, CampaignName) as b
ON a.CampaignId=b.CampaignId
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)
WHERE Date<"2023-05-01"

UNION ALL

SELECT 
Day as date,
"google" as source,
null as medium,
0 as campaign_id,
Campaign as campaign, 
Cost_Nds_Rub as cost,
0 as clicks, 0 as impressions,
ARRAY_TO_STRING([Ad_group, Campaign],"") as  campaign_info -- нет campaign_id
FROM `funnel-flowwow.UAC_RAW.exist_users_cost` 

UNION ALL

SELECT
segments_date as date, "google" as source, null as medium, CAST(a.campaign_id AS INT64) AS campaign_id, 

CASE
WHEN customer_id=7965936301 OR REGEXP_CONTAINS(LOWER(campaign_name),'uac|ios|android|app') THEN CONCAT(campaign_name," ","(",a.campaign_id,")")
ELSE
campaign_name END as campaign, 

1.2*metrics_cost_micros*IF(usd_rub IS NULL,70,usd_rub)/1000000 as cost, 
metrics_clicks as clicks, metrics_impressions as impressions,
campaign_name as campaign_info
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_CampaignBasicStats_*` as a
LEFT JOIN
(SELECT  
campaign_id, campaign_name
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*`
GROUP BY campaign_id, campaign_name) as b
ON a.campaign_id=b.campaign_id
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON segments_date=date
WHERE segments_date>="2023-05-01" AND customer_id IN(2771316133,9347526252,1847327805,7965936301)

)
WHERE date>="2021-01-01"
GROUP BY date, source, medium, campaign_id, campaign, campaign_info
)
, old_new AS ( 
SELECT
  date,
  source,
  medium,
  campaign_id,
  LOWER(campaign) AS campaign,
  LOWER(campaign_info) AS campaign_info,
  CASE
    WHEN REGEXP_CONTAINS(LOWER(campaign),"rename") THEN REGEXP_EXTRACT(campaign, r"rename\D([0-9]+)")
    ELSE NULL
    END AS rename, --только для google
SUM(cost) AS cost,
SUM(clicks) AS clicks,
SUM(impressions) AS impressions
FROM costs
GROUP BY 1,2,3,4,5,6,7
)
-- , old_new_rk AS (
SELECT
  date,
  source,
  medium,
  campaign_id,
  campaign,
  campaign_info,
  rename,
  cost,
  clicks,
  impressions,
  -- ROW_NUMBER() OVER (PARTITION BY date, source, medium, campaign_id, CAST(cost AS STRING), CAST(clicks AS STRING), CAST(impressions AS STRING) ORDER BY rename DESC) AS row_num
FROM old_new
WHERE source = 'google'
AND date >= '2023-05-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY date, source, medium, campaign_id, CAST(cost AS STRING) ORDER BY rename DESC) = 1 

UNION ALL

SELECT
  date,
  source,
  medium,
  campaign_id,
  campaign,
  campaign_info,
  'NULL' AS rename,
  cost,
  clicks,
  impressions,
  -- ROW_NUMBER() OVER (PARTITION BY date, source, medium, campaign_id, CAST(cost AS STRING), CAST(clicks AS STRING), CAST(impressions AS STRING) ORDER BY rename DESC) AS row_num
FROM costs
WHERE source = 'google'
AND date < '2023-05-01'

UNION ALL

SELECT
  date,
  source,
  medium,
  campaign_id,
  campaign,
  campaign_info,
  rename,
  cost,
  clicks,
  impressions
FROM old_new
WHERE source != 'google'
-- )
-- SELECT
--   date,
--   source,
--   medium,
--   campaign_id,
--   campaign,
--   rename,
--   SUM(cost) AS cost,
--   SUM(clicks) AS clicks,
--   SUM(impressions) AS impressions
-- FROM a
-- WHERE 1=1
-- AND REGEXP_CONTAINS(LOWER(campaign),'21066284079')
-- AND date BETWEEN '2024-08-01' AND '2024-08-03'
-- AND rename IS NULL
-- GROUP BY 1,2,3,4,5,6