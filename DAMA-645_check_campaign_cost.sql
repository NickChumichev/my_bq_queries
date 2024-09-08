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
SELECT DISTINCT 
date,
source,
medium,
campaign_id,
IF(campaign_id !=0, LAST_VALUE(LOWER(campaign)) OVER (PARTITION BY campaign_id,source ORDER BY date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),campaign) AS campaign,
IF(campaign_id !=0, LAST_VALUE(LOWER(campaign_info)) OVER (PARTITION BY campaign_id,source ORDER BY date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),campaign) AS campaign_info,
cost,
clicks,
impressions
FROM costs
WHERE REGEXP_CONTAINS(LOWER(campaign_info),r'uac_wrld_tr_customer_android_firebase_purchase_enlang\|id-21001395491') 