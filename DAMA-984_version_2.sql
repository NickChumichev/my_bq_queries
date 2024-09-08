WITH
costs AS
  (
  SELECT
date,
LOWER(source) AS source,
LOWER(medium) AS medium,
LOWER(IF(campaign IS NULL,"none", campaign)) AS campaign,
campaign_id,
SUM(cost) AS cost,
SUM(clicks) AS clicks,
SUM(impressions) AS impressions,
LOWER(IF(campaign_info IS NULL, "none", campaign_info)) AS campaign_info
FROM (
SELECT 
date, source, medium, CAST(campaignId AS INT64) AS campaign_id,campaignName AS campaign, 1.2*CAST(adCost AS INT64) AS cost, adClicks AS clicks, impressions,
ARRAY_TO_STRING([campaign,adContent,campaignName,"smm_vk"],"") AS campaign_info
FROM `funnel-flowwow.VK_ADS_INTERNAL_RAW.vk_internal_cost_data` 
WHERE date>="2022-01-01"

UNION ALL

SELECT 
date, source, medium, CAST(campaignId AS INT64) AS campaign_id, campaignName AS campaign, 1.2*CAST(adCost AS INT64) AS cost, 
0 AS clicks, 0 AS impressions,
ARRAY_TO_STRING([campaign,adContent,campaignName,"smm_vk"],"") AS campaign_info
FROM `funnel-flowwow.OWOXBI_vk.vk_OWOXAdCostData` 
WHERE date<"2022-01-01"


UNION ALL

SELECT date, "vk_ads" AS source, null AS medium, CAST(group_id AS INT64) AS campaign_id, group_name AS campaign, 1.2*cost AS cost,
clicks, impressions,
ARRAY_TO_STRING([campaign_name,group_name,banner_name],"") AS campaign_info FROM `funnel-flowwow.VK_ADS_RAW.vk_ads_cost_data` 


UNION ALL

SELECT 
EXTRACT(DATE FROM tiktok.stat_time_day) AS date, "tiktok" AS source, null AS medium, CAST(tiktok.campaign_id AS INT64) AS campaign_id, CONCAT(tiktok.campaign_name," ","(",tiktok.campaign_id,")") AS campaign, 1.2*IF(exchange_rates_1.usd_rub IS NULL,70,exchange_rates_1.usd_rub)*CAST(tiktok.spend AS INT64) AS cost,
tiktok.clicks, tiktok.impressions,
tiktok.campaign_name AS campaign_info
FROM `funnel-flowwow.TIKTOK_REPORTS.campaigns_report` AS tiktok
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_1  ON EXTRACT(DATE FROM tiktok.stat_time_day)=exchange_rates_1.date


UNION ALL

SELECT 
asa.date, "apple_search_ads" AS source, null AS medium, CAST(asa.campaign_id AS INT64) AS campaign_id, CONCAT(asa.campaign_name," ","(",asa.campaign_id,")") AS campaign, asa.local_spend_amount*IF(asa.local_spend_currency="USD",IF(exchange_rates_2.usd_rub IS NULL,70,exchange_rates_2.usd_rub),1) AS cost, 
asa.taps AS clicks, asa.impressions,
asa.campaign_name AS campaign_info
FROM `funnel-flowwow.APPLE_SEARCH_ADS.ad_campaign_reports` AS asa
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_2 USING(date)


UNION ALL

SELECT 
PARSE_DATE("%Y%m%d", date) AS date,
source, medium, CAST(campaignId AS INT64) AS campaign_id, campaign, 1.2*CAST(adCost AS INT64) AS cost, 
0 AS clicks, 0 AS impressions,
ARRAY_TO_STRING([campaign,adContent,adGroup,campaignName,adGroupName],"") AS campaign_info
FROM `funnel-flowwow.OWOXBI_CostData.CostData_*` 
WHERE source IN ("google") AND PARSE_DATE("%Y%m%d", date)<"2023-01-01"

UNION ALL

SELECT 
googleads_1.Date AS date,
"google" AS source,
"cpc" AS medium,
CAST(googleads_1.CampaignId AS INT64) AS campaign_id, 
googleads_2.CampaignName AS campaign,
1.2*IF(exchange_rates_3.usd_rub IS NULL,70,exchange_rates_3.usd_rub)*googleads_1.Cost/1000000 AS cost,
googleads_1.Clicks AS clicks, googleads_1.Impressions AS impressions,
ARRAY_TO_STRING([googleads_2.CampaignName,googleads_2.AdGroupName],"") AS campaign_info
FROM `funnel-flowwow.GoogleAds.p_AdGroupBasicStats_*` AS googleads_1
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_3 USING(date)

LEFT JOIN (SELECT googleads_3.AdGroupName, googleads_3.AdGroupId, googleads_3.CampaignId,googleads_4.CampaignName
FROM 
(SELECT AdGroupName, AdGroupId, CampaignId FROM `funnel-flowwow.GoogleAds.p_AdGroup_*` GROUP BY 1,2,3) AS googleads_3 
LEFT JOIN
(SELECT CampaignName, CampaignId FROM `funnel-flowwow.GoogleAds.p_Campaign_*` GROUP BY 1,2)  AS googleads_4
USING(CampaignId) QUALIFY ROW_NUMBER() OVER(PARTITION BY AdGroupId, CampaignId)=1) AS googleads_2 USING(AdGroupId,CampaignId)
WHERE date>="2023-01-01" AND ExternalCustomerId IN(7836885233,5909223199)
AND date<"2023-05-01"

UNION ALL 

SELECT 
googleads_5.Date AS date,
"google" AS source,
"cpc" AS medium,
CAST(googleads_5.CampaignId AS INT64) AS campaign_id,
googleads_6.CampaignName AS campaign,
1.2*IF(exchange_rates_4.usd_rub IS NULL,70,exchange_rates_4.usd_rub)*googleads_5.Cost/1000000 AS cost,
googleads_5.Clicks AS clicks, googleads_5.Impressions AS impressions,
googleads_6.CampaignName AS campaign_info
FROM `funnel-flowwow.GoogleAds.p_CampaignBasicStats_*` AS googleads_5 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_4 USING(date)


LEFT JOIN (SELECT CampaignName, CampaignId FROM `funnel-flowwow.GoogleAds.p_Campaign_*` GROUP BY 1,2
QUALIFY ROW_NUMBER() OVER(PARTITION BY CampaignId ORDER BY LENGTH(CampaignName) DESC)=1) AS googleads_6
USING(CampaignId)


WHERE date>="2023-01-01" AND CampaignId NOT IN (SELECT DISTINCT CampaignId FROM `funnel-flowwow.GoogleAds.p_AdGroupBasicStats_*` ) AND ExternalCustomerId IN(7836885233,5909223199)
AND date<"2023-05-01"

UNION ALL

SELECT 
googleads_7.segments_date AS date,
"google" AS source,
"cpc" AS medium,
CAST(googleads_7.campaign_id AS INT64) AS campaign_id, 
googleads_8.campaign_name AS campaign,
1.2*IF(exchange_rates_5.usd_rub IS NULL,70,exchange_rates_5.usd_rub)*googleads_7.metrics_cost_micros/1000000 AS cost,
googleads_7.metrics_clicks AS clicks, googleads_7.metrics_impressions AS impressions,
ARRAY_TO_STRING([googleads_8.campaign_name,googleads_8.ad_group_name],"") AS campaign_info
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*` AS googleads_7
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_5 ON segments_date=date
LEFT JOIN (SELECT ad_group_name, ad_group_id, campaign_id,campaign_name
FROM 
(SELECT ad_group_name, ad_group_id, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroup_*` GROUP BY 1,2,3) AS googleads_9
LEFT JOIN
(SELECT campaign_name, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*` GROUP BY 1,2) AS googleads_10
USING(campaign_id) QUALIFY ROW_NUMBER() OVER(PARTITION BY ad_group_id, campaign_id)=1) AS googleads_8 USING(ad_group_id,campaign_id)
WHERE date>="2023-05-01" AND customer_id IN(7836885233,5909223199,8228670821,3794438144,5342632145,9286752693,3322772858)
AND date>="2023-05-01"

UNION ALL 

SELECT
googleads_11.segments_date AS date,
"google" AS source,
"cpc" AS medium,
CAST(googleads_11.campaign_id AS INT64) AS campaign_id,
googleads_12.campaign_name AS campaign,
1.2*IF(exchange_rates_6.usd_rub IS NULL,70,exchange_rates_6.usd_rub)*googleads_11.metrics_cost_micros/1000000 AS cost,
googleads_11.metrics_clicks AS clicks, googleads_11.metrics_impressions AS impressions,
googleads_12.campaign_name AS campaign_info
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_CampaignBasicStats_*` AS googleads_11
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_6 ON segments_date=date
LEFT JOIN (SELECT campaign_name, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*` GROUP BY 1,2
QUALIFY ROW_NUMBER() OVER(PARTITION BY campaign_id ORDER BY LENGTH(campaign_name) DESC)=1) AS googleads_12
USING(campaign_id)
WHERE segments_date>="2023-05-01" AND campaign_id NOT IN (SELECT DISTINCT campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*` ) AND customer_id IN(7836885233,5909223199,8228670821,3794438144,5342632145,9286752693,3322772858)
AND segments_date>="2023-05-01"



UNION ALL

SELECT Date AS date, "yandex" AS source, "cpc" AS medium, CAST(CampaignId AS INT64) AS campaign_id,
CASE 
WHEN REGEXP_CONTAINS(LOWER(CampaignName),'app|uac|android|ios') THEN CONCAT(CampaignId,"_",REPLACE(REPLACE(CampaignName,".","_"),"-","_")) 
ELSE CONCAT(CampaignName, "|id-", CampaignId, "|pl-", LOWER(Device))
END AS campaign, 

CAST(Cost AS INT64)/1000000 AS cost,
Clicks AS clicks, SAFE_CAST(Impressions AS INT64) AS impressions,
ARRAY_TO_STRING([CONCAT(CampaignName, "|id-", CampaignId, "|pl-", LOWER(Device)), AdGroupName],"") AS campaign_info
FROM `funnel-flowwow.YA_DIRECT_RAW.ya_direct_cost_data`
WHERE Date>="2023-01-01"
UNION ALL
SELECT Date AS date, "yandex" AS source, "cpc" AS medium, CAST(CampaignId AS INT64) AS campaign_id, CampaignName AS campaign, CAST(Cost AS INT64) AS cost, 
0 AS clicks, 0 AS impressions,
ARRAY_TO_STRING([CampaignName, AdGroupName],"") AS campaign_info
FROM `funnel-flowwow.OWOX_AdCostData_Yandex_Direct.yandex_direct_AdCostData` 
WHERE Date<"2023-01-01"




UNION ALL

SELECT date, IF(source IS NULL,"mycom",source) AS source, IF(medium IS NULL,"cpc",medium) AS medium, campaignId AS campaign_id, campaign, 1.2*CAST(adCost AS INT64) AS cost, 
adClicks AS clicks, impressions,
ARRAY_TO_STRING([campaign, adContent],"") AS campaign_info
FROM `funnel-flowwow.MYTARGET_RAW.mytarget_cost_data` 
--`funnel-flowwow.OWOXBI_my_target.my_target_OWOXAdCostData` 






UNION ALL

SELECT facebook.date, "facebook" AS source, "null" AS medium, facebook.campaign_id, facebook.campaign_name, IF(exchange_rates_7.usd_rub IS NULL,70,exchange_rates_7.usd_rub)*IF(facebook.account_id=509197767245431,1.2,1)*cost AS cost, 
facebook.clicks, facebook.impressions,
ARRAY_TO_STRING([facebook.ad_account_name,facebook.campaign_name, facebook.adset_name,facebook.ad_name],"") AS  campaign_info
FROM `funnel-flowwow.FACEBOOK_ADS_RAW.facebook_cost_data` AS facebook
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_7 USING(date)

UNION ALL

SELECT 
googleads_12.Date AS date, "google" AS source, null AS medium, CAST(googleads_13.CampaignId AS INT64) AS campaign_id, googleads_13.CampaignName AS campaign, 1.2*CAST(googleads_12.Cost AS INT64)*IF(exchange_rates_8.usd_rub IS NULL,70,exchange_rates_8.usd_rub)/1000000 AS cost, 
googleads_12.Clicks AS clicks, googleads_12.Impressions AS impressions,
googleads_13.CampaignName AS campaign_info
FROM `funnel-flowwow.GoogleAds.p_CampaignBasicStats_2771316133` AS googleads_12
LEFT JOIN 
(SELECT  
CampaignId, CampaignName
FROM `funnel-flowwow.GoogleAds.p_Campaign_2771316133` 
GROUP BY CampaignId, CampaignName) AS googleads_13
ON googleads_12.CampaignId=googleads_13.CampaignId
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_8 USING(date)
WHERE Date<"2023-05-01"

UNION ALL

SELECT 
Day AS date,
"google" AS source,
null AS medium,
0 AS campaign_id,
Campaign AS campaign, 
Cost_Nds_Rub AS cost,
0 AS clicks, 0 AS impressions,
ARRAY_TO_STRING([Ad_group, Campaign],"") AS  campaign_info -- нет campaign_id
FROM `funnel-flowwow.UAC_RAW.exist_users_cost` 

UNION ALL

SELECT
googleads_14.segments_date AS date, "google" AS source, null AS medium, CAST(googleads_14.campaign_id AS INT64) AS campaign_id, 

CASE
WHEN googleads_14.customer_id=7965936301 OR REGEXP_CONTAINS(LOWER(googleads_15.campaign_name),'uac|ios|android|app') THEN CONCAT(googleads_15.campaign_name," ","(",googleads_14.campaign_id,")")
ELSE
googleads_15.campaign_name END AS campaign, 

1.2*googleads_14.metrics_cost_micros*IF(exchange_rates_9.usd_rub IS NULL,70,exchange_rates_9.usd_rub)/1000000 AS cost, 
googleads_14.metrics_clicks AS clicks, googleads_14.metrics_impressions AS impressions,
googleads_15.campaign_name AS campaign_info
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_CampaignBasicStats_*` AS googleads_14
LEFT JOIN
(SELECT  
campaign_id, campaign_name
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*`
GROUP BY campaign_id, campaign_name) AS googleads_15
ON googleads_14.campaign_id=googleads_15.campaign_id
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` AS exchange_rates_9 ON segments_date=date
WHERE segments_date>="2023-05-01" AND customer_id IN(2771316133,9347526252,1847327805,7965936301)

) AS _costs
WHERE date>="2021-01-01"
GROUP BY date, source, medium, campaign_id, campaign, campaign_info
) 
, old_new AS
(
SELECT LOWER(string_field_0) AS old_rk, LOWER(string_field_1) AS new_rk
FROM `funnel-flowwow.GOOGLE_SHEETS.CGS_OLD_NEW_RK_NAMES_GOOGLE_GS_VIEW` 
WHERE string_field_0!="old_name"
)
, old_new_rk AS (
SELECT
_old_new_rk.date,
_old_new_rk.source,
_old_new_rk.medium,
_old_new_rk.campaign_id,
LOWER(IF(_old_new_rk.source="google" AND _old_new_rk.medium="cpc" AND _old_new.new_rk IS NOT NULL, _old_new.new_rk,campaign)) AS campaign,
LOWER(_old_new_rk.campaign_info) AS campaign_info,
SUM(_old_new_rk.cost) AS cost,
SUM(_old_new_rk.clicks) AS clicks,
SUM(_old_new_rk.impressions) AS impressions
FROM costs AS _old_new_rk
LEFT JOIN old_new AS _old_new ON _old_new_rk.campaign=_old_new.old_rk
GROUP BY 1,2,3,4,5,6
)
, rename_campaign AS
(
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
    END AS rename,
SUM(cost) AS cost,
SUM(clicks) AS clicks,
SUM(impressions) AS impressions
FROM old_new_rk
GROUP BY 1,2,3,4,5,6,7
)
SELECT
  date,
  source,
  medium,
  campaign_id,
  LOWER(campaign) AS campaign,
  LOWER(campaign_info) AS campaign_info,
  rename,
  cost,
  clicks,
  impressions,
FROM rename_campaign
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
  NULL AS rename,
  cost,
  clicks,
  impressions
FROM rename_campaign
WHERE source = 'google'
AND date < '2023-05-01'

UNION ALL

SELECT
  date,
  source,
  medium,
  campaign_id,
  LOWER(campaign) AS campaign,
  LOWER(campaign_info) AS campaign_info,
  NULL AS rename,
  cost,
  clicks,
  impressions
FROM rename_campaign
WHERE source != 'google'
  