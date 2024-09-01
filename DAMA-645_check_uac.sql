WITH a AS (
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
SELECT DISTINCT
  -- date,
  campaign_id,
  -- IF(campaign_id !=0, LAST_VALUE(LOWER(campaign)) OVER (PARTITION BY campaign_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),campaign) AS campaign,
  -- IF(campaign_id !=0, LAST_VALUE(LOWER(campaign_info)) OVER (PARTITION BY campaign_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),campaign) AS campaign_info,  
  campaign,
  campaign_info,
  SUM(cost)
  -- SUM(cost) AS  cost
FROM a
WHERE REGEXP_CONTAINS(LOWER(campaign_info),r'uac_wrld_tr_customer_android_firebase_purchase_enlang\|id-21001395491') 
GROUP BY 1,2,3