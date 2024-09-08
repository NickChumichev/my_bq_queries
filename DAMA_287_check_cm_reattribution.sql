--расходы, клики, показы из рекламных кабинетов и фиксированных затрат
WITH a AS (
SELECT --агрегированные данные из рекламных кабинетов и фиксированных расходов
  date,
  channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions
FROM (
SELECT -- данные по vk_ads
  date,
  CONCAT('vk_ads') AS channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT 
  date,   
  1.2*cost as cost_rub, -- расход в рублях с НДС
  0 AS cost_usd,
  clicks, 
  impressions,
FROM `funnel-flowwow.VK_ADS_RAW.vk_ads_cost_data`
WHERE REGEXP_CONTAINS(LOWER(campaign_name),"алиса|flight|seller|courier|продав|курьер|бизнес")=false
)
GROUP BY 1

UNION ALL 

SELECT -- данные по apple_search_ads
  date,
  CONCAT('apple_search_ads') AS channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT 
  date, 
  campaign_name as campaign, 
  1.2*local_spend_amount as cost_usd, -- расход в долларах с НДС
  1.2*local_spend_amount*IF(local_spend_currency="USD",IF(usd_rub IS NULL,70,usd_rub),1) as cost_rub, -- расход в рублях с НДС
  taps as clicks, 
  impressions
FROM `funnel-flowwow.APPLE_SEARCH_ADS.ad_campaign_reports`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)
WHERE REGEXP_CONTAINS(LOWER(campaign_name),"flight|seller|courier|продав|курьер|бизнес")=false
)
GROUP BY 1

UNION ALL

SELECT -- данные по всему yandex_direct
  Date as date,
  CONCAT('yandex_direct') AS channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  Date,    
  CAST(Cost AS INT64)/1000000 AS cost_rub, -- расход в рублях с НДС
  0 AS cost_usd,
  Clicks AS clicks, 
  SAFE_CAST(Impressions AS INT64) AS impressions
FROM `funnel-flowwow.YA_DIRECT_RAW.ya_direct_cost_data` 
WHERE REGEXP_CONTAINS(LOWER(CampaignName),"flight|seller|courier|продав|курьер|бизнес")=false
AND REGEXP_CONTAINS(LOWER(AdGroupName),"flight|seller|courier|продав|курьер|бизнес")=false
)
GROUP BY 1

UNION ALL

SELECT -- content_fw_client
  date,
  CONCAT('content_fw_client'),
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT -- данные по ya_promo
  date,
  0 AS cost_usd,    
  CAST(Cost AS INT64)/1000000 AS cost_rub, -- расход в рублях с НДС
  Clicks AS clicks, 
  SAFE_CAST(Impressions AS INT64) AS impressions
FROM `funnel-flowwow.YA_DIRECT_RAW.ya_direct_cost_data` 
WHERE REGEXP_CONTAINS(LOWER(CampaignName),"flight|seller|courier|продав|курьер|бизнес")=false
AND REGEXP_CONTAINS(LOWER(campaignname),"yapromo")

UNION ALL

SELECT --dzen_client
  date,
   0 AS cost_usd,   
  1.2*cost as cost_rub, -- расход в рублях с НДС
  clicks, 
  impressions,
FROM `funnel-flowwow.VK_ADS_RAW.vk_ads_cost_data`
WHERE REGEXP_CONTAINS(LOWER(campaign_name),"алиса|flight|seller|courier|продав|курьер|бизнес")=false
AND REGEXP_CONTAINS(LOWER(campaign_name),"dzen_client")
)
GROUP BY 1

UNION ALL

SELECT -- данные по всему google_ads
  segments_date AS date,
  CONCAT('google_ads') AS channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  segments_date,
  "google" as source,
  1.2*metrics_cost_micros/1000000 as cost_usd,-- расход в долларах с НДС
  1.2*IF(usd_rub IS NULL,70,usd_rub)*metrics_cost_micros/1000000 as cost_rub, -- расход в рублях с НДС
  metrics_clicks as clicks, 
  metrics_impressions as impressions
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON segments_date=date
LEFT JOIN (SELECT ad_group_name, ad_group_id, campaign_id,campaign_name --получение названия кампаний
FROM 
(SELECT ad_group_name, ad_group_id, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroup_*` GROUP BY 1,2,3) 
LEFT JOIN
(SELECT campaign_name, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*` GROUP BY 1,2)
USING(campaign_id) QUALIFY ROW_NUMBER() OVER(PARTITION BY ad_group_id, campaign_id)=1) USING(ad_group_id,campaign_id)
WHERE REGEXP_CONTAINS(LOWER(campaign_name),"flight|seller|courier|продав|курьер|бизнес")=false

UNION ALL

SELECT
  segments_date,
  "google" as source,
  1.2*metrics_cost_micros/1000000 as cost_usd, -- расход в долларах с НДС
  1.2*IF(usd_rub IS NULL,70,usd_rub)*metrics_cost_micros/1000000 as cost_rub, -- расход в рублях с НДС
  metrics_clicks as clicks, 
  metrics_impressions as impressions
FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_CampaignBasicStats_*`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON segments_date=date --получение названия кампаний
LEFT JOIN (SELECT campaign_name, campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_Campaign_*` GROUP BY 1,2
QUALIFY ROW_NUMBER() OVER(PARTITION BY campaign_id ORDER BY LENGTH(campaign_name) DESC)=1)
USING(campaign_id)
WHERE campaign_id NOT IN (SELECT DISTINCT campaign_id FROM `funnel-flowwow.GOOGLE_ADS_NATIVE.p_ads_AdGroupBasicStats_*` )
AND REGEXP_CONTAINS(LOWER(campaign_name),"flight|seller|courier|продав|курьер|бизнес")=false 
)
GROUP BY 1

UNION ALL

SELECT -- данные по vkontakte
  date,
  CONCAT('smm_vk') AS channel,
  SUM(cost_rub) as cost_rub,
  SUM(cost_usd) as cost_usd,
  SUM(clicks) as clicks,
  SUM(impressions) as impressions,
FROM (
SELECT
  date,  
  1.2*CAST(adCost AS INT64) AS cost_rub, -- расход в рублях с НДС
  0 AS cost_usd,-- расход в долларах с НДС 
  adClicks as clicks, 
  impressions
FROM `funnel-flowwow.VK_ADS_INTERNAL_RAW.vk_internal_cost_data` 
WHERE REGEXP_CONTAINS(LOWER(campaignName),"алиса") --почему-то все кампании, которые содержат алису
)
GROUP BY 1

UNION ALL

SELECT -- данные по тикток
  date,
  CONCAT('tiktok_app') AS channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,  
  1.2*CAST(spend AS INT64) as cost_usd, -- расход в долларах с НДС
  1.2*IF(usd_rub IS NULL,70,usd_rub)*CAST(spend AS INT64) as cost_rub,-- расход в рублях с НДС
  clicks, 
  impressions
FROM `funnel-flowwow.TIKTOK_REPORTS.campaigns_report`
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` ON EXTRACT(DATE FROM stat_time_day)=date
)
GROUP BY 1

UNION ALL

SELECT -- данные по facebook_ads
  date,
  CONCAT('facebook_ads') AS channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,
  ARRAY_TO_STRING([ad_account_name,campaign_name, adset_name,ad_name],"") as  campaign_info,  
  1.2*cost AS cost_usd, -- расход в долларах с НДС
  IF(usd_rub IS NULL,70,usd_rub)*IF(account_id=509197767245431,1.2,1)*cost as cost_rub, -- расход в рублях с НДС
  clicks, 
  impressions
FROM `funnel-flowwow.FACEBOOK_ADS_RAW.facebook_cost_data` 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)
-- WHERE REGEXP_CONTAINS(LOWER(campaign_name),"flight|seller|courier|продав|курьер|бизнес")=false -- большое расхождение из-за этого фильтра получается
)
WHERE REGEXP_CONTAINS(LOWER(campaign_info),"post-boosting")=false
AND REGEXP_CONTAINS(LOWER(campaign_info),"flight|seller|courier|продав|курьер|бизнес")=false
GROUP BY 1

UNION ALL

SELECT -- smm_instagram_global
  date,
  CONCAT('smm') AS channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,
  ARRAY_TO_STRING([ad_account_name,campaign_name, adset_name,ad_name],"") as  campaign_info,  
  1.2*cost AS cost_usd, -- расход в долларах с НДС
  IF(usd_rub IS NULL,70,usd_rub)*IF(account_id=509197767245431,1.2,1)*cost as cost_rub, -- расход в рублях с НДС
  clicks, 
  impressions
FROM `funnel-flowwow.FACEBOOK_ADS_RAW.facebook_cost_data` 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)
WHERE REGEXP_CONTAINS(LOWER(campaign_name),"flight|seller|courier|продав|курьер|бизнес")=false -- большое расхождение из-за этого фильтра получается
)
WHERE REGEXP_CONTAINS(LOWER(campaign_info),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign_info),"wrld|world")
AND REGEXP_CONTAINS(LOWER(campaign_info),"flight|seller|courier|продав|курьер|бизнес")=false
GROUP BY 1

UNION ALL

SELECT -- smm_instagram
  date,
  CONCAT('smm') AS channel,
  SUM(cost_rub) AS cost_rub,
  SUM(cost_usd) AS cost_usd,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
FROM (
SELECT
  date,
  ARRAY_TO_STRING([ad_account_name,campaign_name, adset_name,ad_name],"") as  campaign_info,  
  1.2*cost AS cost_usd, -- расход в долларах с НДС
  IF(usd_rub IS NULL,70,usd_rub)*IF(account_id=509197767245431,1.2,1)*cost as cost_rub, -- расход в рублях с НДС
  clicks, 
  impressions
FROM `funnel-flowwow.FACEBOOK_ADS_RAW.facebook_cost_data` 
LEFT JOIN `funnel-flowwow.BUSINESS_DM.ii_exchange_rates` USING(date)
WHERE REGEXP_CONTAINS(LOWER(campaign_name),"flight|seller|courier|продав|курьер|бизнес")=false -- большое расхождение из-за этого фильтра получается
)
WHERE REGEXP_CONTAINS(LOWER(campaign_info),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign_info),"wrld|world")=false
AND REGEXP_CONTAINS(LOWER(campaign_info),"flight|seller|courier|продав|курьер|бизнес")=false
GROUP BY 1

UNION ALL

SELECT -- фиксированные ads_cost
  date,
  CASE 
    WHEN REGEXP_CONTAINS(LOWER(segment),"unity") THEN "unity"
    ELSE LOWER(channel) END AS channel,
  SUM(IF(channel NOT IN ("analytics","retention") AND segment NOT IN ("creatives","offline_merch","serm","all_cashback","all_promocode_integration","pr_rus","pr_international","donate","incentive_traffic"),cost,0)) AS cost_rub,
  0 AS cost_usd,
  0 AS clicks,
  0 AS impressions
FROM  `funnel-flowwow.BUSINESS_DM.cm_offline_costs_by_date`
GROUP BY 1,2

UNION ALL 

SELECT -- фиксированные service_cost 
  date,
  CASE 
    WHEN REGEXP_CONTAINS(LOWER(segment),"unity") THEN "unity"
    ELSE LOWER(channel) END AS channel,
  SUM(IF(channel IN ("analytics","retention") OR segment="incentive_traffic",cost,0)) AS cost_rub,
  0 AS cost_usd,
  0 AS clicks,
  0 AS impressions
  FROM  `funnel-flowwow.BUSINESS_DM.cm_offline_costs_by_date`
GROUP BY 1,2

UNION ALL 

SELECT --фиксированные not_ads_cost 
  date,
  CASE 
    WHEN REGEXP_CONTAINS(LOWER(segment),"unity") THEN "unity"
    ELSE LOWER(channel) END AS channel,
  SUM(IF(segment IN ("creatives","offline_merch","serm","all_cashback","all_promocode_integration","pr_rus","pr_international","donate"),cost,0)) AS cost_rub,
  0 AS cost_usd,
  0 AS clicks,
  0 AS impressions
FROM  `funnel-flowwow.BUSINESS_DM.cm_offline_costs_by_date`
GROUP BY 1,2
)
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
GROUP BY date, channel
ORDER BY channel,date DESC
)
, b AS ( --расходы,клики,показы из витрины c переатрибуцией
  SELECT
  date,
  CASE 
    WHEN REGEXP_CONTAINS(LOWER(segment),"vk_ads") THEN "vk_ads"
    WHEN REGEXP_CONTAINS(LOWER(segment),"smm_vk") THEN "smm_vk"
    WHEN REGEXP_CONTAINS(LOWER(segment),"facebook_ads") THEN "facebook_ads"
    WHEN REGEXP_CONTAINS(LOWER(segment),"other|nativity") THEN "other"
    WHEN REGEXP_CONTAINS(LOWER(segment),"google_ads") THEN "google_ads"
    WHEN REGEXP_CONTAINS(LOWER(segment),"regular_online") THEN "regular_online"
    WHEN REGEXP_CONTAINS(LOWER(segment),"yandex_direct") THEN "yandex_direct"
    ELSE LOWER(channel) END AS channel,
    ads_cost AS cost, --рекламные расходы
  clicks,
  impressions
  FROM `funnel-flowwow.PRODUCTION_DM.cm_data_reattribution`
   WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))

  UNION ALL

  SELECT
    date,
    channel, 
    not_ads_cost AS cost, -- нерекламные расходы
    0 AS clicks,
    0 AS impressions
  FROM `funnel-flowwow.PRODUCTION_DM.cm_data_reattribution`
  WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
  AND  REGEXP_CONTAINS(LOWER(segment),"vk_ads|smm_vk|facebook_ads|other|google_ads|regular_online|yandex_direct")=false 

  UNION ALL

  SELECT
    date,
    channel, 
    service_cost AS cost,--сервисные расходы
    0 AS clicks,
    0 AS impressions
  FROM `funnel-flowwow.PRODUCTION_DM.cm_data_reattribution`
   WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
  AND REGEXP_CONTAINS(LOWER(segment),"vk_ads|smm_vk|facebook_ads|other|google_ads|regular_online|yandex_direct")=false 
  )
, c AS (  
SELECT
  date,
  channel AS reattribution_channel,
  SUM(cost) AS reattribution_cost,
  SUM(clicks) AS reattribution_clicks,
  SUM(impressions) AS reattribution_impressions,
FROM b
WHERE date BETWEEN (DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)) AND (DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
GROUP BY date, channel
ORDER BY channel
)
SELECT --объединить сырые данные и из cm_reattribution
  a.channel,
  SUM(a.cost_usd) AS cost_usd,
  SUM(a.cost_rub) AS cost_rub,
  SUM(c.reattribution_cost) AS reattribution_cost,
  SAFE_DIVIDE((SUM(a.cost_rub) - SUM(c.reattribution_cost)),SUM(a.cost_rub)) AS cost_diff,
  SUM(a.clicks) AS clicks,
  SUM(c.reattribution_clicks) AS reattribution_clicks,
  SAFE_DIVIDE((SUM(a.clicks)-SUM(c.reattribution_clicks)),SUM(a.clicks)) AS clicks_diff,
  SUM(a.impressions) AS impressions,
  SUM(c.reattribution_impressions) AS reattribution_impressions,
  SAFE_DIVIDE((SUM(a.impressions)-SUM(c.reattribution_impressions)),SUM(a.impressions)) AS impressions_diff
FROM a FULL JOIN c ON a.date = c.date AND a.channel = c.reattribution_channel
WHERE reattribution_cost !=0 -- убрать каналы с расходом 0
AND a.date BETWEEN '2024-01-26' AND '2024-02-04' 
AND reattribution_channel NOT IN ("other","regular_online") -- не беру, т.к не удобно в сырых данных их исключить, сюда падает flight,seller
GROUP BY 1
ORDER BY channel DESC