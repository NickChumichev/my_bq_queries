
create or replace  table
funnel-flowwow.Analyt_KosarevS.A6_ads_cost_by_traffic
partition by date 
as 

(
WITH
date_v_1 AS
  (
  SELECT
  date, subdivision, channel, segment, campaign, is_retargeting_campaign, is_world_campaign,
  category, subcategory, city_to, country_to, region, platform, ads_cost, not_ads_cost, service_cost,traffic
  FROM `funnel-flowwow.Analyt_KosarevS.A4_cm_data_by_campaigns`
  WHERE date>="2022-01-01"
  )
  
,dates AS
  (
SELECT
    date, subdivision, channel, segment, campaign, is_retargeting_campaign, is_world_campaign,
    IF(category IS NULL,"Цветы и подарки",category) as category, IF(subcategory IS NULL,"Монобукеты", subcategory) as subcategory, IF(city_to IS NULL, "Москва", city_to) as city_to,
  IF(country_to IS NULL, "Россия", country_to) as country_to, IF(region IS NULL, "russia",region) as region,
  CASE
    WHEN platform IS NOT NULL THEN platform -- исключение platform, где NULL
    WHEN platform IS NULL AND subdivision="brand" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_web" THEN "desktop"
    WHEN platform IS NULL AND subdivision="paid_apps" AND segment="appnext" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="apple_search_ads" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="facebook_ads" THEN "desktop"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="unity" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="cpa" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="google_ads_uac" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="mytarget" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="yandex_direct_app" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="vk_ads" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel NOT IN ("appnext","apple_search_ads","facebook_ads","unity","cpa","google_ads_uac","mytarget","yandex_direct_app","vk_ads") THEN "ios app"
    WHEN platform IS NULL AND subdivision="retention" THEN "android app"
    WHEN platform IS NULL AND subdivision="organic_web" THEN "desktop"
    END as platform,
  CASE
    WHEN SUM(traffic)=0 THEN 0.001
    ELSE SUM(traffic) END as traffic,
  CASE 
    WHEN SUM(ads_cost)>0 THEN 0
  ELSE
    SAFE_DIVIDE(SUM(SUM(ads_cost)) OVER(PARTITION BY IF(channel NOT IN ("retargeting") AND segment NOT IN ("facebook_ads"),channel,NULL), segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)), 
    COUNT(*) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date))-
    COUNTIF(SUM(ads_cost)>0) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)))
  END AS ads_cost, -- разбивка расхода по платформам
  CASE 
    WHEN SUM(not_ads_cost)>0 THEN 0
  ELSE
    SAFE_DIVIDE(SUM(SUM(not_ads_cost)) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)), 
    COUNT(*) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date))-
    COUNTIF(SUM(not_ads_cost)>0) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)))
  END AS not_ads_cost,
    CASE 
    WHEN SUM(service_cost)>0 THEN 0
  ELSE
    SAFE_DIVIDE(SUM(SUM(service_cost)) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)), 
    COUNT(*) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date))-
    COUNTIF(SUM(service_cost)>0) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)))
  END AS service_cost,
  FROM date_v_1
  WHERE LOWER(segment) NOT IN ("special_offline","ya_promo","smm", "incentive_traffic", "dzen_client", "twitter_global_smm", "snapchat_global_smm")
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
  UNION ALL
SELECT
    date, subdivision, channel, segment, campaign, is_retargeting_campaign, is_world_campaign,
    IF(category IS NULL,"Цветы и подарки",category) as category, IF(subcategory IS NULL,"Монобукеты", subcategory) as subcategory, IF(city_to IS NULL, "Москва", city_to) as city_to,
    IF(country_to IS NULL, "Россия", country_to) as country_to, IF(region IS NULL, "russia",region) as region,
  CASE
    WHEN platform IS NOT NULL THEN platform -- исключение platform, где NULL
    WHEN platform IS NULL AND subdivision="brand" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_web" THEN "desktop"
    WHEN platform IS NULL AND subdivision="paid_apps" AND segment="appnext" THEN "android app"
    WHEN platform IS NULL AND subdivision="organic_apps" AND segment="incentive_traffic" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="apple_search_ads" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="facebook_ads" THEN "desktop"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="unity" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="cpa" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="google_ads_uac" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="mytarget" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="yandex_direct_app" THEN "android app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel="vk_ads" THEN "ios app"
    WHEN platform IS NULL AND subdivision="paid_apps" AND channel NOT IN ("appnext","apple_search_ads","facebook_ads","unity","cpa","google_ads_uac","mytarget","yandex_direct_app","vk_ads") THEN "ios app"
    WHEN platform IS NULL AND subdivision="partners" THEN "web ios"
    WHEN platform IS NULL AND subdivision="retention" THEN "android app"
    WHEN platform IS NULL AND subdivision="organic_web" THEN "desktop"
  END as platform,
  CASE
    WHEN SUM(traffic)=0 THEN 0.001
    ELSE SUM(traffic) END as traffic,
    SUM(ads_cost) AS ads_cost,
    SUM(not_ads_cost) AS not_ads_cost,
    SUM(service_cost) AS service_cost,

  FROM date_v_1
  WHERE LOWER(segment) IN ("special_offline","ya_promo","smm", "incentive_traffic", "dzen_client", "twitter_global_smm", "snapchat_global_smm")
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
  )
 
 
SELECT
date, subdivision, channel, segment, campaign, is_retargeting_campaign, is_world_campaign, category, subcategory, city_to, country_to, region, platform,
SAFE_DIVIDE(traffic,SUM(traffic) OVER(a))*(SUM(ads_cost) OVER(a)) as ads_cost_by_traffic,
SAFE_DIVIDE(traffic,SUM(traffic) OVER(a))*(SUM(not_ads_cost) OVER(a)) as not_ads_cost_by_traffic,
SAFE_DIVIDE(traffic,SUM(traffic) OVER(a))*(SUM(service_cost) OVER(a)) as service_cost_by_traffic
FROM dates
WINDOW a AS (PARTITION BY date, subdivision, channel, segment, campaign, is_retargeting_campaign, is_world_campaign)

);