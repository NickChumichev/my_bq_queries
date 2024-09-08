WITH date_v_1 AS
  (
  SELECT
  date, subdivision, channel, segment, campaign, is_retargeting_campaign, is_world_campaign, category, subcategory, city_to, country_to, region, platform, ads_cost, not_ads_cost, service_cost,traffic
  FROM `funnel-flowwow.PRODUCTION_DM.cm_data_by_campaigns`
  )
,dates AS
  (
SELECT
    date, subdivision, channel, segment, campaign, is_retargeting_campaign, is_world_campaign
    , IF(category IS NULL,"Цветы и подарки",category) as category, IF(subcategory IS NULL,"Монобукеты", subcategory) AS subcategory, IF(city_to IS NULL, "Москва", city_to) AS city_to,
  IF(country_to IS NULL, "Россия", country_to) AS country_to, IF(region IS NULL, "russia",region) AS region,
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
    END AS platform,
  CASE
    WHEN SUM(traffic)=0 THEN 0.001
    ELSE SUM(traffic) END AS traffic,
  CASE 
    WHEN SUM(ads_cost)>0 THEN 0
  ELSE
    SAFE_DIVIDE(SUM(SUM(ads_cost)) OVER(PARTITION BY IF(channel NOT IN ("retargeting") AND segment NOT IN ("facebook_ads"),channel,NULL), segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)), 
    COUNT(date) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date))-
    COUNTIF(SUM(ads_cost)>0) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)))
  END AS ads_cost, -- разбивка расхода по платформам
  CASE 
    WHEN SUM(not_ads_cost)>0 THEN 0
  ELSE
    SAFE_DIVIDE(SUM(SUM(not_ads_cost)) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)), 
    COUNT(date) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date))-
    COUNTIF(SUM(not_ads_cost)>0) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)))
  END AS not_ads_cost,
    CASE 
    WHEN SUM(service_cost)>0 THEN 0
  ELSE
    SAFE_DIVIDE(SUM(SUM(service_cost)) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)), 
    COUNT(date) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date))-
    COUNTIF(SUM(service_cost)>0) OVER(PARTITION BY channel, segment, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)))
  END AS service_cost,
  FROM date_v_1
  WHERE LOWER(segment) NOT IN ("special_offline","ya_promo","smm", "incentive_traffic", "dzen_client", "smm_global_twitter", "smm_global_snapchat","two_gis", "pikabu","tg_bot","two_gis","nativity","aura","smm_youtube", "dzen", "instagram_smm_global","special_omnichannel","snapchat_global_smm","special_online","twitter_global_smm","twitter","tg_ads","yandex_maps","partners_cpa")
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
  UNION ALL
SELECT
    date, subdivision, channel, segment, IF(campaign IS NULL, 'no_campaign', 'no_campaign') AS campaign, is_retargeting_campaign, is_world_campaign
    , IF(category IS NULL,"Цветы и подарки",category) AS category, IF(subcategory IS NULL,"Монобукеты", subcategory) AS subcategory, IF(city_to IS NULL, "Москва", city_to) AS city_to,
    IF(country_to IS NULL, "Россия", country_to) AS country_to, IF(region IS NULL, "russia",region) AS region,
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
  END AS platform,
  CASE
    WHEN SUM(traffic)=0 THEN 0.001
    ELSE SUM(traffic) END AS traffic,
    SUM(ads_cost) AS ads_cost,
    SUM(not_ads_cost) AS not_ads_cost,
    SUM(service_cost) AS service_cost,

  FROM date_v_1
  WHERE LOWER(segment) IN ("special_offline","ya_promo","smm", "incentive_traffic", "dzen_client", "smm_global_twitter", "smm_global_snapchat","two_gis", "pikabu","tg_bot","two_gis","nativity","aura","smm_youtube", "dzen", "instagram_smm_global","special_omnichannel","snapchat_global_smm","special_online","twitter_global_smm","twitter","tg_ads","yandex_maps","partners_cpa")
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
  )
  , b AS ( 
SELECT
  date, 
  subdivision, 
  channel, 
  segment, 
  campaign, 
  is_retargeting_campaign, 
  is_world_campaign, 
  category, 
  subcategory, 
  city_to, 
  country_to, 
  region, 
  platform,
  SUM(traffic) AS traffic,
  SUM(ads_cost) AS ads_cost,
  SUM(not_ads_cost) AS not_ads_cost,
  SUM(service_cost) AS service_cost,
FROM dates
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
WINDOW a AS (PARTITION BY date, subdivision, channel, segment, campaign)
)
, a AS (
SELECT DISTINCT
  date, 
  subdivision, 
  channel, 
  segment, 
  campaign, 
  is_retargeting_campaign, 
  is_world_campaign, 
  category, 
  subcategory, 
  city_to, 
  country_to, 
  region, 
  platform,
  -- SUM(traffic) AS traffic,
  -- SUM(ads_cost) AS ads_cost,
  SUM(traffic) OVER(PARTITION BY date, subdivision, channel, segment, campaign) AS sum_traffic,
  SUM(ads_cost) OVER(PARTITION BY date, subdivision, channel, segment, campaign) AS sum_ads_cost,
  SAFE_DIVIDE(traffic,SUM(traffic) OVER(PARTITION BY date, subdivision, channel, segment, campaign))*(SUM(ads_cost) OVER(PARTITION BY date, subdivision, channel, segment, campaign)) AS ads_cost_by_traffic,
  SAFE_DIVIDE(traffic,SUM(traffic) OVER(PARTITION BY date, subdivision, channel, segment, campaign))*(SUM(not_ads_cost) OVER(PARTITION BY date, subdivision, channel, segment, campaign)) AS not_ads_cost_by_traffic,
  SAFE_DIVIDE(traffic,SUM(traffic) OVER(PARTITION BY date, subdivision, channel, segment, campaign))*(SUM(service_cost) OVER(PARTITION BY date, subdivision, channel, segment, campaign)) AS service_cost_by_traffic,
FROM b
-- WHERE segment = 'two_gis'
)
SELECT
  segment,
  -- SUM(ads_cost) AS ads_cost,
  SUM(ads_cost_by_traffic) AS ads_cost_by_traffic,
  SUM(not_ads_cost_by_traffic) AS not_ads_cost_by_traffic,
  SUM(service_cost_by_traffic) AS service_cost_by_traffic
FROM a
WHERE date BETWEEN '2024-01-01' AND '2024-05-29'
GROUP BY 1

