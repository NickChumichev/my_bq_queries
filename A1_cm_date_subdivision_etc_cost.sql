create or replace  table
funnel-flowwow.Analyt_KosarevS.A1_cm_date_subdivision_etc_cost
as 

(
WITH
 segmentation_basic AS
   (
   SELECT 
   *,
   CASE
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),".*flight.*")=false AND REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|flight|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм|.*cn-.*|.*cn_.*|yapromo") = false AND REGEXP_CONTAINS(LOWER(campaign_info),"smart|banner|display|flight|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм")=false AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false AND REGEXP_CONTAINS(LOWER(campaign),r".*\|.*|.*\\.*") AND REGEXP_CONTAINS(LOWER(campaign),"video")=false THEN "yandex_direct_web_non_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),".*flight.*")=false AND REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") AND REGEXP_CONTAINS(LOWER(campaign),r".*\|.*|.*\\.*") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|rsy|yapromo")=false THEN "yandex_direct_web_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),".*flight.*")=false AND REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"рся|rsy|network|retarget|smart") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|brand|бренд|.*cn-.*|.*cn_.*|yapromo") = false AND REGEXP_CONTAINS(LOWER(campaign),r".*\|.*|.*\\.*") THEN "yandex_direct_web_rsy"


   WHEN REGEXP_CONTAINS(LOWER(campaign_info),".*flight.*")=false AND REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм")=false AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|uac|kms|app|.*[CONV].*")=false  THEN "google_ads_web_non_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),".*flight.*")=false AND REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|kms|.*[CONV].*")=false THEN "google_ads_web_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),".*flight.*")=false AND REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"кмс|kms|network|retarget") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|src|.*[CONV].*")=false THEN "google_ads_web_kms"


    WHEN REGEXP_CONTAINS(LOWER(campaign),"ios|android|app|uac|.*cn-.*|.*cn_.*") AND REGEXP_CONTAINS(LOWER(campaign),"flight")=false AND source IN ("yandex", "yabs.yandex.ru") THEN "yandex_direct_app"


   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight")=false AND source="google" AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac") THEN "google_ads_uac"


   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight")=false AND REGEXP_CONTAINS(LOWER(source),"target.my.com|mycom|mytarget") AND REGEXP_CONTAINS(LOWER(campaign),"seller")=false AND REGEXP_CONTAINS(LOWER(campaign_info),"flight")=false AND date <= "2023-02-28" THEN "mytarget"

WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight")=false AND source IN("vk_ads","vk apps") AND REGEXP_CONTAINS(LOWER(campaign_info),r"smm|алиса|\/ влад|широк|пост[\d][\D]")=false AND  REGEXP_CONTAINS(LOWER(campaign),r"smm|алиса|\/ влад|широк|пост[\d][\D]")=false AND REGEXP_CONTAINS(LOWER(campaign),"dzen")=false THEN "vk_ads"

 WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight")=false AND (REGEXP_CONTAINS(LOWER(campaign_info),r"smm_vk|алиса|\/ влад|широк") OR REGEXP_CONTAINS(LOWER(campaign),r"smm_vk|алиса|\/ влад|широк|пост[\d][\D]")) THEN "vk_smm"

   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight")=false AND source="apple_search_ads" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")  THEN "asa_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight")=false AND source="apple_search_ads" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")=false THEN "asa_non_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight")=false AND source="tiktok" THEN "tiktok_app"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight|post-boosting")=false AND REGEXP_CONTAINS(LOWER(source),"facebook|23847044487210398")  THEN "facebook_ads"


   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"flight") OR REGEXP_CONTAINS(LOWER(campaign),"flight") THEN "regular_online"

   WHEN source IN ("google","yandex") AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"yapromo") THEN "ya_promo"

   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign_info),"wrld|world") THEN "instagram_global_smm"

   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign_info),"wrld|world")=false THEN "instagram_smm"

   WHEN source IN("vk_ads","vk apps") AND REGEXP_CONTAINS(LOWER(campaign),"dzen")  THEN "dzen_client"

   ELSE "other"


   END AS segment,
   CAST(IF((REGEXP_CONTAINS(LOWER(campaign),"ретарг|ремарк|retarg|remark|exist|rem") AND REGEXP_CONTAINS(LOWER(campaign),"premium")=false) OR (REGEXP_CONTAINS(LOWER(campaign_info),"ретарг|ремарк|retarg|remark|exist|rem") AND REGEXP_CONTAINS(LOWER(campaign_info),"premium")=false) OR (source IN ("yandex","google") AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|retargering|ретаргетинг|rlsa|рлса|rmk")),1,0) AS INT64) as is_retargeting_campaign,


   FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cost`


   WHERE REGEXP_CONTAINS(LOWER(campaign),"seller|courier|продав|курьер|бизнес")=false AND REGEXP_CONTAINS(LOWER(campaign_info),"seller|courier|продав|курьер|бизнес")=false
   ),
 channels AS
   (
   SELECT date, source, medium, campaign, cost, clicks, impressions, segment, campaign_info, is_retargeting_campaign,
   CASE
   WHEN segment IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy") AND is_retargeting_campaign!=1 THEN "yandex_direct_web"
   WHEN segment IN ("google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") AND is_retargeting_campaign!=1 THEN "google_ads_web"
   WHEN segment IN ("asa_brand", "asa_non_brand") AND is_retargeting_campaign!=1 THEN "apple_search_ads"
   WHEN segment IN ("special_offline","regular_online", "special_online") AND is_retargeting_campaign!=1 THEN "mediaflights"
   WHEN segment IN ("vk_smm","instagram_global_smm","instagram_smm") AND is_retargeting_campaign!=1 THEN "smm"
   WHEN segment IN ("ya_promo") THEN "content_fw_client"
   WHEN segment IN ("dzen_client") THEN "content_fw_client"
   WHEN segment NOT IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy","google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms","asa_non_brand", "asa_brand","special_offline","regular_online", "special_online","vk_smm") AND is_retargeting_campaign!=1 THEN segment
   WHEN is_retargeting_campaign=1 THEN "retargeting"
   END AS channel
   FROM segmentation_basic
   ),
 subdivisions AS  
   (
   SELECT date, source, medium, campaign, cost, clicks, impressions, segment, channel, campaign_info, is_retargeting_campaign,
   CASE
   WHEN segment IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy") OR segment IN ("google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") THEN "paid_web"
   WHEN segment IN ("vk_smm","telegram_smm","orm","offline_merch","pr","pr_telegram","influencers","instagram_smm","creatives","regular_online","special_offline","special_online","dzen_client","ya_promo","pikabu_smm","instagram_global_smm") THEN "brand"
   WHEN channel ="other" THEN "other"
   ELSE "paid_apps"
   END AS subdivision
   FROM channels
   ),
 cities_categories_subcategories_id AS
   (
   SELECT date, source, medium, campaign, campaign_info, cost, clicks, impressions, segment, channel, subdivision, is_retargeting_campaign,


   CASE
   --WHEN source="google" AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac") AND REGEXP_CONTAINS(LOWER(campaign_info), ".*nocity.*") THEN  REGEXP_EXTRACT(campaign_info, "1")
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"cityid") THEN REGEXP_EXTRACT(campaign_info, r"cityid-([0-9]+)")
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"city") THEN REGEXP_EXTRACT(campaign_info, r"city-([0-9]+)")
   ELSE "without_city" 
   END AS city_id,


   CASE
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"catid-") THEN REGEXP_EXTRACT(campaign_info,r"catid-([0-9]+)")
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"cat-")  THEN REGEXP_EXTRACT(campaign_info,r"cat-([0-9]+)")
   ELSE "nocategory"
   END AS category_id,


   CASE
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"subcatid-") THEN REGEXP_EXTRACT(campaign_info,r"subcatid-([0-9]+)")
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"subcat-")  THEN REGEXP_EXTRACT(campaign_info,r"subcat-([0-9]+)")
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"subc-")  THEN REGEXP_EXTRACT(campaign_info,r"subc-([0-9]+)")
   ELSE "nosubcategory"
   END AS subcategory_id,

   CASE 
   WHEN REGEXP_CONTAINS(campaign_info,"regionid-") THEN REGEXP_EXTRACT(campaign_info,r"regionid-([0-9]+)")
   WHEN REGEXP_CONTAINS(campaign_info,r"regionid_") THEN REGEXP_EXTRACT(campaign_info,r"regionid_([0-9]+)")
   WHEN REGEXP_CONTAINS(campaign_info,"region-") THEN REGEXP_EXTRACT(campaign_info,r"region-([0-9]+)")
   WHEN REGEXP_CONTAINS(campaign_info,r"region_") THEN REGEXP_EXTRACT(campaign_info,r"region_([0-9]+)")

   END AS region_id

   FROM subdivisions
   ),
 cities_categories_subcategories AS
   (
   SELECT date, source, medium, campaign, campaign_info, cost, clicks, impressions, segment, channel, subdivision, city, countries.country, category_name as category, a.region_id,
   sub_category_name as subcategory, 
   CASE
   WHEN regionid IS NOT NULL THEN regionid 
   WHEN c_r_id.region IS NOT NULL THEN c_r_id.region
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"cis") AND REGEXP_CONTAINS(LOWER(campaign_info),"non_cis")=false THEN "cis"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"uk|londo") THEN "uk"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"spain|barcelon|madri|barselon") THEN "spain"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"mena|uae|emirates|arab|duba") THEN "mena"
   WHEN REGEXP_CONTAINS(LOWER(campaign_info),"russia") THEN "russia"
   END as region, 
   is_retargeting_campaign
   FROM cities_categories_subcategories_id as a
   LEFT JOIN
   (SELECT city_id, city
   FROM `funnel-flowwow.MYSQL_EXPORT.f_city`
  GROUP BY city_id, city ) as b
  ON a.city_id=CAST(b.city_id AS STRING)
  LEFT JOIN
  (SELECT category_id, category_name
  FROM `funnel-flowwow.ANALYTICS_DM.category_subcategory`
  GROUP BY category_id, category_name) as c
  ON a.category_id=CAST(c.category_id AS STRING)
  LEFT JOIN
  (SELECT sub_category_id, sub_category_name
  FROM `funnel-flowwow.ANALYTICS_DM.category_subcategory`
  GROUP BY sub_category_id, sub_category_name) as d
  ON a.subcategory_id=CAST(d.sub_category_id AS STRING)
  LEFT JOIN
  (SELECT city_id, country_id
  FROM `funnel-flowwow.MYSQL_EXPORT.f_city_all`
  GROUP BY 1,2) as cities
  ON b.city_id=cities.city_id
  LEFT JOIN
  (SELECT country_id, country
  FROM `funnel-flowwow.MYSQL_EXPORT.f_country`
  GROUP BY 1,2) as countries
  ON cities.country_id=countries.country_id
  LEFT JOIN
  (SELECT CAST(id AS STRING) as id, regionid
  FROM `funnel-flowwow.BUSINESS_DM.regions_gs_view`) as r_ids
  ON r_ids.id=CAST(a.region_id AS STRING)
  LEFT JOIN
  `funnel-flowwow.BUSINESS_DM.m_countries_regions` as c_r_id 
  ON c_r_id.country_id=countries.country_id
  )


 SELECT date, subdivision, channel, segment, source, medium, campaign,
 is_retargeting_campaign,
 CAST(IF(REGEXP_CONTAINS(LOWER(campaign_info),"wrld|world"),1,0) AS INT64) as is_world_campaign,
 CAST(null AS STRING) as attributed_by,  CAST(null AS STRING) as city_from, city as city_to,
 CAST(null AS STRING) as country_from, IF(country IS NULL,"no_country", country) as country_to, CASE 
 WHEN (country IS NULL AND region_id NOT BETWEEN "1" AND "99") OR region IS NULL THEN "no_region"
 ELSE region END as region,
 category, subcategory, CAST(null AS STRING) as platform, SUM(cost) as ads_cost, 0 as service_cost, 0 as not_ads_cost,
 SUM(clicks) as clicks, SUM(impressions) as impressions
 FROM cities_categories_subcategories
 GROUP BY date, subdivision, channel, segment, source, medium, campaign,
 is_retargeting_campaign, is_world_campaign,
 attributed_by, city_from, city_to, country_from, country_to, region, category, subcategory, platform

 /*
 -----------временные ручные косты по гуглу
UNION ALL
SELECT CAST(date AS DATE) as date, subdivision, channel, segment, "google" as source, "cpc" as medium, null as campaign,
null as is_retargeting_campaign, null as is_world_campaign, 
CAST(null AS STRING) as attributed_by,  CAST(null AS STRING) as city_from, CAST(null AS STRING) as city_to,
CAST(null AS STRING) as country_from, CAST(null AS STRING) as country_to,
CAST(null AS STRING) as category, CAST(null AS STRING) as subcategory, CAST(null AS STRING) as platform, CAST(ads_cost AS NUMERIC) as ads_cost, 0 as service_cost, 0 as not_ads_cost
FROM `funnel-flowwow.BUSINESS_DM.google_ads_cost_gs_view` 
WHERE ads_cost IS NOT NULL
---------------------
*/
UNION ALL
(SELECT date,
CASE
WHEN channel="seo" THEN "organic_web"
WHEN channel IN ("influencers","pr","orm","offline_merch","content_fw_client","creatives", "smm","instagram",
"mediaflights") THEN "brand"
WHEN channel IN ("cashback","geo_services","promocode_integration","donate", "partners_cpa") THEN "partners"
WHEN channel IN ("analytics") THEN "services"
WHEN channel IN ("appnext", "cpa","unity","vk_ads","unity_ios","unity_android","google_ads_uac","predownloads", "xapads", "apps") THEN "paid_apps"
WHEN channel IN ("email_push") THEN "retention"
WHEN channel="aso_organic" THEN "organic_apps"
END AS subdivision,
CASE
WHEN REGEXP_CONTAINS(LOWER(segment),"unity") THEN "unity"
ELSE
LOWER(channel) END AS channel,
CASE
WHEN REGEXP_CONTAINS(LOWER(segment),"unity") THEN "unity"
WHEN channel="influencers" THEN "influencers"
WHEN channel="instagram" THEN "smm"
 ELSE LOWER(segment)
 END as segment,


 CAST(null AS STRING) as source,
 CAST(null AS STRING) as medium,
 CAST(null AS STRING) as campaign,
 CAST(0 AS INT64) as is_retargeting_campaign,
 CAST(0 AS INT64) as is_world_campaign,
 CAST(null AS STRING) as attributed_by,




 CAST(null AS STRING) as city_from,
city_to,
CAST(null AS STRING) as country_from,
IF(country_to IS NULL,"no_country", country_to) as country_to,
CASE 
WHEN country_to IS NULL AND region IS NULL AND regionid IS NULL THEN "no_region"
WHEN regionid IS NOT NULL THEN regionid
WHEN region IS NOT NULL THEN region
END as region,
CAST(null AS STRING) as category, CAST(null AS STRING) as subcategory, CAST(null AS STRING) as platform,
SUM(IF(channel NOT IN ("analytics","retention") AND segment NOT IN ("creatives","offline_merch","orm","all_cashback","all_promocode_integration","pr_rus","pr_international","donate","incentive_traffic","google_seo","yandex_seo","instagram_global_smm","smm_tiktok"),cost,0)) as ads_cost,
SUM(IF(channel IN ("analytics","retention") OR segment="incentive_traffic",cost,0)) as service_cost,
SUM(IF(segment IN ("creatives","offline_merch","orm","all_cashback","all_promocode_integration","pr_rus","pr_international","donate","google_seo","yandex_seo","instagram_global_smm","smm_tiktok"),cost,0)) as not_ads_cost,
0 as clicks,
 0 as impressions
 FROM  `funnel-flowwow.BUSINESS_DM.cm_offline_costs_by_date`
 LEFT JOIN
 (SELECT CAST(id AS STRING) as id, regionid
 FROM `funnel-flowwow.BUSINESS_DM.regions_gs_view`) as r_ids_2
 ON region_id=r_ids_2.id
LEFT JOIN `funnel-flowwow.BUSINESS_DM.m_countries_regions` 
 ON country_to=country

 WHERE cost IS NOT NULL AND segment NOT IN ("Criteo","vk_ads","google_ads_uac")
 GROUP BY date, subdivision, channel, segment, source, medium, campaign, is_retargeting_campaign, is_world_campaign, attributed_by, city_from, city_to, country_from, country_to, region, category, subcategory, platform)

);