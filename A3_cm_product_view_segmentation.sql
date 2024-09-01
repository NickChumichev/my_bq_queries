
create or replace  table
funnel-flowwow.Analyt_KosarevS.A3_cm_product_view_segmentation
 as
 
(
WITH
views AS
  (
   SELECT * EXCEPT(source, medium, campaign), LOWER(source) as source, LOWER(medium) as medium, LOWER(campaign) as campaign
   FROM `funnel-flowwow.BUSINESS_DM.m_product_views`
   ),
 segments AS
   (
   SELECT
   *,
   CASE
   WHEN REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|flight|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм|.*cn-.*|.*cn_.*|yapromo") = false AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false AND REGEXP_CONTAINS(LOWER(campaign),r".*\|.*|.*\\.*") AND REGEXP_CONTAINS(LOWER(campaign),"video")=false THEN "yandex_direct_web_non_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") AND REGEXP_CONTAINS(LOWER(campaign),r".*\|.*|.*\\.*") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|rsy|yapromo")=false THEN "yandex_direct_web_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"рся|rsy|network|retarget|smart") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|brand|бренд|.*cn-.*|.*cn_.*|yapromo") = false AND REGEXP_CONTAINS(LOWER(campaign),r".*\|.*|.*\\.*") THEN "yandex_direct_web_rsy"

   WHEN REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм")=false AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|uac|kms|app|.*[CONV].*")=false  THEN "google_ads_web_non_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|kms|.*[CONV].*")=false THEN "google_ads_web_brand"
   WHEN REGEXP_CONTAINS(LOWER(campaign),".*flight.*")=false 
   AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"кмс|kms|network|retarget") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|src|.*[CONV].*")=false THEN "google_ads_web_kms"



   WHEN REGEXP_CONTAINS(LOWER(campaign),"ios|android|app|uac") AND REGEXP_CONTAINS(LOWER(campaign),"flight|yapromo")=false AND REGEXP_CONTAINS(LOWER(campaign), 'rem') = false AND source IN ("yandex direct") THEN "yandex_direct_app"

   WHEN REGEXP_CONTAINS(LOWER(campaign),"ios|android|app|uac") AND REGEXP_CONTAINS(LOWER(campaign),"flight|yapromo")=false AND REGEXP_CONTAINS(LOWER(campaign), 'rem') AND source IN ("yd_retargeting") THEN "yandex_direct_app"

   WHEN LOWER(source) IN ("google ads aci","google ads ace") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac") THEN "google_ads_uac"

   WHEN LOWER(source) IN("target.my.com","mycom","mytarget", "mt_ios_feed", "mt_feed", "mytarget_ios") AND date <= "2023-02-28" THEN "mytarget"
   WHEN LOWER(source) IN("vk apps", "vk_android", "vk_retargeting") AND REGEXP_CONTAINS(LOWER(campaign),"dzen")=false THEN "vk_ads"
   WHEN LOWER(source) IN("apple_search_ads","apple search ads") AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")  THEN "asa_brand"
   WHEN LOWER(source) IN("apple_search_ads","apple search ads") AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")=false THEN "asa_non_brand"
   WHEN REGEXP_CONTAINS(LOWER(source),"tiktok") THEN "tiktok_app"
   WHEN REGEXP_CONTAINS(LOWER(source),"unattributed") THEN "facebook_ads"
   WHEN REGEXP_CONTAINS(LOWER(source),"facebook") AND REGEXP_CONTAINS(LOWER(campaign),"post-boosting")=false THEN "facebook_ads"

   WHEN LOWER(source)="appnext_android" THEN "appnext"
   WHEN REGEXP_CONTAINS(LOWER(source),"unit") THEN "unity"

   WHEN REGEXP_CONTAINS(LOWER(source),"reliz|adchampagne|hybe") THEN "cpa"

   WHEN REGEXP_CONTAINS(LOWER(source),"2gis") THEN "two_gis"

   WHEN source="google" AND LOWER(medium)="organic" THEN "google_seo"
   WHEN (source="yandex" AND LOWER(medium)="organic") OR (source = "yandex.ru" AND medium = "referral") THEN "yandex_seo"
   WHEN source ="(direct)" THEN "direct"

   WHEN LOWER(source) IN ("organic", "imported devices", "untrusted devices", "google organic search") THEN "aso_organic"
   WHEN LOWER(source)="flowwow_com" THEN "flowwow_com"
   WHEN LOWER(source)="invite_friend" THEN "invite_friend"

   WHEN REGEXP_CONTAINS(LOWER(source),"email|push") THEN "email_push"
   WHEN REGEXP_CONTAINS(LOWER(source),"sms") THEN "sms"

   WHEN LOWER(source) IN ("instagram_influencers","youtube","podcast","twitter","telegram","tiktok","dzen","stream","cpa") THEN "influencers"



   WHEN LOWER(source) IN ("vkontakte_groups", "vkontakte_our_group","vk_bio") THEN "vk_smm"
   WHEN LOWER(source) IN ("instagram_bio", "instagram_stories") THEN "instagram_smm"
   WHEN REGEXP_CONTAINS(LOWER(campaign),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign),"wrld|world")=false THEN "instagram_smm"
   WHEN LOWER(source) IN ("instagram.global_bio","global_bio", "worldwide") THEN "instagram_global_smm"
   WHEN REGEXP_CONTAINS(LOWER(campaign),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign),"wrld|world") THEN "instagram_global_smm"



   WHEN REGEXP_CONTAINS(LOWER(campaign),"flight") THEN "regular_online"
   WHEN REGEXP_CONTAINS(LOWER(source),"partners_event") THEN "special_offline"
   WHEN REGEXP_CONTAINS(LOWER(source),"partners_media") THEN "special_online"

   WHEN REGEXP_CONTAINS(LOWER(source),"partners_staff|partners_loyalty") THEN "all_promocode_integration"
   WHEN LOWER(source) IN ("telegram_bio", "telegram_product") THEN "telegram_smm"
   WHEN REGEXP_CONTAINS(LOWER(source),"aura") THEN "aura"
   WHEN REGEXP_CONTAINS(LOWER(source),"ya_promo") OR REGEXP_CONTAINS(LOWER(campaign),"yapromo") THEN "ya_promo"
   WHEN source="partners_cashback" THEN "all_cashback"
   WHEN REGEXP_CONTAINS(LOWER(source),"dzen_clients") OR REGEXP_CONTAINS(LOWER(campaign),"dzen") THEN "dzen_client"
   WHEN REGEXP_CONTAINS(LOWER(source),"yandex_maps") THEN "yandex_maps"
   WHEN REGEXP_CONTAINS(LOWER(source),"pikabu") THEN "pikabu"
   WHEN REGEXP_CONTAINS(LOWER(source),"tgbot") THEN "tg_ads"


   ELSE "other"

   END AS segment,
   CAST(IF(REGEXP_CONTAINS(LOWER(campaign),"wrld|world"),1,0) AS INT64) as is_world_campaign,

   CASE
   WHEN REGEXP_CONTAINS(LOWER(campaign),"cityid") THEN REGEXP_EXTRACT(campaign, r"cityid-([0-9]+)")
   WHEN REGEXP_CONTAINS(LOWER(campaign),"city") THEN REGEXP_EXTRACT(campaign, r"city-([0-9]+)")
   ELSE "without_city" 
   END AS city_id,

   CASE 
   WHEN REGEXP_CONTAINS(campaign,"regionid") THEN REGEXP_EXTRACT(campaign,r"regionid-([0-9]+)")
   END AS region_id

   FROM views
   ),
 segmentation_basic_transactions_with_regions AS
   (
   SELECT *,
     CASE
     WHEN regionid IS NOT NULL THEN regionid 
     WHEN c_r_id.region IS NOT NULL THEN c_r_id.region
     WHEN REGEXP_CONTAINS(LOWER(campaign),"cis") AND REGEXP_CONTAINS(LOWER(campaign),"non_cis")=false THEN "cis"
     WHEN REGEXP_CONTAINS(LOWER(campaign),"uk|londo") THEN "uk"
     WHEN REGEXP_CONTAINS(LOWER(campaign),"spain|barcelon|madri|barselon") THEN "spain"
     WHEN REGEXP_CONTAINS(LOWER(campaign),"mena|uae|emirates|arab|duba") THEN "mena"
     WHEN REGEXP_CONTAINS(LOWER(campaign),"russia") THEN "russia"
     END as region_name, 
   FROM segments
   LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_city_all` as cities ON CAST(cities.city_id AS STRING)=CAST(segments.city_id AS STRING)
   LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_country` as countries ON CAST(countries.country_id AS STRING)=CAST(cities.country_id AS STRING)
   LEFT JOIN
     (SELECT CAST(id AS STRING) as id, regionid
     FROM `funnel-flowwow.BUSINESS_DM.regions_gs_view`) as r_ids
     ON r_ids.id=CAST(segments.region_id AS STRING)
   LEFT JOIN `funnel-flowwow.BUSINESS_DM.m_countries_regions` as c_r_id ON c_r_id.country_id=countries.country_id
   ),
 segmentation_basic_transactions_retargering AS
   (
   SELECT *,
   CASE 
   WHEN segment IN ("facebook_ads","google_ads_uac","google_ads_web_kms","google_ads_web_non_brand","google_ads_web_brand","mytarget","vk_ads","yandex_direct_app","yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy")
   AND (

 REGEXP_CONTAINS(LOWER(campaign),"ретарг|ремарк|retarg|remark|exist") OR 
 (source IN ("yandex","google") AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|retargering|ретаргетинг|rlsa|рлса|rmk")) OR 
 LOWER(source)="yd_retargeting"


 ) THEN 1 ELSE 0
   END as is_retargeting_campaign
   FROM segmentation_basic_transactions_with_regions
   ),
 channels AS
   (
   SELECT *,
   CASE
   WHEN segment IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy") AND is_retargeting_campaign!=1 THEN "yandex_direct_web"
   WHEN segment IN ("google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") AND is_retargeting_campaign!=1  THEN "google_ads_web"
   WHEN REGEXP_CONTAINS(segment,"seo") AND is_retargeting_campaign!=1 THEN "seo"
   WHEN segment IN ("asa_brand", "asa_non_brand") AND is_retargeting_campaign!=1 THEN "apple_search_ads"
   WHEN segment IN ("two_gis","yandex_maps") AND is_retargeting_campaign!=1 THEN "geo_services"
   WHEN segment IN("regular_online", "special_offline","special_online") AND is_retargeting_campaign!=1 THEN "mediaflights"
   WHEN segment="all_promocode_integration" AND is_retargeting_campaign!=1 THEN "promocode_integration"
   WHEN segment IN ("vk_smm","instagram_smm", "telegram_smm","instagram_global_smm", "twitter_global_smm", "snapchat_global_smm") AND is_retargeting_campaign!=1 THEN "smm"
   WHEN segment IN ("flowwow_com","invite_friend") AND is_retargeting_campaign!=1 THEN "aso_organic"
   WHEN segment="aura" AND is_retargeting_campaign!=1 THEN "predownloads"
   WHEN segment IN ("dzen_client","ya_promo", "pikabu") AND is_retargeting_campaign!=1 THEN "content_fw_client"
   WHEN is_retargeting_campaign=1 AND segment IN ("facebook_ads","google_ads_uac","google_ads_web_kms","google_ads_web_non_brand","mytarget","vk_ads","yandex_direct_app","yandex_direct_web_non_brand","yandex_direct_web_rsy") THEN"retargeting"
   WHEN segment="all_cashback" AND is_retargeting_campaign!=1 THEN "cashback"
   WHEN segment="tg_ads" AND is_retargeting_campaign!=1 THEN "apps"
   ELSE segment
   END AS channel

   FROM segmentation_basic_transactions_retargering
   ),
 subdivisions AS
   (
   SELECT *,
   CASE
   WHEN segment IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy") OR segment IN ("google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") THEN "paid_web"
   WHEN REGEXP_CONTAINS(segment,"seo") OR segment="direct" THEN "organic_web"
   WHEN segment IN ("vk_ads","mytarget","asa_brand","asa_non_brand","yandex_direct_app","google_ads_uac","tiktok_app","appnext","cpa","unity","facebook_ads","aura","tg_ads") THEN "paid_apps"
   WHEN segment IN ("email_push") THEN "retention"
   WHEN segment IN ("aso_organic","incentive_traffic","sms","flowwow_com","invite_friend") THEN "organic_apps"
   WHEN segment IN ("all_promocode_integration","all_cashback","two_gis","donate","yandex_maps") THEN "partners"
   WHEN segment IN ("vk_smm","telegram_smm","orm","offline_merch","pr","pr_telegram","influencers","instagram_smm", "instagram_global_smm","creatives","regular_online","special_offline","special_online","dzen_client","ya_promo", "pikabu", "global_twitter_smm", "global_snapchat_smm") THEN "brand"
   WHEN segment="other" THEN "other"
   END AS subdivision,
   regionid as region
   FROM channels
   )


 SELECT
 date, subdivision, channel, segment, LOWER(source) as source, LOWER(medium) as medium, LOWER(campaign) as campaign, is_retargeting_campaign, is_world_campaign, CAST(null AS STRING) as attributed_by, platform, 
 IF(city_from IS NULL OR city_from="", "Москва", city_from) as city_from, city_to, country_from, country_to, IF(region_name IS NULL, "no_region",region_name) as region,
  category_name as category, sub_category_name as subcategory,
 COUNTIF(category_name IS NOT NULL AND city_to IS NOT NULL) as distinct_views,
 COUNTIF(category_name IS NOT NULL AND city_to IS NOT NULL AND first_session=TRUE) distinct_first_views

 FROM subdivisions
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)