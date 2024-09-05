
create or replace  table
funnel-flowwow.Analyt_KosarevS.A2_cm_date_subdivision_etc_transactions
 as
(

WITH
 segmentation_attribution AS
   (
   SELECT *,
   --CASE
   --WHEN source IS NULL AND _network_name_ IS NULL THEN "none"
   --WHEN source IS NOT NULL THEN "owox"
   --ELSE "adjust"

   CASE WHEN source IS NOT NULL THEN "owox"
   ELSE "adjust"
   END AS attributed_by,

   FROM `funnel-flowwow.Analyt_KosarevS.A0_date_source_medium_campaign_cities_categories_platform_transactions`
   ),
 segmentation_owox_or_adjust as 
   (
   SELECT * EXCEPT(source,campaign,_network_name_,_campaign_name_),
   CASE
   WHEN attributed_by="owox" THEN source
   WHEN attributed_by="adjust" THEN _network_name_
   --WHEN attributed_by="none" THEN NULL
   ELSE null
   END as source,
   CASE
   WHEN attributed_by="owox" THEN campaign
   WHEN attributed_by="adjust" THEN _campaign_name_
   --WHEN attributed_by="none" THEN NULL
   ELSE null
   END as campaign,  
   FROM segmentation_attribution
   ),
 segmentation_flowwow_com AS 
   (
   SELECT 
   * EXCEPT(source, medium, campaign),
   CASE WHEN source="flowwow_com" AND source_from_web IS NOT NULL THEN source_from_web ELSE source END as source,
   CASE WHEN source="flowwow_com" AND source_from_web IS NOT NULL THEN medium_from_web ELSE medium END as medium,
   CASE WHEN source="flowwow_com" AND source_from_web IS NOT NULL THEN campaign_from_web ELSE campaign END as campaign,
   FROM segmentation_owox_or_adjust
   ),
 segmentation_basic_transactions AS
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
   WHEN LOWER(source) IN("apple_search_ads","apple search ads") AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") THEN "asa_brand"
   WHEN LOWER(source) IN("apple_search_ads","apple search ads") AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")=false THEN "asa_non_brand"
   WHEN REGEXP_CONTAINS(LOWER(source),"tiktok") THEN "tiktok_app"
   WHEN REGEXP_CONTAINS(LOWER(source),"unattributed") THEN "facebook_ads"
   WHEN REGEXP_CONTAINS(LOWER(source),"facebook") AND REGEXP_CONTAINS(LOWER(campaign),"post-boosting")=false THEN "facebook_ads"

   WHEN LOWER(source)="appnext_android" THEN "appnext"
   WHEN REGEXP_CONTAINS(LOWER(source),"unity") AND REGEXP_CONTAINS(LOWER(source),"reliz|adchampag|hybe")=false THEN "unity"

   WHEN REGEXP_CONTAINS(LOWER(source),"reliz|adchampagne|hybe") THEN "cpa"

   WHEN REGEXP_CONTAINS(LOWER(source),"2gis|2gis_context") THEN "two_gis"

   WHEN source="google" AND LOWER(medium)="organic" THEN "google_seo"
   WHEN (source="yandex" AND LOWER(medium)="organic") OR (source = "yandex.ru" AND medium = "referral") THEN "yandex_seo"
   WHEN source ="(direct)" THEN "direct"

   WHEN LOWER(source) IN ("organic", "imported devices", "untrusted devices", "google organic search") THEN "aso_organic"
   WHEN LOWER(source)="flowwow_com" THEN "flowwow_com"
   WHEN LOWER(source)="invite_friend" THEN "invite_friend"

   WHEN REGEXP_CONTAINS(LOWER(source),"email|push") THEN "email_push"
   WHEN REGEXP_CONTAINS(LOWER(source),"sms") THEN "sms"

   WHEN LOWER(source) IN ("instagram_influencers","youtube","podcast","twitter","telegram","tiktok","dzen","stream","cpa", "instagram_influencers_global","telegram_global","partners_global") THEN "influencers"


   WHEN LOWER(source) IN ("vkontakte_groups", "vkontakte_our_group","vk_bio", "vk_advertisment", "vk_influencers", "vkontakte_stories") THEN "vk_smm"
   WHEN LOWER(source) IN ("instagram_bio", "instagram_stories") THEN "instagram_smm" 
   WHEN REGEXP_CONTAINS(LOWER(campaign),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign),"wrld|world")=false THEN "instagram_smm"
   WHEN LOWER(source) IN ("instagram.global_bio","global_bio", "worldwide") THEN "instagram_global_smm"
   WHEN REGEXP_CONTAINS(LOWER(campaign),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign),"wrld|world") THEN "instagram_global_smm"

   WHEN REGEXP_CONTAINS(LOWER(campaign),"flight")  THEN "regular_online"
   WHEN REGEXP_CONTAINS(LOWER(source),"partners_event") THEN "special_offline"
   WHEN REGEXP_CONTAINS(LOWER(source),"partners_media") THEN "special_online"

   WHEN REGEXP_CONTAINS(LOWER(source),"partners_staff|partners_loyalty") THEN "all_promocode_integration"
   WHEN source="partners_cashback" THEN "all_cashback"
   WHEN LOWER(source) IN ("telegram_bio", "telegram_product") THEN "telegram_smm"
   WHEN REGEXP_CONTAINS(LOWER(source),"aura") THEN "aura"
   WHEN REGEXP_CONTAINS(LOWER(source),"ya_promo") OR REGEXP_CONTAINS(LOWER(campaign),"yapromo") THEN "ya_promo"
   WHEN REGEXP_CONTAINS(LOWER(source),"tgbot") THEN "tg_ads"
   WHEN REGEXP_CONTAINS(LOWER(source),"xapads") THEN "xapads"
   WHEN REGEXP_CONTAINS(LOWER(source),"dzen_clients") OR REGEXP_CONTAINS(LOWER(campaign),"dzen") THEN "dzen"
   WHEN REGEXP_CONTAINS(LOWER(source),"yandex_maps")  THEN "yandex_maps" 
   WHEN REGEXP_CONTAINS(LOWER(source),"pikabu")  THEN "pikabu_smm" 

   ELSE "other"

   END AS segment,
   CAST(IF(REGEXP_CONTAINS(LOWER(campaign),"wrld|world"),1,0) AS INT64) as is_world_campaign,

   CASE
   --WHEN source="google" AND REGEXP_CONTAINS(LOWER(campaign),'ios|andr|app|uac') AND REGEXP_CONTAINS(LOWER(campaign), ".*nocity.*") THEN  REGEXP_EXTRACT(campaign, "1")
   WHEN REGEXP_CONTAINS(LOWER(campaign),"cityid") THEN REGEXP_EXTRACT(campaign, r"cityid-([0-9]+)")
   WHEN REGEXP_CONTAINS(LOWER(campaign),"city") THEN REGEXP_EXTRACT(campaign, r"city-([0-9]+)")
   ELSE "without_city" 
   END AS city_id,

   CASE 
   WHEN REGEXP_CONTAINS(campaign,"regionid-") THEN REGEXP_EXTRACT(campaign,r"regionid-([0-9]+)")
   ELSE REGEXP_EXTRACT(campaign,r"regionid_([0-9]+)")
   END AS region_id

   FROM segmentation_flowwow_com
   ),
 segmentation_basic_transactions_with_regions AS
   (
   SELECT *,
     CASE
     WHEN regionid IS NOT NULL THEN regionid 
     WHEN c_r_id.region IS NOT NULL THEN c_r_id.region
     WHEN m_c_r.region IS NOT NULL THEN m_c_r.region
     WHEN REGEXP_CONTAINS(LOWER(campaign),"cis") AND REGEXP_CONTAINS(LOWER(campaign),"non_cis")=false THEN "cis"
     WHEN REGEXP_CONTAINS(LOWER(campaign),"uk|londo") THEN "uk"
     WHEN REGEXP_CONTAINS(LOWER(campaign),"spain|barcelon|madri|barselon") THEN "spain"
     WHEN REGEXP_CONTAINS(LOWER(campaign),"mena|uae|emirates|arab|duba") THEN "mena"
     WHEN REGEXP_CONTAINS(LOWER(campaign),"russia") THEN "russia"
     END as region_name, 
   FROM segmentation_basic_transactions
   LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_city_all` as cities ON CAST(cities.city_id AS STRING)=CAST(segmentation_basic_transactions.city_id AS STRING)
   LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_country` as countries ON CAST(countries.country_id AS STRING)=CAST(cities.country_id AS STRING)
   LEFT JOIN
     (SELECT CAST(id AS STRING) as id, regionid
     FROM `funnel-flowwow.BUSINESS_DM.regions_gs_view`) as r_ids
     ON r_ids.id=CAST(segmentation_basic_transactions.region_id AS STRING)
   LEFT JOIN `funnel-flowwow.BUSINESS_DM.m_countries_regions` as c_r_id ON c_r_id.country_id=countries.country_id
   LEFT JOIN `funnel-flowwow.BUSINESS_DM.m_countries_regions` as m_c_r ON m_c_r.country=inf_country
   ),
 segmentation_basic_transactions_promo AS
   (
   SELECT * EXCEPT(segment),
   CASE
   WHEN if_promo IS NOT NULL AND segment IN ("other","aso_organic","direct","flowwow_com","google_seo","incentive_traffic","invite_friend","sms","yandex_seo") 
     AND if_promo="partner" THEN "all_promocode_integration"
   WHEN if_promo IS NOT NULL AND segment IN ("other","aso_organic","direct","flowwow_com","google_seo","incentive_traffic","invite_friend","sms","yandex_seo") 
     AND if_promo="promo_pr_international" THEN "pr_international"
   WHEN if_promo IS NOT NULL AND segment IN ("other","aso_organic","direct","flowwow_com","google_seo","incentive_traffic","invite_friend","sms","yandex_seo") 
     AND if_promo="promo_pr_rus" THEN "pr_rus"
   WHEN if_promo IS NOT NULL AND segment IN ("other","aso_organic","direct","flowwow_com","google_seo","incentive_traffic","invite_friend","sms","yandex_seo") 
     AND if_promo="influencer" THEN "influencers" 
   WHEN if_promo IS NOT NULL AND segment IN ("other","aso_organic","direct","flowwow_com","google_seo","incentive_traffic","invite_friend","sms","yandex_seo") 
     AND if_promo="marketing" THEN "email_push" 
   WHEN if_promo IS NOT NULL AND segment IN ("other","aso_organic","direct","flowwow_com","google_seo","incentive_traffic","invite_friend","sms","yandex_seo") 
     AND if_promo="friends"THEN "invite_friend"
   WHEN if_promo IS NOT NULL AND segment IN ("other","aso_organic","direct","flowwow_com","google_seo","incentive_traffic","invite_friend","sms","yandex_seo") 
     AND if_promo="cpa_partners" THEN "partners_cpa" 
   WHEN if_promo IS NOT NULL AND segment IN ("other","aso_organic","direct","flowwow_com","google_seo","incentive_traffic","invite_friend","sms","yandex_seo") 
     AND if_promo="orm" THEN "orm"   

   ELSE segment
   END as segment

   FROM segmentation_basic_transactions_with_regions
   ),
 segmentation_basic_transactions_retargering AS
   (
   SELECT *,
   CASE 
   WHEN segment IN ("facebook_ads","google_ads_uac","google_ads_web_kms","google_ads_web_non_brand", "google_ads_web_brand", "mytarget","vk_ads","yandex_direct_app","yandex_direct_web_non_brand","yandex_direct_web_brand", "yandex_direct_web_rsy")
   AND (

 REGEXP_CONTAINS(LOWER(campaign),"ретарг|ремарк|retarg|remark|exist") OR 
 (source IN("yandex","google") AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|retargering|ретаргетинг|rlsa|рлса|rmk")) OR 
 LOWER(source)="yd_retargeting"


 ) THEN 1 ELSE 0
   END as is_retargeting_campaign
   FROM segmentation_basic_transactions_promo
   ),
 channels_transactions AS
   (
   SELECT *,
   CASE
   WHEN segment IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy") AND is_retargeting_campaign!=1 THEN "yandex_direct_web"
   WHEN segment IN ("google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") AND is_retargeting_campaign!=1  THEN "google_ads_web"
   WHEN REGEXP_CONTAINS(segment,"seo") AND is_retargeting_campaign!=1 THEN "seo"
   WHEN segment IN ("asa_brand", "asa_non_brand") AND is_retargeting_campaign!=1 THEN "apple_search_ads"
   WHEN segment IN ("two_gis", "yandex_maps") AND is_retargeting_campaign!=1 THEN "geo_services"
   WHEN segment IN("regular_online", "special_offline","special_online") AND is_retargeting_campaign!=1 THEN "mediaflights"
   WHEN segment="all_promocode_integration" AND is_retargeting_campaign!=1 THEN "promocode_integration"
   WHEN segment IN ("vk_smm","instagram_smm", "telegram_smm","instagram_global_smm") AND is_retargeting_campaign!=1 THEN "smm"
   WHEN segment IN ("flowwow_com","invite_friend") AND is_retargeting_campaign!=1 THEN "aso_organic"
   WHEN segment="aura" AND is_retargeting_campaign!=1 THEN "predownloads"
   WHEN segment="all_cashback" AND is_retargeting_campaign!=1 THEN "cashback"
   WHEN segment IN ("dzen_client","ya_promo", "pikabu_smm") AND is_retargeting_campaign!=1 THEN "content_fw_client"
   WHEN is_retargeting_campaign=1 AND segment IN ("facebook_ads","google_ads_uac","mytarget","vk_ads","yandex_direct_app","google_ads_web_kms","google_ads_web_non_brand", "google_ads_web_brand", "yandex_direct_web_non_brand","yandex_direct_web_rsy") THEN "retargeting"
   WHEN segment IN ("pr_rus","pr_international") THEN "pr"
   WHEN segment IN ("partners_cpa") THEN "partners_cpa"
   WHEN segment IN ("orm") THEN "orm"
   WHEN segment IN ("xapads") THEN "xapads"
   WHEN segment IN ("tg_ads") THEN "apps"
   ELSE segment
   END AS channel

   FROM segmentation_basic_transactions_retargering
   ),
 subdivisions_transactions AS
   (
   SELECT *,
   CASE
   WHEN segment IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy") OR segment IN ("google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") THEN "paid_web"
   WHEN REGEXP_CONTAINS(segment,"seo") OR segment="direct" THEN "organic_web"
   WHEN segment IN ("vk_ads","mytarget","asa_brand","asa_non_brand","yandex_direct_app","google_ads_uac","tiktok_app","appnext","cpa","unity","facebook_ads","aura", "xapads", "tg_ads") THEN "paid_apps"
   WHEN segment IN ("email_push") THEN "retention"
   WHEN segment IN ("aso_organic","incentive_traffic","sms","flowwow_com","invite_friend") THEN "organic_apps"
   WHEN segment IN ("all_promocode_integration","all_cashback","two_gis","donate","cashback", "partners_cpa", "yandex_maps") THEN "partners"
   WHEN segment IN ("vk_smm","telegram_smm","orm","offline_merch","pr_rus","pr_international","influencers","instagram_smm", "instagram_global_smm","creatives","regular_online","special_offline","special_online","dzen","ya_promo", "pikabu_smm") THEN "brand"
   WHEN segment="other" THEN "other"
   END AS subdivision
   FROM channels_transactions
   )


 SELECT date, subdivision, channel, segment, source, medium, campaign, is_retargeting_campaign, is_world_campaign, attributed_by, city_from, city_to, country_from, country_to,
 IF(region_name IS NULL, "no_region",region_name) as region,
 category, subcategory, platform,
 SUM(purchases) as purchases, SUM(first_purchases) as first_purchases,
 SUM(transactions) as transactions, SUM(first_transactions) as first_transactions, SUM(revenue) as revenue, SUM(revenue_first_transactions) as revenue_first_transactions,
 SUM(promo_cost) as promo_cost, SUM(bonus_company) as bonus_company
 FROM subdivisions_transactions
 GROUP BY date, subdivision, channel, segment, source, medium, campaign, is_retargeting_campaign, is_world_campaign,attributed_by, city_to, city_from, country_from, country_to, region_name, category, subcategory, platform

);