create or replace  table
funnel-flowwow.Analyt_KosarevS.A0_cohorts_from_first_purchase
as
(
WITH
countries AS
  (
  SELECT country_id, country
  FROM `funnel-flowwow.MYSQL_EXPORT.f_country`
  GROUP BY 1,2
  ),
cities AS
  (
  SELECT city_id, country_id
  FROM `funnel-flowwow.MYSQL_EXPORT.f_city_all`
  GROUP BY 1,2
  ),
city_id_country AS
  (
  SELECT city_id, country
  FROM cities
  LEFT JOIN countries
  USING(country_id)
  ),
city_id_country_2 AS
  (
  SELECT city_id, country
  FROM cities
  LEFT JOIN countries
  USING(country_id)
  ),
categories AS
  (
  SELECT category_id, category_name, sub_category_id, sub_category_name
  FROM `funnel-flowwow.ANALYTICS_DM.category_subcategory`
  GROUP BY 1,2,3,4
  ),
count_table AS
  (
  SELECT purchase_id, COUNT(*) as count_purchase_id
  FROM `funnel-flowwow.BUSINESS_DM.cm_order_sku_categories_with_additions`
  GROUP BY 1
  ),
first_table AS
  (
  SELECT
  a.user_id, purchase_id, product_category_id, product_subcategory_id, b.city_name as city_to, DATE(purchase_timestamp) as date, product_price, city_from_name as city_from, city_id_country_2.country as country_from, city_id_country.country as country_to,  count_purchase_id, category_name as category, sub_category_name as subcategory_name, platform, promocode
  FROM `funnel-flowwow.BUSINESS_DM.cm_order_sku_categories_with_additions`  as a
  LEFT JOIN `funnel-flowwow.CRM_DM_PRTND.crm_com` as b
  USING(purchase_id)
  LEFT JOIN count_table
  USING(purchase_id)
  LEFT JOIN categories
  ON categories.category_id=a.product_category_id AND categories.sub_category_id=a.product_subcategory_id
  LEFT JOIN city_id_country
  ON b.city_id=city_id_country.city_id
  LEFT JOIN city_id_country_2
  ON b.city_from_id=city_id_country_2.city_id
  WHERE b.paid=1 AND b.order_sequence=1
  ),
repeat_table AS
  (
  SELECT
  a.user_id, purchase_id as purchase_id_repeat, product_category_id as product_category_id_repeat, product_subcategory_id as product_subcategory_id_repeat, b.city_name as city_to_repeat, DATE(purchase_timestamp) as date_repeat, product_price as revenue_repeat, city_from_name as city_from_repeat, count_purchase_id as count_purchase_id_repeat, category_name as category_repeat, sub_category_name as subcategory_name_repeat
  FROM `funnel-flowwow.BUSINESS_DM.cm_order_sku_categories_with_additions`  as a
  LEFT JOIN `funnel-flowwow.CRM_DM_PRTND.crm_com` as b
  USING(purchase_id)
  LEFT JOIN count_table
  USING(purchase_id)
  LEFT JOIN categories
  ON categories.category_id=a.product_category_id AND categories.sub_category_id=a.product_subcategory_id
  WHERE b.paid=1 AND b.order_sequence>1
  ),
owox AS  
  (
  SELECT *
  FROM (
	SELECT  
	SAFE_CAST(transaction.transactionid AS NUMERIC) as purchase_id, trafficSource.source, trafficSource.medium, trafficSource.campaign
	FROM `funnel-flowwow.OWOXBI_Streaming_Google_Analytics.owoxbi_sessions_*` ,
	UNNEST(hits) as transactions
	WHERE SAFE_CAST(transaction.transactionid AS NUMERIC) IS NOT NULL AND date<"2023-02-01"
	QUALIFY ROW_NUMBER() OVER(PARTITION BY transaction.transactionid ORDER BY transactions.timestamp)=1
    
    
	UNION ALL

	SELECT
	SAFE_CAST(ecommerce.transaction_id AS NUMERIC) as purchase_id,
	traffic_source.source,
	traffic_source.medium,
	traffic_source.name as campaign
	FROM `firebase-flowwow.analytics_150948805.events_2023*`
	WHERE stream_id="3464829917" AND PARSE_DATE("%Y%m%d", event_date)>="2023-02-01" AND ecommerce.transaction_id!="(not set)"
	QUALIFY ROW_NUMBER() OVER(PARTITION BY SAFE_CAST(ecommerce.transaction_id AS NUMERIC))=1
    
	)
	QUALIFY ROW_NUMBER() OVER(PARTITION BY purchase_id)=1
  ),
adjust AS
  (
  SELECT  
  CAST(_purchase_id_ AS NUMERIC) as purchase_id,
  _network_name_, _campaign_name_
  FROM `funnel-flowwow.ADJUST_RAW.clients_app`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(_purchase_id_ AS NUMERIC) ORDER BY _created_at_)=1
  ),
segmentation_attribution AS
  (
  SELECT * EXCEPT(medium), IF(personal_promocode IS NOT NULL,"influencer",
  medium) as medium,

  CASE WHEN _network_name_ IS NOT NULL THEN "adjust"
  ELSE "owox"
  END AS attributed_by,


  FROM first_table
  LEFT JOIN repeat_table
  USING(user_id)
  LEFT JOIN owox
  USING(purchase_id)
  LEFT JOIN adjust
  USING(purchase_id)

  LEFT JOIN (SELECT
  LOWER(personal_promocode) as personal_promocode
  FROM `funnel-flowwow.flowwow_data_sets_from_google_sheets.promocodes_influencers`
  WHERE personal_promocode IS NOT NULL AND personal_promocode!="none"
  QUALIFY ROW_NUMBER() OVER(PARTITION BY personal_promocode)=1) as c
  ON LOWER(first_table.promocode)=c.personal_promocode


  ),
segmentation_owox_or_adjust AS
  (
  SELECT * EXCEPT(source,campaign,_network_name_,_campaign_name_, medium), LOWER(medium) as medium,
  CASE
  WHEN attributed_by="owox" THEN LOWER(source)
  WHEN attributed_by="adjust" THEN LOWER(_network_name_)
  --WHEN attributed_by="none" THEN NULL
  ELSE null
  END as source,
  CASE
  WHEN attributed_by="owox" THEN LOWER(campaign)
  WHEN attributed_by="adjust" THEN LOWER(_campaign_name_)
  --WHEN attributed_by="none" THEN NULL
  ELSE null
  END as campaign,  
  FROM segmentation_attribution
  ),
segmentation_basic_transactions AS
  (
  SELECT
  *,
  CASE
  WHEN REGEXP_CONTAINS(LOWER(campaign),"flight")=false AND  medium NOT IN ("influencer","partner","friends","marketing") AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|flight|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм|yapromo")=false AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false AND REGEXP_CONTAINS(LOWER(campaign),"video")=false THEN "yandex_direct_web_non_brand"
  WHEN REGEXP_CONTAINS(LOWER(campaign),"flight")=false AND medium NOT IN ("influencer","partner","friends","marketing") AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|yapromo")=false THEN "yandex_direct_web_brand"
  WHEN REGEXP_CONTAINS(LOWER(campaign),"flight")=false AND medium NOT IN ("influencer","partner","friends","marketing")  AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"рся|rsy|network|retarget|smart") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|brand|бренд|yapromo")=false THEN "yandex_direct_web_rsy"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм")=false AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|uac")=false THEN "google_ads_web_non_brand"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false THEN "google_ads_web_brand"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"кмс|kms|network|retarget") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false THEN "google_ads_web_kms"

  WHEN REGEXP_CONTAINS(LOWER(campaign),"ios|android|app|uac") AND REGEXP_CONTAINS(LOWER(campaign),"flight|yapromo")=false AND REGEXP_CONTAINS(LOWER(campaign), 'rem') = false AND source IN ("yandex direct") THEN "yandex_direct_app"

  WHEN REGEXP_CONTAINS(LOWER(campaign),"ios|android|app|uac") AND REGEXP_CONTAINS(LOWER(campaign),"flight|yapromo")=false AND REGEXP_CONTAINS(LOWER(campaign), 'rem') AND source IN ("yd_retargeting") THEN "yandex_direct_app"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("google ads aci","google ads ace") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac") THEN "google_ads_uac"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN("target.my.com","mycom","mytarget", "mt_ios_feed", "mt_feed", "mytarget_ios") AND date <= "2023-02-28" THEN "mytarget"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN("vk apps", "vk_android") /*AND medium!="influencer"*/ AND REGEXP_CONTAINS(LOWER(campaign),"dzen_client")=false THEN "vk_ads"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN("apple_search_ads","apple search ads") AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")  THEN "asa_brand"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN("apple_search_ads","apple search ads") AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")=false THEN "asa_non_brand"
  WHEN REGEXP_CONTAINS(LOWER(source),"tiktok") AND medium NOT IN ("influencer","partner","friends","marketing")   THEN "tiktok_app"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"unattributed") THEN "facebook_ads"  
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"facebook") AND REGEXP_CONTAINS(LOWER(campaign),"post-boosting")=false THEN "facebook_ads"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source)="appnext_android" THEN "appnext"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"unit") THEN "unity"

  --WHEN REGEXP_CONTAINS(LOWER(source),"alfale|mobup|mobap|dipper|wakeapp|toptr|admita|appalg|zumoads_litchiads|adchampagne_unity") OR REGEXP_CONTAINS(LOWER(medium),"lfale|mobup|mobap|dipper|wakeapp|toptr|admita|appalg|zumoads_litchiads|adchampagne_unity") THEN "cpa_inactive"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"reliz|adchampagne") THEN "cpa"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"2gis") THEN "two_gis"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND source="google" AND LOWER(medium)="organic" THEN "google_seo"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND ((source="yandex" AND LOWER(medium)="organic") OR (source = "yandex.ru" AND medium = "referral")) THEN "yandex_seo"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND source ="(direct)" THEN "direct"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("organic", "imported devices", "untrusted devices", "google organic search") THEN "aso_organic"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source)="flowwow_com" THEN "flowwow_com"
  WHEN (medium ="friends") OR (LOWER(source)="invite_friend" AND medium NOT IN ("influencer","partner","marketing")) THEN "invite_friend"

  WHEN (medium ="marketing") OR (REGEXP_CONTAINS(LOWER(source),"email|push") AND medium NOT IN ("influencer","partner","friends")) THEN "email_push"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"sms") THEN "sms"

  WHEN medium="influencer" OR LOWER(source) IN ("instagram_influencers","youtube","podcast","twitter","telegram","tiktok","dzen","stream","cpa") THEN "influencers"



  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("vkontakte_groups", "vkontakte_our_group","vk_bio") THEN "smm_vk"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("instagram_bio", "instagram_stories") THEN "smm_instagram" 
  WHEN REGEXP_CONTAINS(LOWER(campaign),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign),"wrld|world")=false THEN "smm_instagram"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("instagram.global_bio","global_bio", "worldwide") THEN "smm_instagram_global"
  WHEN REGEXP_CONTAINS(LOWER(campaign),"post-boosting") AND REGEXP_CONTAINS(LOWER(campaign),"wrld|world") THEN "smm_instagram_global"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(campaign),"flight")  THEN "regular_online"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"partners_event") THEN "special_offline"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"partners_media") THEN "special_online"

  WHEN medium NOT IN ("influencer","friends","marketing") AND (medium="partner" OR REGEXP_CONTAINS(LOWER(source),"partners_staff|partners_loyalty")) THEN "all_promocode_integration"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("telegram_bio", "telegram_product") THEN "smm_telegram"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"aura") THEN "aura"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"ya_promo") OR REGEXP_CONTAINS(LOWER(campaign),"yapromo") THEN "ya_promo"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND (REGEXP_CONTAINS(LOWER(source),"dzen_clients") OR REGEXP_CONTAINS(LOWER(campaign),"dzen_client")) THEN "dzen_client"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"yandex_maps") THEN "yandex_maps"
  WHEN REGEXP_CONTAINS(LOWER(source),"pikabu") THEN "smm_pikabu"
  WHEN REGEXP_CONTAINS(LOWER(source),"tgbot") THEN "tg_ads"


  ELSE "other"

  END AS segment,
  CAST(IF((REGEXP_CONTAINS(LOWER(campaign),"ретарг|ремарк|retarg|remark|exist")) OR (source IN ("yandex","google") AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|retargering|ретаргетинг|rlsa|рлса|rmk")),1,0) AS INT64) as is_retargeting_campaign,
  CAST(IF(REGEXP_CONTAINS(LOWER(campaign),"wrld|world"),1,0) AS INT64) as is_world_campaign,
  CASE 
  WHEN REGEXP_CONTAINS(campaign,"regionid") THEN REGEXP_EXTRACT(campaign,r"regionid-([0-9]+)")
  END AS region_id

  FROM segmentation_owox_or_adjust
  ),
channels_transactions AS
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
  WHEN segment IN ("smm_vk","smm_instagram", "smm_telegram","smm_instagram_global") AND is_retargeting_campaign!=1 THEN "smm"
  WHEN segment IN ("flowwow_com","invite_friend") AND is_retargeting_campaign!=1 THEN "aso_organic"
  WHEN segment="aura" AND is_retargeting_campaign!=1 THEN "predownloads"
  WHEN segment IN ("dzen_client","ya_promo", "smm_pikabu") AND is_retargeting_campaign!=1 THEN "content_fw_client"
  WHEN segment="tg_ads" AND is_retargeting_campaign!=1 THEN "apps"
  WHEN is_retargeting_campaign=1 AND segment IN ("facebook_ads","google_ads_uac","mytarget","vk_ads","yandex_direct_app","google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") THEN "retargeting"
  ELSE segment
  END AS channel

  FROM segmentation_basic_transactions
  ),
subdivisions_transactions AS
  (
  SELECT *,
  CASE
  WHEN segment IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy") OR segment IN ("google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") THEN "paid_web"
  WHEN REGEXP_CONTAINS(segment,"seo") OR segment="direct" THEN "organic_web"
  WHEN segment IN ("vk_ads","mytarget","asa_brand","asa_non_brand","yandex_direct_app","google_ads_uac","tiktok_app","appnext","cpa","unity","facebook_ads","aura", "tg_ads") THEN "paid_apps"
  WHEN segment IN ("email_push") THEN "retention"
  WHEN segment IN ("aso_organic","incentive_traffic","sms","flowwow_com","invite_friend") THEN "organic_apps"
  WHEN segment IN ("all_promocode_integration","all_cashback","two_gis","donate","yandex_maps") THEN "partners"
  WHEN segment IN ("smm_vk","smm_telegram","orm","offline_merch","pr","pr_telegram","influencers","smm_instagram", "smm_instagram_global","creatives","regular_online","special_offline","special_online","dzen_client","ya_promo", "smm_pikabu", "smm_global_twitter", "smm_global_snapchat") THEN "brand"
  WHEN segment="other" THEN "other"
  END AS subdivision,
  regionid as region
  FROM channels_transactions as a
  LEFT JOIN
  (SELECT CAST(id AS STRING) as id, regionid
  FROM `funnel-flowwow.BUSINESS_DM.regions_gs_view`) as r_ids
  ON r_ids.id=CAST(a.region_id AS STRING)
  )
SELECT
subdivision, channel, segment, attributed_by, source, medium, campaign, is_retargeting_campaign, is_world_campaign,
user_id, purchase_id as id_of_first_purchase, count_purchase_id, date as date_of_first_purchase, city_to, city_from, country_from, country_to, region, platform,
product_price as revenue_first, category, subcategory_name as subcategory,
purchase_id_repeat, count_purchase_id_repeat, date_repeat as date_of_repeat_purchase, category_repeat, subcategory_name_repeat as subcategory_repeat, revenue_repeat


FROM subdivisions_transactions
);