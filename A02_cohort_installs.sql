create or replace  table
funnel-flowwow.Analyt_KosarevS.A0_cohort_installs
as

(


WITH
install_table AS
  (
  SELECT
  _adid_, DATE(DATETIME_ADD(_created_at_, INTERVAL 3 HOUR)) as date, _network_name_, _campaign_name_,
  CASE
  WHEN REGEXP_CONTAINS(LOWER(_os_name_),"ios") THEN "ios app"
  ELSE "android app"
  END AS platform,
  _city_,
  IF(_city_ IS NOT NULL,LOWER(_country_),null) as country_code
  FROM `funnel-flowwow.ADJUST_RAW.clients_app`
  WHERE _event_name_="first_open"
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _adid_ ORDER BY _created_at_)=1
  ),
countries AS
  (
  SELECT LOWER(code) as country_code, country
  FROM `funnel-flowwow.MYSQL_EXPORT.f_country`
  ),
crm_first AS
  (
  SELECT  
  purchase_id, purchase_sum_rub, DATE(purchase_date) as purchase_date
  FROM `funnel-flowwow.CRM_DM_PRTND.crm_com`
  WHERE paid=1 AND order_sequence=1
  ),
crm_repeat AS
  (
  SELECT  
  purchase_id, purchase_sum_rub, DATE(purchase_date) as purchase_date
  FROM `funnel-flowwow.CRM_DM_PRTND.crm_com`
  WHERE paid=1 AND order_sequence>1  
  ),
adjust_purchases AS
  (
  SELECT _adid_, _purchase_id_
  FROM `funnel-flowwow.ADJUST_RAW.clients_app`
  WHERE _purchase_id_ IS NOT NULL
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _purchase_id_ ORDER BY _created_at_)=1
  ),
adjust_first AS
  (
  SELECT _adid_, purchase_id as id_of_first_purchase, purchase_sum_rub as revenue_of_first_purchase, purchase_date as date_of_first_purchase
  FROM adjust_purchases
  LEFT JOIN crm_first
  ON adjust_purchases._purchase_id_=crm_first.purchase_id
  ),
adjust_repeat AS
  (
  SELECT _adid_, purchase_id as id_of_repeat_purchase, purchase_sum_rub as revenue_of_repeat_purchase, purchase_date as date_of_repeat_purchase
  FROM adjust_purchases
  LEFT JOIN crm_repeat
  ON adjust_purchases._purchase_id_=crm_repeat.purchase_id
  ),
 
installs_purchases AS
  (
  SELECT *,
  ROW_NUMBER() OVER(PARTITION BY _adid_, id_of_first_purchase ORDER BY date_of_first_purchase) as rn_first,
  ROW_NUMBER() OVER(PARTITION BY _adid_, id_of_repeat_purchase ORDER BY date_of_repeat_purchase) as rn_repeat,
 
  FROM (
      SELECT _adid_,
      date as date_of_install, _network_name_, _campaign_name_ , platform as platform, _city_ as city_from, country as country_from,
      id_of_first_purchase, revenue_of_first_purchase, date_of_first_purchase,
      id_of_repeat_purchase, revenue_of_repeat_purchase, date_of_repeat_purchase,
      IF(date_of_first_purchase>=date AND date_of_repeat_purchase>=date,1,0) as
      if_date_of_install_more_than_dates_of_purchases
      FROM install_table
      LEFT JOIN adjust_first
      USING(_adid_)
      LEFT JOIN adjust_repeat
      USING(_adid_)
      LEFT JOIN countries
      USING(country_code)
    )
  )
 
,
 
cohorts AS
  (
  SELECT
  date_of_install as date, LOWER(_network_name_) as source , CAST(null AS STRING) as medium, LOWER(_campaign_name_ ) as campaign, platform, city_from, country_from,
 
  COUNT(DISTINCT _adid_) as installs,
 
  COUNT(DISTINCT IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 6 ,id_of_first_purchase,null)) as first_transactions_from_install_7,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 13,id_of_first_purchase,null)) as first_transactions_from_install_14,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 29,id_of_first_purchase,null)) as first_transactions_from_install_30,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 89,id_of_first_purchase,null)) as first_transactions_from_install_90,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 364,id_of_first_purchase,null)) as first_transactions_from_install_365,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY)>=365,id_of_first_purchase,null)) as first_transactions_from_install_365_more,
 
  SUM(IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 6 AND rn_first=1,revenue_of_first_purchase,0)) as revenue_first_transactions_from_install_7,
  SUM(IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 13 AND rn_first=1,revenue_of_first_purchase,0)) as revenue_first_transactions_from_install_14,
  SUM(IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 29 AND rn_first=1,revenue_of_first_purchase,0)) as revenue_first_transactions_from_install_30,
  SUM(IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 89 AND rn_first=1,revenue_of_first_purchase,0)) as revenue_first_transactions_from_install_90,
  SUM(IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY) BETWEEN 0 AND 364 AND rn_first=1,revenue_of_first_purchase,0)) as revenue_first_transactions_from_install_365,
  SUM(IF(DATE_DIFF(date_of_first_purchase,date_of_install,DAY)>=365 AND rn_first=1,revenue_of_first_purchase,0)) as revenue_first_transactions_from_install_365_more,
 
 
  COUNT(DISTINCT IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 6,id_of_repeat_purchase,null)) as repeat_transactions_from_install_7,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 13,id_of_repeat_purchase,null)) as repeat_transactions_from_install_14,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 29,id_of_repeat_purchase,null)) as repeat_transactions_from_install_30,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 89,id_of_repeat_purchase,null)) as repeat_transactions_from_install_90,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 364,id_of_repeat_purchase,null)) as repeat_transactions_from_install_365,
  COUNT(DISTINCT IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY)>=365,id_of_repeat_purchase,null)) as repeat_transactions_from_install_365_more,
 
  SUM(IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 6 AND rn_repeat=1,revenue_of_repeat_purchase,0)) as revenue_repeat_transactions_from_install_7,
  SUM(IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 13 AND rn_repeat=1,revenue_of_repeat_purchase,0)) as revenue_repeat_transactions_from_install_14,
  SUM(IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 29 AND rn_repeat=1,revenue_of_repeat_purchase,0)) as revenue_repeat_transactions_from_install_30,
  SUM(IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 89 AND rn_repeat=1,revenue_of_repeat_purchase,0)) as revenue_repeat_transactions_from_install_90,
  SUM(IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY) BETWEEN 0 AND 364 AND rn_repeat=1,revenue_of_repeat_purchase,0)) as revenue_repeat_transactions_from_install_365,
  SUM(IF(DATE_DIFF(date_of_repeat_purchase,date_of_install,DAY)>=365 AND rn_repeat=1,revenue_of_repeat_purchase,0)) as revenue_repeat_transactions_from_install_365_more,
 
 
  FROM installs_purchases
  GROUP BY 1,2,3,4,5,6,7
  ),
segments AS
  (
  SELECT
  *,
  CASE
  WHEN REGEXP_CONTAINS(LOWER(campaign),"flight")=false AND  medium NOT IN ("influencer","partner","friends","marketing") AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|flight|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм|yapromo")=false AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false AND REGEXP_CONTAINS(LOWER(campaign),"video")=false THEN "yandex_direct_web_non_brand"
  WHEN REGEXP_CONTAINS(LOWER(campaign),"flight")=false AND medium NOT IN ("influencer","partner","friends","marketing") AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|yapromo")=false THEN "yandex_direct_web_brand"
  WHEN REGEXP_CONTAINS(LOWER(campaign),"flight")=false AND medium NOT IN ("influencer","partner","friends","marketing")  AND source="yandex" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"рся|rsy|network|retarget|smart|src") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|brand|бренд|yapromo")=false THEN "yandex_direct_web_rsy"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм")=false AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|uac")=false THEN "google_ads_web_non_brand"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false THEN "google_ads_web_brand"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND source="google" AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"кмс|kms|network|retarget") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false THEN "google_ads_web_kms"

  WHEN REGEXP_CONTAINS(LOWER(campaign),"ios|android|app|uac") AND REGEXP_CONTAINS(LOWER(campaign),"flight|yapromo")=false AND REGEXP_CONTAINS(LOWER(campaign), 'rem') = false AND source IN ("yandex direct") THEN "yandex_direct_app"

  WHEN REGEXP_CONTAINS(LOWER(campaign),"ios|android|app|uac") AND REGEXP_CONTAINS(LOWER(campaign),"flight|yapromo")=false AND REGEXP_CONTAINS(LOWER(campaign), 'rem') AND source IN ("yd_retargeting") THEN "yandex_direct_app"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("google ads aci","google ads ace") AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac") THEN "google_ads_uac"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN("target.my.com","mycom","mytarget", "mt_ios_feed", "mt_feed", "mytarget_ios")  AND date <= "2023-02-28" THEN "mytarget"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN("vk apps", "vk_android") /*AND medium!="influencer"*/ AND REGEXP_CONTAINS(LOWER(campaign),"dzen_client")=false THEN "vk_ads"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN("apple_search_ads","apple search ads") AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")  THEN "asa_brand"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN("apple_search_ads","apple search ads") AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд")=false THEN "asa_non_brand"
  WHEN REGEXP_CONTAINS(LOWER(source),"tiktok") AND medium NOT IN ("influencer","partner","friends","marketing")   THEN "tiktok_app"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"facebook|unatributed") THEN "facebook_ads"

  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source)="appnext_android" THEN "appnext"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"unit") THEN "unity"

  --WHEN REGEXP_CONTAINS(LOWER(source),"alfale|mobup|mobap|dipper|wakeapp|toptr|admita|appalg|zumoads_litchiads|adchampagne_unity") OR REGEXP_CONTAINS(LOWER(medium),"lfale|mobup|mobap|dipper|wakeapp|toptr|admita|appalg|zumoads_litchiads|adchampagne_unity") THEN "cpa_inactive"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"reliz|adchampagne|hybe") THEN "cpa"

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
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("instagram.global_bio","global_bio", "worldwide") THEN "smm_instagram_global"



  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(campaign),"flight")  THEN "regular_online"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"partners_event") THEN "special_offline"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"partners_media") THEN "special_online"

  WHEN medium NOT IN ("influencer","friends","marketing") AND (medium="partner" OR REGEXP_CONTAINS(LOWER(source),"partners_staff|partners_loyalty")) THEN "all_promocode_integration"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND LOWER(source) IN ("telegram_bio", "telegram_product") THEN "smm_telegram"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"aura") THEN "aura"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"ya_promo") OR  REGEXP_CONTAINS(LOWER(campaign),"yapromo") THEN "ya_promo"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND (REGEXP_CONTAINS(LOWER(source),"dzen_clients") OR REGEXP_CONTAINS(LOWER(campaign),"dzen_client")) THEN "dzen_client"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"yandex_maps") THEN "yandex_maps"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"pikabu") THEN "smm_pikabu"
  WHEN medium NOT IN ("influencer","partner","friends","marketing")  AND REGEXP_CONTAINS(LOWER(source),"tgbot") THEN "tg_ads"

  ELSE "other"

  END AS segment,
  CAST(IF((REGEXP_CONTAINS(LOWER(campaign),"ретарг|ремарк|retarg|remark|exist")) OR (source IN("yandex","google") AND medium="cpc" AND REGEXP_CONTAINS(LOWER(campaign),"smart|retargering|ретаргетинг|rlsa|рлса|rmk")),1,0) AS INT64) as is_retargeting_campaign,
  CAST(IF(REGEXP_CONTAINS(LOWER(campaign),"wrld|world"),1,0) AS INT64) as is_world_campaign

  FROM cohorts
  ),
channels AS
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
  WHEN segment IN ("smm_vk","smm_instagram", "smm_telegram","smm_instagram_global") AND is_retargeting_campaign!=1 THEN "smm"
  WHEN segment IN ("flowwow_com","invite_friend") AND is_retargeting_campaign!=1 THEN "aso_organic"
  WHEN segment="aura" AND is_retargeting_campaign!=1 THEN "predownloads"
  WHEN segment IN ("dzen_client","ya_promo", "smm_pikabu") AND is_retargeting_campaign!=1 THEN "content_fw_client"
  WHEN segment="tg_ads" AND is_retargeting_campaign!=1 THEN "apps"
  WHEN is_retargeting_campaign=1 AND segment IN ("facebook_ads","google_ads_uac","mytarget","vk_ads","yandex_direct_app") THEN "retargeting"
  ELSE segment
  END AS channel

  FROM segments
  ),
subdivisions AS
  (
  SELECT *,
  CASE
  WHEN segment IN ("yandex_direct_web_non_brand","yandex_direct_web_brand","yandex_direct_web_rsy") OR segment IN ("google_ads_web_non_brand","google_ads_web_brand","google_ads_web_kms") THEN "paid_web"
  WHEN REGEXP_CONTAINS(segment,"seo") OR segment="direct" THEN "organic_web"
  WHEN segment IN ("vk_ads","mytarget","asa_brand","asa_non_brand","yandex_direct_app","google_ads_uac","tiktok_app","appnext","cpa","unity","facebook_ads","aura", "tg_ads") THEN "paid_apps"
  WHEN segment IN ("email_push") THEN "retention"
  WHEN segment IN ("aso_organic","incentive_traffic","sms","flowwow_com","invite_friend") THEN "organic_apps"
  WHEN segment IN ("all_promocode_integration","all_cashback","two_gis","donate","yandex_maps") THEN "partners"
  WHEN segment IN ("smm_vk","smm_telegram","orm","offline_merch","pr","pr_telegram","influencers","smm_instagram", "smm_instagram_global","creatives","regular_online","special_offline","special_online","dzen_client","ya_promo","smm_pikabu") THEN "brand"
  WHEN segment="other" THEN "other"
  END AS subdivision
  FROM channels
  )
 
SELECT * EXCEPT (city_from), city_ru as city_from FROM subdivisions
LEFT JOIN `funnel-flowwow.Analyt_GoryaynovaL.cities_Adjust_en_ru` as aa
ON subdivisions.city_from=aa.city
);