WITH
 owox AS  
   (
    SELECT
    SAFE_CAST(transaction_id AS NUMERIC) as transactionid,
    IFNULL(session_traffic_source_last_non_direct.source,"(direct)") as source,
    IFNULL(session_traffic_source_last_non_direct.medium,"(none)") as medium,
    session_traffic_source_last_non_direct.campaign as campaign
    FROM `funnel-flowwow.BUSINESS_DM.cm_web_ga_4_sessions_attribution` as a
    RIGHT JOIN `funnel-flowwow.BUSINESS_DM.cm_web_ga_4_transactions_attribution` as b ON session_id=attributed_session_id AND a.user_pseudo_id=b.user_pseudo_id
     WHERE b.date>= "2023-06-12" AND transaction_id IN (SELECT CAST(purchase_id AS STRING)
     FROM `funnel-flowwow.CRM_DM_PRTND.crm_com`
     WHERE paid=1)


    QUALIFY ROW_NUMBER() OVER(PARTITION BY transactionid)=1
  ),
adjust AS
  (
  SELECT  
  SAFE_CAST(_purchase_id_ AS NUMERIC) as transactionid,
  _network_name_, _campaign_name_,
  CASE 
  WHEN _network_name_="flowwow_com" AND REGEXP_CONTAINS(_tracker_name_,"google::organic")=false AND REGEXP_CONTAINS(_tracker_name_,"yandex::organic")=false 
  THEN REGEXP_EXTRACT(_tracker_name_,r"source-([0-9a-zA-z]+)") 
  WHEN _network_name_="flowwow_com" AND REGEXP_CONTAINS(_tracker_name_,"google::organic") THEN "google"
  WHEN _network_name_="flowwow_com" AND REGEXP_CONTAINS(_tracker_name_,"yandex::organic") THEN "yandex"
  END as source_from_web,
  CASE 
  WHEN _network_name_="flowwow_com" AND REGEXP_CONTAINS(_tracker_name_,"google::organic")=false AND REGEXP_CONTAINS(_tracker_name_,"yandex::organic")=false
  THEN REGEXP_EXTRACT(_tracker_name_,r"medium-([0-9a-zA-z]+)") 
  WHEN _network_name_="flowwow_com" AND REGEXP_CONTAINS(_tracker_name_,"google::organic|yandex::organic") THEN "organic"
  END as medium_from_web,
  CASE WHEN _network_name_="flowwow_com" THEN REGEXP_EXTRACT(_tracker_name_,r"campaign-(.*)") END as campaign_from_web,
  FROM `funnel-flowwow.ADJUST_RAW.clients_app`
  WHERE SAFE_CAST(_purchase_id_ AS NUMERIC) IS NOT NULL AND _event_name_="s2s_ecommerce_purchase_paid"
  AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2024-04-25")
  QUALIFY ROW_NUMBER() OVER (PARTITION BY transactionid ORDER BY _created_at_)=1
  ),
crm AS
  (
  SELECT  
  product_id,
  purchase_id as transactionid, purchase_timestamp as purchase_date, is_first_purchase,
  product_price as purchase_sum_rub, promo_sum_rub, bonus_company, promocode, city_name, city_from_name, country_to, country_from,
  category_name, subcategory_name, platform, partner_name
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  ),
promo_contains AS
  (
  SELECT LOWER(LEFT(promo,16)) as partner_promo
  FROM `funnel-flowwow.BUSINESS_DM.partners_gs_view`
  WHERE promo IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")=false
  ),
f_program AS
  (
  SELECT type, LOWER(code) as partner_promo
  FROM `funnel-flowwow.BUSINESS_DM.partners_gs_view`
  LEFT JOIN (SELECT code,type FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes` UNION ALL
  SELECT code,type FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes_log`) ON promo=type
  WHERE promo IS NOT NULL AND REGEXP_CONTAINS(position,"f_program")
  ),
promos AS
  (
  SELECT  
  name,
  CASE
  WHEN release_type IN (1,21) THEN "friends"
  WHEN release_type=17 THEN "marketing"
  END as promo_type

  FROM `funnel-flowwow.MYSQL_EXPORT.f_promocodes`
  WHERE release_type IN (1,17,21)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY name)=1
  ),
promo_inf_table AS
  (
  SELECT LOWER(promocode) as promocode_inf
  FROM `funnel-flowwow.BUSINESS_DM.promo_inf_gs_view`
  ),
promo_pr_table AS
  (
  SELECT LOWER(promocode) as promocode_pr
  FROM `funnel-flowwow.BUSINESS_DM.promo_pr_gs_view`
  ),
promo_pr_rus_table AS
  (
  SELECT promocode_pr_rus
  FROM `funnel-flowwow.GOOGLE_SHEETS.CGS_PROMO_PR_RUS_GS_VIEW`
  ),
c AS
  (
  SELECT
  country,
  personal_promocode
  FROM (
  SELECT
    NULL as country,
    LOWER(personal_promocode) as personal_promocode
    FROM  `funnel-flowwow.BUSINESS_DM.influencers_gs_view`
    WHERE personal_promocode IS NOT NULL AND personal_promocode!="none"
  UNION ALL
  SELECT
    country,
    LOWER(personal_promocode) as personal_promocode
    FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`
    WHERE personal_promocode IS NOT NULL AND personal_promocode!="none"
)
QUALIFY ROW_NUMBER() OVER(PARTITION BY personal_promocode)=1  
  ),
cpa_codes_table AS
  (
  SELECT
  DISTINCT code
  FROM (
  SELECT
  CAST(setting_id AS STRING) as setting_id,
  CAST(code AS STRING) as code
  FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes_log`
  UNION DISTINCT
  SELECT
  CAST(setting_id AS STRING) as setting_id,
  CAST(code AS STRING) as code
  FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes`)
  WHERE setting_id IN (SELECT setting_id
  FROM `funnel-flowwow.BUSINESS_DM.cpa_partners_gs_view` where setting_id IS NOT NULL AND TRIM(setting_id) != "")
  AND code IS NOT NULL AND TRIM(code)!=""),
  serm_gs_view AS
  (
  SELECT
  DISTINCT code
  FROM (
  SELECT
  CAST(setting_id AS STRING) as setting_id,
  CAST(code AS STRING) as code
  FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes_log`
  UNION DISTINCT
  SELECT
  CAST(setting_id AS STRING) as setting_id,
  CAST(code AS STRING) as code
  FROM `funnel-flowwow.MYSQL_EXPORT.f_program_loyalty_codes`)
  WHERE CAST(setting_id AS NUMERIC) IN (SELECT CAST(setting_id AS NUMERIC)
  FROM `funnel-flowwow.BUSINESS_DM.serm_gs_view` where setting_id IS NOT NULL) -- TRIM не рабоатет с INT64
  AND code IS NOT NULL AND TRIM(code)!=""  
  )

-- ,full_details AS
  -- (
  SELECT 
  DISTINCT
  product_id,
  transactionid, 1/d.count_purchase_id as normalized_transaction, purchase_date as date, is_first_purchase, purchase_sum_rub, promo_sum_rub, bonus_company, promocode,
  city_name, city_from_name, country_to, country_from, category_name as category, subcategory_name as subcategory, crm.platform
  , coalesce (source_from_web, source, _network_name_ ) as source
  , coalesce (campaign_from_web, campaign, _campaign_name_ ) as campaign
  , coalesce (medium_from_web, medium) as medium 
  , CASE
  WHEN personal_promocode IS NOT NULL OR promocode_inf IS NOT NULL THEN "influencer"
  WHEN s.partner_promo IS NOT NULL OR am.partner_promo IS NOT NULL OR partner_name="TinkoffSuperApp" THEN "partner"
  WHEN promo_type IS NOT NULL AND promocode_pr IS NULL AND promocode_pr_rus IS NULL THEN promo_type
  WHEN promocode_pr IS NOT NULL THEN "promo_pr_international"
  WHEN promocode_pr_rus IS NOT NULL THEN "promo_pr_rus"
  WHEN cpa_codes_table.code IS NOT NULL THEN "cpa_partners"
  WHEN serm_gs_view.code IS NOT NULL THEN "orm"
  ELSE NULL
  END as if_promo,
  _network_name_, _campaign_name_, c.country as inf_country
  FROM crm
  LEFT JOIN owox USING (transactionid)
  LEFT JOIN adjust USING (transactionid)
  LEFT JOIN c
  ON LOWER(crm.promocode)=c.personal_promocode
  LEFT JOIN (SELECT
  purchase_id, COUNT(*) as count_purchase_id
  FROM `funnel-flowwow.ORDER_SKU_DM_PRTND.order_sku_com`
  WHERE paid=1 AND product_type="main"
  GROUP BY purchase_id) as d
  ON crm.transactionid=d.purchase_id
  LEFT JOIN promo_contains s ON (STARTS_WITH(LOWER(promocode),LOWER(s.partner_promo)))
  LEFT JOIN f_program am ON LOWER(promocode) = LOWER(am.partner_promo)
  LEFT JOIN promos ON LOWER(promocode)=LOWER(name)
  LEFT JOIN promo_inf_table ON LOWER(promocode)=LOWER(promocode_inf)
  LEFT JOIN promo_pr_table ON LOWER(promocode)=LOWER(promocode_pr)
  LEFT JOIN promo_pr_rus_table ON promocode=promocode_pr_rus
  LEFT JOIN cpa_codes_table ON LOWER(promocode)= LOWER(cpa_codes_table.code)
  LEFT JOIN serm_gs_view ON LOWER(promocode)= LOWER(serm_gs_view.code)
  WHERE LOWER(coalesce (source_from_web, source, _network_name_ )) IN ('reliz_unity','reliz_smadex','reliz_hybrid','adchampagne_bigo','reliz_xiaomi','adchampagne_unity','adchampagne.com','adchampagne_ironsource','reliz_yappy','reliz_vfineads','reliz_ironsource','reliz_bigo')