create or replace  table 
funnel-flowwow.Analyt_KosarevS.A4_cm_data_by_campaigns
partition by date 
 as(

SELECT
 date, subdivision, channel, segment, source, medium, campaign, is_retargeting_campaign, is_world_campaign, attributed_by, city_from, city_to, country_from, country_to, region, category, subcategory, platform,

 IFNULL(ads_cost,0) as ads_cost,	
 IFNULL(not_ads_cost,0) as not_ads_cost,	
IFNULL(service_cost,0) as service_cost, 
IFNULL(clicks,0) as clicks,
IFNULL(impressions,0) as impressions,
IFNULL(purchases,0) as purchases,	
IFNULL(first_purchases,0) as first_purchases,	
IFNULL(transactions,0) as transactions,	
IFNULL(first_transactions,0) as first_transactions,	
IFNULL(revenue,0) as revenue, 
IFNULL(revenue_first_transactions,0) as revenue_first_transactions,	
IFNULL(promo_cost,0) as promo_cost,	
IFNULL(bonus_company,0) as bonus_company,	
IFNULL(distinct_views,0) as traffic,	
IFNULL(distinct_first_views,0) as first_traffic,

/*
IFNULL(installs,0) as installs,
IFNULL(first_transactions_from_install_7,0) as first_transactions_from_install_7, 
IFNULL(first_transactions_from_install_14,0) as first_transactions_from_install_14, 
IFNULL(first_transactions_from_install_30,0) as first_transactions_from_install_30, 
IFNULL(first_transactions_from_install_90,0) as first_transactions_from_install_90, 
IFNULL(first_transactions_from_install_365,0) as first_transactions_from_install_365, 
IFNULL(first_transactions_from_install_365_more,0) as first_transactions_from_install_365_more,
IFNULL(revenue_first_transactions_from_install_7,0) as revenue_first_transactions_from_install_7, 
IFNULL(revenue_first_transactions_from_install_14,0) as revenue_first_transactions_from_install_14,
IFNULL(revenue_first_transactions_from_install_30,0) as revenue_first_transactions_from_install_30, 
IFNULL(revenue_first_transactions_from_install_90,0) as revenue_first_transactions_from_install_90, 
IFNULL(revenue_first_transactions_from_install_365,0) as revenue_first_transactions_from_install_365, 
IFNULL(revenue_first_transactions_from_install_365_more,0) as revenue_first_transactions_from_install_365_more,
IFNULL(repeat_transactions_from_install_7,0) as repeat_transactions_from_install_7, 
IFNULL(repeat_transactions_from_install_14,0) as repeat_transactions_from_install_14, 
IFNULL(repeat_transactions_from_install_30,0) as repeat_transactions_from_install_30,
IFNULL(repeat_transactions_from_install_90,0) as repeat_transactions_from_install_90, 
IFNULL(repeat_transactions_from_install_365,0) as repeat_transactions_from_install_365, 
IFNULL(repeat_transactions_from_install_365_more,0) as repeat_transactions_from_install_365_more,
IFNULL(revenue_repeat_transactions_from_install_7,0) as revenue_repeat_transactions_from_install_7,
IFNULL(revenue_repeat_transactions_from_install_14,0) as revenue_repeat_transactions_from_install_14,
IFNULL(revenue_repeat_transactions_from_install_30,0) as revenue_repeat_transactions_from_install_30,
IFNULL(revenue_repeat_transactions_from_install_90,0) as revenue_repeat_transactions_from_install_90, 
IFNULL(revenue_repeat_transactions_from_install_365,0) as revenue_repeat_transactions_from_install_365,
IFNULL(revenue_repeat_transactions_from_install_365_more,0) as revenue_repeat_transactions_from_install_365_more,
*/
 
IFNULL(purchases_from_first_30,0) as purchases_from_first_30,
IFNULL(purchases_from_first_90,0) as purchases_from_first_90,
IFNULL(purchases_from_first_365,0) as purchases_from_first_365,
IFNULL(purchases_from_first_2_years,0) as purchases_from_first_2_years,
IFNULL(purchases_from_first_3_years,0) as purchases_from_first_3_years,
IFNULL(purchases_from_first_4_years,0) as purchases_from_first_4_years,
IFNULL(purchases_from_first_5_years,0) as purchases_from_first_5_years,
IFNULL(purchases_from_first_5_years_more,0) as purchases_from_first_5_years_more,
IFNULL(transactions_from_first_30,0) as transactions_from_first_30,   
IFNULL(transactions_from_first_90,0) as transactions_from_first_90,    
IFNULL(transactions_from_first_365,0) as transactions_from_first_365,   
IFNULL(transactions_from_first_2_years,0) as transactions_from_first_2_years,
IFNULL(transactions_from_first_3_years,0) as transactions_from_first_3_years,   
IFNULL(transactions_from_first_4_years,0) as transactions_from_first_4_years,    
IFNULL(transactions_from_first_5_years,0) as transactions_from_first_5_years,    
IFNULL(transactions_from_first_5_years_more,0) as transactions_from_first_5_years_more,          
IFNULL(revenue_from_first_30,0) as revenue_from_first_30,   
IFNULL(revenue_from_first_90,0) as revenue_from_first_90,  
IFNULL(revenue_from_first_365,0) as revenue_from_first_365,    
IFNULL(revenue_from_first_2_years,0) as revenue_from_first_2_years, 
IFNULL(revenue_from_first_3_years,0) as revenue_from_first_3_years, 
IFNULL(revenue_from_first_4_years,0) as revenue_from_first_4_years, 
IFNULL(revenue_from_first_5_years,0) as revenue_from_first_5_years, 
IFNULL(revenue_from_first_5_years_more,0) as revenue_from_first_5_years_more, 
IFNULL(flowers_purchases_from_first_30,0) as flowers_purchases_from_first_30,    
IFNULL(flowers_purchases_from_first_90,0) as flowers_purchases_from_first_90,   
IFNULL(flowers_purchases_from_first_365,0) as flowers_purchases_from_first_365,    
IFNULL(flowers_purchases_from_first_365_more,0) as flowers_purchases_from_first_365_more,   
IFNULL(flowers_revenue_from_first_30,0) as flowers_revenue_from_first_30,  
IFNULL(flowers_revenue_from_first_90,0) as flowers_revenue_from_first_90,   
IFNULL(flowers_revenue_from_first_365,0) as flowers_revenue_from_first_365,     
IFNULL(flowers_revenue_from_first_365_more,0) as flowers_revenue_from_first_365_more,   
IFNULL(bakery_purchases_from_first_30,0) as bakery_purchases_from_first_30,    
IFNULL(bakery_purchases_from_first_90,0) as bakery_purchases_from_first_90,    
IFNULL(bakery_purchases_from_first_365,0) as bakery_purchases_from_first_365,   
IFNULL(bakery_purchases_from_first_365_more,0) as bakery_purchases_from_first_365_more,    
IFNULL(bakery_revenue_from_first_30,0) as bakery_revenue_from_first_30,    
IFNULL(bakery_revenue_from_first_90,0) as bakery_revenue_from_first_90,    
IFNULL(bakery_revenue_from_first_365,0) as bakery_revenue_from_first_365,   
IFNULL(bakery_revenue_from_first_365_more,0) as bakery_revenue_from_first_365_more,    
IFNULL(plant_flowers_purchases_from_first_30,0) as plant_flowers_purchases_from_first_30,  
IFNULL(plant_flowers_purchases_from_first_90,0) as plant_flowers_purchases_from_first_90,   
IFNULL(plant_flowers_purchases_from_first_365,0) as plant_flowers_purchases_from_first_365,    
IFNULL(plant_flowers_purchases_from_first_365_more,0) as plant_flowers_purchases_from_first_365_more,   
 IFNULL(plant_flowers_revenue_from_first_30,0) as plant_flowers_revenue_from_first_30,   
 IFNULL(plant_flowers_revenue_from_first_90,0) as plant_flowers_revenue_from_first_90,   
 IFNULL(plant_flowers_revenue_from_first_365,0) as plant_flowers_revenue_from_first_365,    
 IFNULL(plant_flowers_revenue_from_first_365_more,0) as plant_flowers_revenue_from_first_365_more,


 --IFNULL(segment_revenue_prediction,0) as segment_revenue_prediction,
 --IFNULL(ads_cost_by_traffic,0) as ads_cost_by_traffic,

/* 
 IFNULL(ads_cost_year_ago,0) as ads_cost_year_ago,
 IFNULL(not_ads_cost_year_ago,0) as  not_ads_cost_year_ago,
 IFNULL(service_cost_year_ago,0) as service_cost_year_ago,
 IFNULL(transactions_year_ago,0) as transactions_year_ago,
 IFNULL(first_transactions_year_ago,0) as first_transactions_year_ago,
 IFNULL(revenue_year_ago,	0) as revenue_year_ago,
 IFNULL(revenue_first_transactions_year_ago,0) as revenue_first_transactions_year_ago,
 IFNULL(promo_cost_year_ago, 0) as 	promo_cost_year_ago,
 IFNULL(bonus_company_year_ago,0) as bonus_company_year_ago,
 IFNULL(traffic_year_ago, 0) as 	traffic_year_ago,
 IFNULL(first_traffic_year_ago,0) as first_traffic_year_ago,
 IFNULL(ads_cost_by_traffic_year_ago,0) as ads_cost_by_traffic_year_ago,
 IFNULL(ads_cost_30_days_ago, 0) as ads_cost_30_days_ago,
 IFNULL(not_ads_cost_30_days_ago, 0) as not_ads_cost_30_days_ago,
 IFNULL(service_cost_30_days_ago,0) as service_cost_30_days_ago,
 IFNULL(transactions_30_days_ago, 0) as transactions_30_days_ago,
 IFNULL(first_transactions_30_days_ago,0) as first_transactions_30_days_ago,
 IFNULL(revenue_30_days_ago,	0) as revenue_30_days_ago,
 IFNULL(revenue_first_transactions_30_days_ago,	0) as revenue_first_transactions_30_days_ago,
 IFNULL(promo_cost_30_days_ago,0) as promo_cost_30_days_ago,
 IFNULL(bonus_company_30_days_ago,0) as bonus_company_30_days_ago,
 IFNULL(traffic_30_days_ago, 0) as 	traffic_30_days_ago,
 IFNULL(first_traffic_30_days_ago,0) as first_traffic_30_days_ago,
 IFNULL(ads_cost_by_traffic_30_days_ago,0) as ads_cost_by_traffic_30_days_ago
 */
 

 FROM `funnel-flowwow.Analyt_KosarevS.A1_cm_date_subdivision_etc_cost` 

 FULL JOIN `funnel-flowwow.Analyt_KosarevS.A2_cm_date_subdivision_etc_transactions` 
 USING (date, subdivision, channel, segment, source, medium, campaign, is_retargeting_campaign, is_world_campaign, attributed_by, city_from, city_to, country_from, country_to, region, category, subcategory, platform)

 FULL JOIN (SELECT *, CAST(null AS STRING) as city_to, CAST(null AS STRING) as country_to,  CAST(null AS STRING) as region, CAST(null AS STRING) as category, CAST(null AS STRING) as subcategory, "adjust" as attributed_by FROM `funnel-flowwow.Analyt_KosarevS.A0_cohort_installs`)
 USING(date, subdivision, channel, segment, source, medium, campaign, is_retargeting_campaign, is_world_campaign, attributed_by, city_from, city_to, country_from, country_to, region, category, subcategory, platform)

 FULL JOIN `funnel-flowwow.Analyt_KosarevS.A3_cm_product_view_segmentation`
 USING (date, subdivision, channel, segment, source, medium, campaign, is_retargeting_campaign, is_world_campaign, attributed_by, city_from, city_to, country_from, country_to, region, category, subcategory, platform)

FULL JOIN `funnel-flowwow.Analyt_KosarevS.A01_date_cohorts_from_first_purchase` 
USING (date, subdivision, channel, segment, source, medium, campaign, is_retargeting_campaign, is_world_campaign, attributed_by, city_from, city_to, country_from, country_to, region, category, subcategory, platform)

)
 ; 