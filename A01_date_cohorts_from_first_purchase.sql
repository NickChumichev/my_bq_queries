create or replace  table
funnel-flowwow.Analyt_KosarevS.A01_date_cohorts_from_first_purchase
as
(

SELECT 
date_of_first_purchase as date,
subdivision,  channel,  segment,  source, medium, campaign, is_retargeting_campaign, is_world_campaign, city_to, city_from, country_from, country_to, category, subcategory, platform, region, attributed_by,
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29,1/(count_purchase_id),0)) as purchases_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89,1/(count_purchase_id),0)) as purchases_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364,1/(count_purchase_id),0)) as purchases_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*2-1,1/(count_purchase_id),0)) as purchases_from_first_2_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*3-1,1/(count_purchase_id),0)) as purchases_from_first_3_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*4-1,1/(count_purchase_id),0)) as purchases_from_first_4_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*5-1,1/(count_purchase_id),0)) as purchases_from_first_5_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) > 365*5,1/(count_purchase_id),0)) as purchases_from_first_5_years_more,
 
 
 
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29,1/(count_purchase_id_repeat*count_purchase_id),0)) as transactions_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89,1/(count_purchase_id_repeat*count_purchase_id),0)) as transactions_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364,1/(count_purchase_id_repeat*count_purchase_id),0)) as transactions_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*2-1,1/(count_purchase_id_repeat*count_purchase_id),0)) as transactions_from_first_2_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*3-1,1/(count_purchase_id_repeat*count_purchase_id),0)) as transactions_from_first_3_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*4-1,1/(count_purchase_id_repeat*count_purchase_id),0)) as transactions_from_first_4_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*5-1,1/(count_purchase_id_repeat*count_purchase_id),0)) as transactions_from_first_5_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) > 365*5,1/(count_purchase_id_repeat*count_purchase_id),0)) as transactions_from_first_5_years_more,
 
 
 
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29,revenue_repeat/count_purchase_id,0)) as revenue_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89,revenue_repeat/count_purchase_id,0)) as revenue_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364,revenue_repeat/count_purchase_id,0)) as revenue_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*2-1,revenue_repeat/count_purchase_id,0)) as revenue_from_first_2_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*3-1,revenue_repeat/count_purchase_id,0)) as revenue_from_first_3_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*4-1,revenue_repeat/count_purchase_id,0)) as revenue_from_first_4_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 365*5-1,revenue_repeat/count_purchase_id,0)) as revenue_from_first_5_years,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) > 365*5,revenue_repeat/count_purchase_id,0)) as revenue_from_first_5_years_more,
 
 
 
 
 
 
-----------------
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29 AND category_repeat="Цветы и подарки",1/(count_purchase_id),0)) as flowers_purchases_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89 AND category_repeat="Цветы и подарки",1/(count_purchase_id),0)) as flowers_purchases_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364 AND category_repeat="Цветы и подарки",1/(count_purchase_id),0)) as flowers_purchases_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) >364 AND category_repeat="Цветы и подарки",1/(count_purchase_id),0)) as flowers_purchases_from_first_365_more,
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29 AND category_repeat="Цветы и подарки",revenue_repeat/count_purchase_id,0)) as flowers_revenue_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89 AND category_repeat="Цветы и подарки",revenue_repeat/count_purchase_id,0)) as flowers_revenue_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364 AND category_repeat="Цветы и подарки",revenue_repeat/count_purchase_id,0)) as flowers_revenue_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY)>364 AND category_repeat="Цветы и подарки",revenue_repeat/count_purchase_id,0)) as flowers_revenue_from_first_365_more,
 
--------------------------------
 
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29 AND category_repeat="Кондитерские и пекарни",1/(count_purchase_id),0)) as bakery_purchases_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89 AND category_repeat="Кондитерские и пекарни",1/(count_purchase_id),0)) as bakery_purchases_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364 AND category_repeat="Кондитерские и пекарни",1/(count_purchase_id),0)) as bakery_purchases_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) >364 AND category_repeat="Кондитерские и пекарни",1/(count_purchase_id),0)) as bakery_purchases_from_first_365_more,
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29 AND category_repeat="Кондитерские и пекарни",revenue_repeat/count_purchase_id,0)) as bakery_revenue_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89 AND category_repeat="Кондитерские и пекарни",revenue_repeat/count_purchase_id,0)) as bakery_revenue_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364 AND category_repeat="Кондитерские и пекарни",revenue_repeat/count_purchase_id,0)) as bakery_revenue_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY)>364 AND category_repeat="Кондитерские и пекарни",revenue_repeat/count_purchase_id,0)) as bakery_revenue_from_first_365_more,
 
 
-----------------
 
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29 AND category_repeat="Живые растения",1/(count_purchase_id),0)) as plant_flowers_purchases_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89 AND category_repeat="Живые растения",1/(count_purchase_id),0)) as plant_flowers_purchases_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364 AND category_repeat="Живые растения",1/(count_purchase_id),0)) as plant_flowers_purchases_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) >364 AND category_repeat="Живые растения",1/(count_purchase_id),0)) as plant_flowers_purchases_from_first_365_more,
 
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29 AND category_repeat="Живые растения",revenue_repeat/count_purchase_id,0)) as plant_flowers_revenue_from_first_30,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 89 AND category_repeat="Живые растения",revenue_repeat/count_purchase_id,0)) as plant_flowers_revenue_from_first_90,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 364 AND category_repeat="Живые растения",revenue_repeat/count_purchase_id,0)) as plant_flowers_revenue_from_first_365,
SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY)>364 AND category_repeat="Живые растения",revenue_repeat/count_purchase_id,0)) as plant_flowers_revenue_from_first_365_more,
 
 
FROM `funnel-flowwow.Analyt_KosarevS.A0_cohorts_from_first_purchase` 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
 
 
 
