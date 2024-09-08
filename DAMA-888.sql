WITH a AS (
SELECT DISTINCT
  date_of_first_purchase,date_of_repeat_purchase,user_id,
subdivision,  channel,  segment,  source, medium, campaign, is_retargeting_campaign, is_world_campaign, city_to, city_from, country_from, country_to, category, subcategory, platform, region, attributed_by,count_purchase_id
FROM `funnel-flowwow.Analyt_KosarevS.A0_cohorts_from_first_purchase`
WHERE 1=1
-- AND user_id = 6367011
AND date_of_first_purchase BETWEEN '2024-01-01' AND '2024-06-30'
 AND segment = 'yandex_direct_app'
AND date_of_repeat_purchase IS NOT NULL
-- GROUP BY 1,2,3
)
, b AS (
SELECT
  COUNT(DISTINCT user_id) AS user_id,
  SUM(count_purchase_id) AS count_purchase_id,  
  SUM(1/(count_purchase_id)) AS count_purchase_id_1
FROM a
-- WHERE DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29
WHERE DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) =1
)
, c AS (
SELECT DISTINCT
  date_of_first_purchase,date_of_repeat_purchase,user_id,
subdivision,  channel,  segment,  source, medium, campaign, is_retargeting_campaign, is_world_campaign, city_to, city_from, country_from, country_to, category, subcategory, platform, region, attributed_by,count_purchase_id
  -- SUM(IF(DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29,1/(count_purchase_id),0)) AS purchases_from_first_30,
FROM `funnel-flowwow.BUSINESS_DM.cm_A0_cohorts_from_first_purchase`
WHERE 1=1
-- AND user_id = 986524
AND date_of_first_purchase BETWEEN '2024-01-01' AND '2024-06-30'
AND segment = 'yandex_direct_app'
AND date_of_repeat_purchase IS NOT NULL
-- GROUP BY 1,2,3
)
, d AS (
SELECT  
  COUNT(DISTINCT user_id) AS user_id,
  SUM(count_purchase_id) AS count_purchase_id,
  SUM(1/(count_purchase_id)) AS count_purchase_id_1
FROM c
--WHERE DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) BETWEEN 0 AND 29
WHERE DATE_DIFF(date_of_repeat_purchase, date_of_first_purchase, DAY) =1
)
SELECT
  b.count_purchase_id,
  d.count_purchase_id,
  b.user_id,
  d.user_id,
  b.count_purchase_id_1,
  d.count_purchase_id_1
FROM d FULL JOIN b USING(count_purchase_id)