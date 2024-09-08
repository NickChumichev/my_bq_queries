WITH
orders AS
  (
  SELECT  
  CAST(user_id AS STRING) as user_id,
  purchase_id,
  platform,
  DATE(purchase_date) as date,
  purchase_sum_rub as revenue,
  is_first_purchase,
  IF(promo_sum_rub>0,'1','0') as promo_used,
  IF(bonuses_sum_rub>0,'1','0') as bonus_used,
  shop_id
  FROM `funnel-flowwow.CRM_DM_PRTND.crm_com` 
  WHERE paid=1 AND DATE(purchase_date) BETWEEN '2023-09-01' AND '2023-11-30'
  ),
reviews AS
  (
  SELECT
  order_id as purchase_id,
  CASE
  WHEN MIN(min_value)=0 THEN 'dont press stars'
  WHEN MIN(min_value) BETWEEN 1 AND 3 THEN 'bad review'
  WHEN MIN(min_value)>=4 THEN 'good review'
  END as review
  FROM `funnel-flowwow.MYSQL_EXPORT.f_review` 
  WHERE type='buyer-flowwow' 
  GROUP BY 1
  ),
shops_rating AS
  (
  SELECT 
  shop_id, DATE(date) as date, IF(AVG(rating_avg)>=4.85,'high_rating','not_high_rating') as shop_rating
  FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_stats` 
  GROUP BY 1,2
  ),
users AS 
  (
  SELECT
  CAST(id AS STRING) as user_id, DATE(create_at) as date_of_registration
  FROM `funnel-flowwow.MYSQL_EXPORT.f_user` 
  WHERE create_at BETWEEN '2023-09-01' AND '2023-11-30'
  ),
app_users AS
  (
  SELECT
  user_id, web_to_app, if_app
  FROM (
  SELECT  
  _user_id_ as user_id, IF(_network_name_='flowwow_com',1,0) as web_to_app, 1 as if_app
  FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-09-01") AND TIMESTAMP("2023-11-30") AND _event_name_='complete_registration' 
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _user_id_ ORDER BY _created_at_)=1
  ) 
  GROUP BY 1,2,3
  ),
users_information AS
  (
  SELECT
  user_id, 
    CASE
    WHEN if_app=1 AND web_to_app=1 THEN "web_to_app"
    WHEN if_app=1 AND web_to_app=0 THEN "app"
    ELSE "web"
    END as 
  platform_of_registration,
  DATE_TRUNC(date_of_registration,MONTH) as month_of_registration,
  IFNULL(CAST(DATE_TRUNC(orders.date, MONTH) AS STRING),'havent purchased yet') as month_of_first_purchase,
  IF(DATE_TRUNC(orders.date, MONTH) IS NULL,'havent purchased yet', shop_rating) as first_shop_rating,
  IF(DATE_TRUNC(orders.date, MONTH) IS NULL,'havent purchased yet', IFNULL(review,'didnt leave')) as first_purchase_review,
  IF(DATE_TRUNC(orders.date, MONTH) IS NULL,'havent purchased yet', promo_used) as first_purchase_promo_used,
  IF(DATE_TRUNC(orders.date, MONTH) IS NULL,'havent purchased yet', bonus_used) as first_purchase_bonus_used,
  revenue as first_revenue,
  purchase_id
  FROM users
  LEFT JOIN app_users USING(user_id)
  LEFT JOIN (SELECT * FROM orders WHERE is_first_purchase=1) as orders USING(user_id)
  LEFT JOIN shops_rating USING(shop_id,date)
  LEFT JOIN reviews USING(purchase_id)
  ),
all_table AS
  (
  SELECT user_id, platform_of_registration,month_of_registration,month_of_first_purchase,first_shop_rating,first_purchase_review,first_purchase_promo_used,first_purchase_bonus_used,
  orders.purchase_id,
  DATE_TRUNC(orders.date, MONTH) as month_of_purchase,
  revenue,
  FROM users_information
  LEFT JOIN (SELECT * FROM orders WHERE is_first_purchase!=1) as orders USING(user_id)
  ),
users_grouped AS
  (
  SELECT
  platform_of_registration,
  month_of_registration,
  month_of_first_purchase,
  first_shop_rating,
  first_purchase_review,
  first_purchase_promo_used,
  first_purchase_bonus_used,
  COUNT(DISTINCT user_id) as users_registered,
  SUM(first_revenue) as first_revenue,
  COUNT(DISTINCT purchase_id) as first_purchases
  FROM users_information
  GROUP BY 1,2,3,4,5,6,7
  ORDER BY 1,2,3,4,5,6,7
  )

SELECT
platform_of_registration,month_of_registration,month_of_first_purchase,first_shop_rating,first_purchase_review,first_purchase_promo_used,first_purchase_bonus_used, 
SUM(users_registered) OVER(PARTITION BY platform_of_registration,month_of_registration) as users_registered_that_platform_and_month, 
users_registered as users_registered_who_made_transaction,
first_purchases,
first_revenue,
* EXCEPT(platform_of_registration,month_of_registration,month_of_first_purchase,first_shop_rating,first_purchase_review,first_purchase_promo_used,first_purchase_bonus_used, 
users_registered, first_revenue)

FROM all_table
PIVOT(
COUNT(DISTINCT user_id) as returned_users,
COUNT(DISTINCT purchase_id) as repeat_tranasctions,
SUM(revenue) as repeat_revenue
FOR month_of_purchase IN ('2022-01-01',
'2022-02-01',
'2022-03-01',
'2022-04-01',
'2022-05-01',
'2022-06-01',
'2022-07-01',
'2022-08-01',
'2022-09-01',
'2022-10-01',
'2022-11-01',
'2022-12-01',
'2023-01-01',
'2023-02-01',
'2023-03-01',
'2023-04-01',
'2023-05-01',
'2023-06-01',
'2023-07-01',
'2023-08-01',
'2023-09-01',
'2023-10-01',
'2023-11-01')
)
FULL JOIN users_grouped USING (platform_of_registration,month_of_registration,month_of_first_purchase,first_shop_rating,first_purchase_review,first_purchase_promo_used,first_purchase_bonus_used)

