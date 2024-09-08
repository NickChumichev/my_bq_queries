--CRM-2-когорты
WITH
installs_registrations AS
  (
SELECT -- найти месяц регистрации и месяц  установки?
  user_pseudo_id as _adid_,
  DATE_TRUNC(MIN(date),MONTH) as month_of_install,
  DATE_TRUNC(MIN(IF(date_user_registration=date,date,NULL)),MONTH) as month_of_registration,
  MIN(user_id) as user_id,
  MIN(date) as user_time,
FROM (
SELECT -- получить дату регистрации пользователя и дату его событий
  s.user_pseudo_id,
  PARSE_DATE("%Y%m%d", s.event_date) as date,
  s.user_id,
  DATE(r.create_at) as date_user_registration
FROM `firebase-flowwow.analytics_150948805.events_20231001` s
FULL JOIN `funnel-flowwow.MYSQL_EXPORT.f_user` r ON CAST(r.id AS STRING)=s.user_id
WHERE stream_id="3464829917" -- веб поток
)
GROUP BY 1
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY user_time)=1
  )
,transactions AS
  (
SELECT  -- найти покупки
  purchase_id, 
  purchase_sum_rub, --без доставки?
  DATE_TRUNC(DATE(purchase_date), MONTH) as month_of_purchase, 
  CAST(user_id AS STRING) as user_id
FROM `funnel-flowwow.CRM_DM_PRTND.crm_com` 
WHERE TIMESTAMP_TRUNC(purchase_date, MONTH) >= TIMESTAMP("2022-01-01") 
AND paid=1
  )
,traffic AS
  (
SELECT -- посчитать количество установок в месяц
  month_of_install as month, 
  COUNT(DISTINCT _adid_) as user_traffic,
FROM installs_registrations
  GROUP BY 1
  )
,registrations AS
  (
SELECT -- посчитать количество регистраций в месяц
  month_of_registration as month, 
  COUNT(DISTINCT _adid_) as user_registrations,
FROM installs_registrations
  GROUP BY 1
  )
,prep AS -- соединить месяц установки с месяцем покупки
  (
  SELECT 
  month_of_install as month,
  month_of_purchase,
  user_id,
  FROM installs_registrations
  LEFT JOIN transactions USING(user_id)
  )
SELECT -- посчитать количество покупателей по месяцам
  month, 
  user_traffic, 
  user_registrations, 
  * EXCEPT(month, user_traffic, user_registrations) 
FROM prep
PIVOT (
COUNT(DISTINCT user_id) as users_who_purchased
FOR month_of_purchase IN ('2022-01-01','2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01','2022-07-01','2022-08-01','2022-09-01','2022-10-01','2022-11-01','2022-12-01','2023-01-01','2023-02-01','2023-03-01','2023-04-01','2023-05-01','2023-06-01','2023-07-01','2023-08-01','2023-09-01','2023-10-01','2023-11-01','2023-12-01')
)
RIGHT JOIN traffic USING(month)
RIGHT JOIN registrations USING(month)
WHERE month IS NOT NULL
ORDER BY month