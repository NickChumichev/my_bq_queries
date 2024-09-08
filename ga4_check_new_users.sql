WITH
pages AS
  (
  SELECT
  CAST(event_date AS DATE FORMAT "YYYYMMDD") as date,
  user_pseudo_id, 
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id") as session_id,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location" AND event_name="page_view") as page,
  TIMESTAMP_MICROS(event_timestamp) as event_timestamp
  FROM `firebase-flowwow.analytics_150948805.events_*` 
  WHERE stream_id = "3464829917" 
  AND _TABLE_SUFFIX >= "30201"
  GROUP BY 1,2,3,4,5
  ),
first_session AS 
  (
  SELECT
  MIN(TIMESTAMP_MICROS(event_timestamp)) as event_timestamp,
  user_pseudo_id,
  MAX(IF(event_name IN ('first_visit', 'first_open'),1,0)) AS is_first_session
  FROM `firebase-flowwow.analytics_150948805.events_*`
  WHERE 1=1
  AND stream_id = "3464829917" 
  AND _TABLE_SUFFIX >= "30201"
  AND user_pseudo_id = '1401580159.1617652539' 
  AND event_name IN ('first_visit', 'first_open')
  GROUP BY 2
  ),
ts AS
  (
  SELECT
  s.date,
  s.user_pseudo_id,
  s.session_id,
  n.is_first_session,
  TIMESTAMP_SECONDS(s.session_id) as timestamp_start,
  MAX(s.event_timestamp) as timestamp_finish
  FROM pages s LEFT JOIN first_session n ON s.event_timestamp=n.event_timestamp AND s.user_pseudo_id=n.user_pseudo_id
  GROUP BY 1,2,3,4,5
  ),
landing AS
  (
  SELECT
  date,
  user_pseudo_id,
  session_id,
  landing_page,
  FROM (
  SELECT date,
  user_pseudo_id,
  session_id,
  FIRST_VALUE(page IGNORE NULLS) OVER(PARTITION BY user_pseudo_id,session_id ORDER BY event_timestamp) as landing_page
  FROM pages
  ) WHERE landing_page IS NOT NULL
  GROUP BY 1,2,3,4
  )
SELECT 
ts.date,
user_pseudo_id,
session_id,
STRUCT (is_first_session,
timestamp_start,
IF(timestamp_finish<timestamp_start,NULL,timestamp_finish) as timestamp_finish,
landing_page) as session_information
FROM ts
LEFT JOIN landing USING(user_pseudo_id,session_id,date)
WHERE  user_pseudo_id = '1401580159.1617652539'