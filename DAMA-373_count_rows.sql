WITH a AS (
SELECT *, COUNT(user_id) OVER (ORDER BY user_id) AS number
FROM `funnel-flowwow.Analyt_ChumichevN.DAMA-273_check_user_id` 
GROUP BY user_id,last_event_city,platform_of_registration	
)
SELECT
  *
FROM a
WHERE number BETWEEN 400001 AND 550000
 