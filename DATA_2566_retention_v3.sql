WITH a AS ( -- получить первые покупки с TinkoffSuperApp
  SELECT
    user_id, 
    DATE_TRUNC(CAST(purchase_date AS DATE),MONTH) as cohort_month, -- месяц, когда была совершена первая покупка
    purchase_id as first_purchase_id,
    partner_name
  FROM `funnel-flowwow.CRM_DM_PRTND.crm_com`
  WHERE 1=1
  AND paid=1 
  AND is_first_purchase=1 
  AND partner_name = 'TinkoffSuperApp'
  AND DATE(purchase_date) BETWEEN "2023-02-01" AND "2023-09-30"
  )
, b AS ( --повторные покупки, где partner_name IOSAPP|ANDROIDAPP 
  SELECT
    user_id,
    partner_name,
    DATE_TRUNC(CAST(purchase_date AS DATE),MONTH) AS repeat_month,-- месяц, когда была совершена повторная покупка
    purchase_id AS repeat_purchase_id 
  FROM `funnel-flowwow.CRM_DM_PRTND.crm_com`
  WHERE 1=1
  AND paid=1 
  AND is_first_purchase = 0 
  AND user_id IN (SELECT user_id FROM a) 
  AND DATE(purchase_date) BETWEEN "2023-02-01" AND "2023-09-30"
  AND REGEXP_CONTAINS(partner_name,r'IOSAPP|ANDROIDAPP|\s') 
  )
, c AS (  -- посчитать дельту по месяцам
  SELECT 
  a.user_id,
  EXTRACT(MONTH FROM a.cohort_month) AS cohort_month,
  EXTRACT(MONTH FROM b.repeat_month) AS repeat_month,
  a.first_purchase_id,
  b.repeat_purchase_id,
  a.partner_name,
  EXTRACT(MONTH FROM b.repeat_month) - EXTRACT(MONTH FROM a.cohort_month) AS delta_month 
  FROM a LEFT JOIN b ON a.user_id = b.user_id
  )
,d AS (  -- группировка первых,повторных покупок и покупателей 
  SELECT
    cohort_month,
    CASE WHEN repeat_month IS NULL THEN 0 
    ELSE repeat_month
    END AS repeat_month, 
    CASE WHEN delta_month IS NULL THEN 0 -- объединил дельту null и 0,т.к по дельте null user_id, у которых нет повторных покупок
    ELSE delta_month
    END AS delta_month,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT first_purchase_id) AS first_purchases,
    COUNT(DISTINCT repeat_purchase_id) AS repeat_purchases,  
  FROM c
  GROUP BY 1,2,3
)
  SELECT -- посчитать retention_rate
    cohort_month, -- месяц, когда была совершена первая покупка
    repeat_month,-- месяц, когда была совершена повторная покупка
    delta_month, -- разница между месяцем первой покупки и месяцрм повторной покупки
    users,
    first_purchases,
    repeat_purchases,
    users / FIRST_VALUE(users) OVER (PARTITION BY cohort_month ORDER BY repeat_month) AS retention_rate
  FROM d
  GROUP BY 1,2,3,4,5,6




--соединить две таблицы, DATE_DIFF(MONTH - delta)
--GROUP BY cohort_motnh
--user_id|cohort_month|first_purchase|repeat_purchase_id