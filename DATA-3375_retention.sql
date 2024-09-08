WITH a AS ( -- получить первые покупки с программы лояльности промокодов
  SELECT DISTINCT 
    user_id, 
    DATE_TRUNC(CAST(purchase_timestamp AS DATE),MONTH) as cohort_month, -- месяц, когда была совершена первая покупка
    purchase_id as first_purchase_id
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  WHERE purchase_id IN (
SELECT
  purchase_id
FROM `funnel-flowwow.CRM_DM_PRTND.crm_com` 
WHERE 1=1 
  AND promocode_release_type = 54 
  AND DATE(purchase_date) BETWEEN '2023-01-01' AND '2023-12-31' 
  AND purchase_status = 'Завершён'
  AND is_first_purchase = 1
  AND paid = 1
)
  )
, b AS ( --повторные покупки тех, у кого первая покупка была с программы лояльности промокодов
  SELECT DISTINCT
    user_id,
    DATE_TRUNC(CAST(purchase_timestamp AS DATE),MONTH) AS repeat_month,-- месяц, когда была совершена повторная покупка
    purchase_id AS repeat_purchase_id 
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  WHERE 1=1 
  AND is_first_purchase = 0 
  AND user_id IN (SELECT user_id FROM a)  
  )
, c AS (  -- посчитать дельту по месяцам
  SELECT 
    a.user_id,
    EXTRACT(MONTH FROM a.cohort_month) AS cohort_month,
    EXTRACT(MONTH FROM b.repeat_month) AS repeat_month,
    a.first_purchase_id,
    b.repeat_purchase_id,
    EXTRACT(MONTH FROM b.repeat_month) - EXTRACT(MONTH FROM a.cohort_month) AS delta_month -- дельта отрицательная получается из-за 01.2024
  FROM a FULL JOIN b ON a.user_id = b.user_id
  -- WHERE a.user_id = 3823973
  )
,d AS (   
  SELECT
    cohort_month,
    CASE WHEN repeat_month IS NULL THEN 0 
    ELSE repeat_month
    END AS repeat_month, 
    CASE WHEN delta_month IS NULL THEN 0 -- объединил дельту null и 0,т.к по дельте null user_id, у которых нет повторных покупок
    ELSE delta_month
    END AS delta_month,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY delta_month) AS row,-- присвоить номер покупки в разрезе покупателя
    first_purchase_id,
    repeat_purchase_id
  FROM c
)
, e AS ( --удалить дубли user_id
SELECT
  cohort_month,
  repeat_month,
  delta_month,
  CASE WHEN row = 1 THEN user_id
  ELSE NULL 
  END AS new_users,
  repeat_purchase_id
FROM d
)
, g AS ( -- посчитать количество  новых пользователей и повторных покупок
SELECT
  cohort_month,
  repeat_month,
  delta_month,
  COUNT(DISTINCT new_users) AS new_users,
  COUNT(repeat_purchase_id) AS repeat_purchases
FROM e
GROUP BY 1,2,3
)
SELECT -- посчитать retention_rate
  cohort_month, -- месяц, когда была совершена первая покупка
  repeat_month,-- месяц, когда была совершена повторная покупка
  delta_month, -- разница между месяцем первой покупки и месяцем повторной покупки
  new_users,
  repeat_purchases,
  FIRST_VALUE(new_users) OVER (PARTITION BY cohort_month ORDER BY repeat_month), -- количество пользователей на начало периода;
  new_users / FIRST_VALUE(new_users) OVER (PARTITION BY cohort_month ORDER BY repeat_month) AS retention_rate -- процен оставшихся пользователей на конец периода
FROM g
GROUP BY 1,2,3,4,5