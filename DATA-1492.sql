WITH table_1 AS (
SELECT segment_cab, SUM(expenses) AS expense_cab FROM ( -- посчитать сумму расходов из интерфейса
	SELECT
	CASE
		WHEN REGEXP_CONTAINS(LOWER(campaign),"seller|courier|продав|курьер|бизнес")=false
		AND REGEXP_CONTAINS(LOWER(campaign),'ios|android|app|uac')
		AND REGEXP_CONTAINS(LOWER(campaign),"flight")=false THEN 'yandex_direct_app'

		WHEN REGEXP_CONTAINS(LOWER(campaign),"seller|courier|продав|курьер|бизнес")=false AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") 
		AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|rsy")=false 
		AND REGEXP_CONTAINS(LOWER(campaign),"flight")=false AND REGEXP_CONTAINS(LOWER(campaign),"brand|бренд") 
		AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|rsy")=false
		AND channel NOT IN ('apple_search_ads')
		AND REGEXP_CONTAINS(LOWER(campaign),"flight")=false THEN 'yandex_direct_web_brand'

		WHEN REGEXP_CONTAINS(LOWER(campaign),"seller|courier|продав|курьер|бизнес")=false 
		AND REGEXP_CONTAINS(LOWER(campaign),"flight")=false
		AND REGEXP_CONTAINS(LOWER(campaign),"smart|banner|display|flight|rsy|sb|msk2|retarg|brand|бренд|vvk|network|rsy|рся|срм")=false 
		AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac")=false 
		AND channel NOT IN ('apple_search_ads')
		AND REGEXP_CONTAINS(LOWER(campaign),"video")=FALSE THEN 'yandex_direct_web_non_brand'

		WHEN REGEXP_CONTAINS(LOWER(campaign),"seller|courier|продав|курьер|бизнес")=false 
		AND REGEXP_CONTAINS(LOWER(campaign),"рся|rsy|network|retarget|smart") 
		AND REGEXP_CONTAINS(LOWER(campaign),"ios|andr|app|uac|brand|бренд|src")=false
		AND REGEXP_CONTAINS(LOWER(campaign),"flight")=false THEN 'yandex_direct_web_rsy'

		WHEN REGEXP_CONTAINS(LOWER(campaign),"seller|courier|продав|курьер|бизнес")=FALSE
 		AND REGEXP_CONTAINS(LOWER(campaign),'brand|бренд')
		AND REGEXP_CONTAINS(LOWER(channel),'apple_search_ads') THEN 'asa_brand' 
 
		WHEN REGEXP_CONTAINS(LOWER(campaign),"seller|courier|продав|курьер|бизнес")=false 
 		AND REGEXP_CONTAINS(LOWER(campaign),'brand|бренд')=FALSE
 		AND REGEXP_CONTAINS(LOWER(channel),'apple_search_ads') THEN 'asa_non_brand'

		ELSE 'other'
		END AS segment_cab,	
		expenses -- расходы из интерфейса кабинета
	FROM `funnel-flowwow.Analyt_ChumichevN.asa_cost_july_2023_v2`
)
GROUP BY segment_cab
)
, table_2 AS ( -- посчитать сумму расходов по сегментам из бд
SELECT
	segment,
	-- ads_cost AS ads_expense_m, 
	SUM(ads_cost) AS ads_expense_m, 
	-- SUM(ads_cost)/1.2 AS expemse_without_vat
FROM funnel-flowwow.PRODUCTION_DM.cm_data_reattribution
WHERE date BETWEEN "2023-08-01" AND "2023-08-13" AND segment IN ('yandex_direct_web_non_brand','yandex_direct_web_brand','yandex_direct_web_rsy', 'yandex_direct_app','vk_ads','asa_non_brand', 'asa_brand')
GROUP BY 1
ORDER BY 1 DESC
)
SELECT --сравнить расходы из интерфейса и бд
	segment,
	ROUND(ads_expense_m,2) AS ads_expense_m_,
	segment_cab,
	ROUND(expense_cab,2) as expense_cab_,
	ads_expense_m - expense_cab AS diff,
	100 * ((ads_expense_m - expense_cab) / ads_expense_m) AS diff_perc,
FROM table_1 i 
	FULL JOIN table_2 m ON i.segment_cab = m.segment 
	-- GROUP BY campaign_, segment
	-- ORDER BY expense_cab DESC