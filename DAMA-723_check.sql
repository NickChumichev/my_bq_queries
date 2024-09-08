WITH a AS (
SELECT 
  date,
  source,
  medium,
  campaign_id,
  campaign,
  campaign_info,
  cost,
  clicks,
  impressions
 FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cost` 
 WHERE 1=1
--  AND date <= '2024-06-03'
 AND source != 'google'

UNION ALL

SELECT 
  date,
  source,
  medium,
  campaign_id,
  campaign,
  campaign_info,
  cost,
  clicks,
  impressions
 FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cost` 
 WHERE 1=1
--  AND date <= '2024-06-03'
 AND source = 'google'
 QUALIFY ROW_NUMBER() OVER (PARTITION BY date, source, medium, campaign_id, CAST(cost AS STRING)) = 1
)
SELECT
  SUM(cost)
FROM a