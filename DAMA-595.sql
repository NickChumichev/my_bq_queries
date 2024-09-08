 WITH
    without_campaign_table AS
    (
    -- список РК, где заносятся косты вручную и не может быть метчинга (косты-трафик) в разрезе РК
    SELECT "all_cashback"as without_campaign UNION ALL
    SELECT "appnext"as without_campaign UNION ALL
    SELECT "aura"as without_campaign UNION ALL
    SELECT "cpa"as without_campaign UNION ALL
    SELECT "cpa_inactive"as without_campaign UNION ALL
    SELECT "creatives"as without_campaign UNION ALL
    SELECT "donate"as without_campaign UNION ALL
    SELECT "dzen_client"as without_campaign UNION ALL
    SELECT "email_push"as without_campaign UNION ALL
    SELECT "google_seo"as without_campaign UNION ALL
    SELECT "offline_merch"as without_campaign UNION ALL
    SELECT "pr_rus"as without_campaign UNION ALL
    SELECT "pr_international"as without_campaign UNION ALL
    SELECT "serm"as without_campaign UNION ALL
    SELECT "smm_telegram"as without_campaign UNION ALL
    SELECT "special_offline"as without_campaign UNION ALL
    SELECT "special_online"as without_campaign UNION ALL
    SELECT "two_gis"as without_campaign UNION ALL
    SELECT "unity"as without_campaign UNION ALL
    SELECT "ya_promo"as without_campaign UNION ALL
    SELECT "yandex_seo"as without_campaign UNION ALL
    SELECT "test_segment"as without_campaign UNION ALL
    SELECT "partners_cpa"as without_campaign UNION ALL
    SELECT "tg_bot"as without_campaign UNION ALL
    SELECT "xapads"as without_campaign UNION ALL
    SELECT "creatives"as without_campaign UNION ALL
    SELECT "creatives"as without_campaign UNION ALL
    SELECT "smm_pikabu"as without_campaign UNION ALL
    SELECT "appnext"as without_campaign UNION ALL
    SELECT "aura"as without_campaign UNION ALL
    SELECT "cpa"as without_campaign UNION ALL
    SELECT "facebook_ads"as without_campaign UNION ALL
    SELECT "mytarget"as without_campaign UNION ALL
    SELECT "unity"as without_campaign UNION ALL
    SELECT "xapads"as without_campaign 
    ),
    traffic_table AS 
     (
     -- в рекламных сегментах, где возможен мэтчинг в разерезе РК, трафик делится по РК, где нет - трафик делится только по дням.
     SELECT  
     date, subdivision, channel, segment, source, medium, campaign,   city_from, city_to, country_from, country_to, region, category, subcategory, platform,
     SUM(traffic) as traffic,
     SAFE_DIVIDE(SUM(traffic), SUM(SUM(traffic)) OVER(PARTITION BY date, subdivision, channel, segment, campaign)) as proportion,
     SAFE_DIVIDE(SUM(traffic), SUM(SUM(traffic)) OVER(PARTITION BY date, subdivision, channel, segment)) as proportion_without_campaign,
     FROM `funnel-flowwow.PRODUCTION_DM.cm_data_by_campaigns` 
     WHERE traffic>0
     AND segment = 'two_gis' AND date BETWEEN '2024-03-01' AND '2024-05-02'
     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
     )
     ,traffic_table_top_100_proportion AS
     (
     -- определяем перыве топ 100 разрезов каждого дня
     SELECT
     date, 
     subdivision, channel, segment, source, medium, campaign,   city_from, city_to, country_from, country_to, region, category, subcategory, platform,
     traffic,SAFE_DIVIDE(traffic,SUM(traffic) OVER(PARTITION BY date)) as proportion_date
     FROM (
     SELECT
     date, subdivision, channel, segment, source, medium, campaign,   city_from, city_to, country_from, country_to, region, category, subcategory, platform,
     traffic,
     FROM traffic_table
     WHERE segment = 'two_gis' AND date BETWEEN '2024-03-01' AND '2024-05-02'
     QUALIFY RANK() OVER(PARTITION BY date ORDER BY traffic DESC)<=100)
     )
     ,cost_table AS
     (
     -- косты сегментов, где возможен мэтчинг по РК
     SELECT  
     date, subdivision, channel, segment, source, medium, campaign,    SUM(ads_cost) as ads_cost,
     FROM `funnel-flowwow.PRODUCTION_DM.cm_data_by_campaigns` 
     WHERE segment NOT IN (SELECT without_campaign FROM without_campaign_table)
     AND segment = 'two_gis' AND date BETWEEN '2024-03-01' AND '2024-05-02'
     GROUP BY 1,2,3,4,5,6,7
     ),
     cost_without_campaign_table AS
     (
     -- косты сегментов, где НЕвозможен мэтчинг по РК
     SELECT  
     date, subdivision, channel, segment, source, medium, campaign,    SUM(ads_cost) as ads_cost,
     FROM `funnel-flowwow.PRODUCTION_DM.cm_data_by_campaigns` 
     WHERE segment IN (SELECT without_campaign FROM without_campaign_table)
     AND segment = 'two_gis' AND date BETWEEN '2024-03-01' AND '2024-05-02'
     GROUP BY 1,2,3,4,5,6,7
     )
     -- соединяем косты, поделённые по РК и НЕ поделённые по РК
     -- если РК и/или сегмент не имеет трафика в этот день, то срабатывает логика первых топ 100 разрезов этого дня, чтобы косты разнести по разрезам
     , a AS (
     SELECT 
     date, subdivision, channel, segment, cost_table.source, cost_table.medium, campaign,city_from, city_to, country_from, country_to, region, category, subcategory, platform,
     ads_cost*proportion as ads_cost_by_traffic_v_2
     FROM traffic_table
     FULL JOIN cost_table USING(date, subdivision, channel, segment, campaign)
    WHERE proportion IS NOT NULL
    
     UNION ALL

     SELECT 
     date, subdivision, channel, segment, cost_without_campaign_table.source, cost_without_campaign_table.medium, traffic_table.campaign, city_from, city_to, country_from, country_to, region, category, subcategory, platform,
     ads_cost*proportion_without_campaign as ads_cost_by_traffic_v_2
     FROM traffic_table
     FULL JOIN cost_without_campaign_table USING(date, subdivision, channel, segment)
    WHERE proportion IS NOT NULL
     UNION ALL

     SELECT
     cost_table.date, cost_table.subdivision, cost_table.channel, cost_table.segment, cost_table.source, cost_table.medium, cost_table.campaign,  traffic_table_top_100_proportion.city_from,  traffic_table_top_100_proportion.city_to,  traffic_table_top_100_proportion.country_from,  traffic_table_top_100_proportion.country_to,  traffic_table_top_100_proportion.region,  traffic_table_top_100_proportion.category,  traffic_table_top_100_proportion.subcategory,  traffic_table_top_100_proportion.platform,
     ads_cost*IFNULL(proportion_date,1) as ads_cost_by_traffic_v_2
     FROM traffic_table
     FULL JOIN cost_table USING(date, subdivision, channel, segment, campaign) CROSS JOIN traffic_table_top_100_proportion
    WHERE proportion IS NULL AND traffic_table_top_100_proportion.date=cost_table.date 
     UNION ALL

     SELECT
     cost_without_campaign_table.date, cost_without_campaign_table.subdivision, cost_without_campaign_table.channel, cost_without_campaign_table.segment, cost_without_campaign_table.source, cost_without_campaign_table.medium, cost_without_campaign_table.campaign,traffic_table_top_100_proportion.city_from,  traffic_table_top_100_proportion.city_to,  traffic_table_top_100_proportion.country_from,  traffic_table_top_100_proportion.country_to,  traffic_table_top_100_proportion.region,  traffic_table_top_100_proportion.category,  traffic_table_top_100_proportion.subcategory,  traffic_table_top_100_proportion.platform,
     ads_cost*IFNULL(proportion_date,1) as ads_cost_by_traffic_v_2
     FROM traffic_table
     FULL JOIN cost_without_campaign_table USING(date, subdivision, channel, segment) CROSS JOIN traffic_table_top_100_proportion
    WHERE proportion IS NULL AND traffic_table_top_100_proportion.date=cost_without_campaign_table.date 
    )
    , b AS (
      SELECT DISTINCT 
      date, subdivision, channel,	segment,	source,	medium,	campaign,	city_from,	city_to,	country_from,	country_to,	region,	category,	subcategory,	platform,	is_retargeting_campaign,	is_world_campaign,	ads_cost,	not_ads_cost,	service_cost,	clicks,	impressions,	purchases,	first_purchases,	transactions,	first_transactions,	revenue,	revenue_first_transactions,	promo_cost,	bonus_company,	traffic,	first_traffic,	installs,	first_transactions_from_install_7,	first_transactions_from_install_14,	first_transactions_from_install_30,	first_transactions_from_install_90,	first_transactions_from_install_365,	first_transactions_from_install_365_more,	revenue_first_transactions_from_install_7,	revenue_first_transactions_from_install_14,	revenue_first_transactions_from_install_30,	revenue_first_transactions_from_install_90,	revenue_first_transactions_from_install_365,	revenue_first_transactions_from_install_365_more,	repeat_transactions_from_install_7,	repeat_transactions_from_install_14,	repeat_transactions_from_install_30,	repeat_transactions_from_install_90,	repeat_transactions_from_install_365,	repeat_transactions_from_install_365_more,	revenue_repeat_transactions_from_install_7,	revenue_repeat_transactions_from_install_14,	revenue_repeat_transactions_from_install_30,	revenue_repeat_transactions_from_install_90,	revenue_repeat_transactions_from_install_365,	revenue_repeat_transactions_from_install_365_more,	purchases_from_first_30,	purchases_from_first_90,	purchases_from_first_365,	purchases_from_first_2_years,	purchases_from_first_3_years,	purchases_from_first_4_years,	purchases_from_first_5_years,	purchases_from_first_5_years_more,	transactions_from_first_30,	transactions_from_first_90,	transactions_from_first_365,	transactions_from_first_2_years,	transactions_from_first_3_years,	transactions_from_first_4_years,	transactions_from_first_5_years,	transactions_from_first_5_years_more,	revenue_from_first_30,	revenue_from_first_90,	revenue_from_first_365,	revenue_from_first_2_years,	revenue_from_first_3_years,	revenue_from_first_4_years,	revenue_from_first_5_years,	revenue_from_first_5_years_more,	flowers_purchases_from_first_30,	flowers_purchases_from_first_90,	flowers_purchases_from_first_365,	flowers_purchases_from_first_365_more,	flowers_revenue_from_first_30,	flowers_revenue_from_first_90,	flowers_revenue_from_first_365,	flowers_revenue_from_first_365_more,	bakery_purchases_from_first_30,	bakery_purchases_from_first_90,	bakery_purchases_from_first_365,	bakery_purchases_from_first_365_more,	bakery_revenue_from_first_30,	bakery_revenue_from_first_90,	bakery_revenue_from_first_365,	bakery_revenue_from_first_365_more,	plant_flowers_purchases_from_first_30,	plant_flowers_purchases_from_first_90,	plant_flowers_purchases_from_first_365,	plant_flowers_purchases_from_first_365_more,	plant_flowers_revenue_from_first_30,	plant_flowers_revenue_from_first_90,	plant_flowers_revenue_from_first_365,	plant_flowers_revenue_from_first_365_more,	ads_cost_by_traffic_v_2,

      FROM `funnel-flowwow.PRODUCTION_DM.cm_data_by_campaigns` 
 FULL JOIN a
 USING(date, subdivision, channel, segment, source, medium, campaign,   city_from, city_to, country_from, country_to, region, category, subcategory, platform)
 WHERE segment = 'two_gis' AND date BETWEEN '2024-03-01' AND '2024-05-02'
    )
     SELECT
      DATE_TRUNC(date,WEEK(MONDAY)) AS week,
      SUM(ads_cost_by_traffic_v_2) AS cost,
      -- COUNT(date) AS cnt,
      SUM(transactions) AS transactions,
      SUM(revenue) AS revenue
      FROM b
      -- WHERE date = '2024-04-22'
      GROUP BY 1
      ORDER BY week DESC