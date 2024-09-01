WITH a AS ( -- фиксированный расход по интеграции
  SELECT
    DATE_TRUNC(PARSE_DATE('%m/%d/%Y', date),MONTH) AS month, 
    CASE
      WHEN country IN ('Испания') THEN 'Испания' 
      WHEN country IN ('Сербия') THEN 'Сербия'
      WHEN country IN ('Турция') THEN 'Турция'
      WHEN country IN ('Грузия') THEN 'Грузия'
      WHEN country IN ('Казахстан') THEN 'Казахстан'
      WHEN country IN ('Беларусь') THEN 'Беларусь'
      WHEN country IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
      WHEN country IN ('Россия') THEN 'Россия'
      WHEN country IN ('Армения') THEN 'Армения'
      WHEN country IN ('Великобритания') THEN 'Великобритания'
      WHEN country IN ('Польша') THEN 'Польша'
      WHEN country IN ('Турция') THEN 'Турция'
      WHEN country IN ('Франция') THEN 'Франция'
      WHEN country IN ('Украина') THEN 'Украина'
      WHEN country IN ('Узбекистан') THEN 'Узбекистан'
      WHEN country IN ('Израиль') THEN 'Израиль'
      WHEN country IN ('Латвия') THEN 'Латвия'
      WHEN country IN ('Эстония') THEN 'Эстония'
      WHEN country IN ('Чехия') THEN 'Чехия'
      WHEN country IN ('Литва') THEN 'Литва'
      WHEN country IN ('Кипр') THEN 'Кипр'
      WHEN country IN ('Кыргызстан') THEN 'Кыргызстан'
      ELSE 'other countries'
      END AS country, --отделяет интеграции и расход друг от друга
    SUM(CAST(cost_rub AS numeric)) AS cost
  FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`
  WHERE 1=1
  AND cost_rub IS NOT NULL -- у некоторых инфлюенсеров нет расхода
  AND CAST(cost_rub AS NUMERIC) != 0
  GROUP BY 1,2
  ORDER BY month ASC
  )
  , b AS (
  SELECT --расходы по промокодам
    DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS month,
    CASE --страна расхода по промокоду
      WHEN country_to IN ('Испания') THEN 'Испания' 
      WHEN country_to IN ('Сербия') THEN 'Сербия'
      WHEN country_to IN ('Турция') THEN 'Турция'
      WHEN country_to IN ('Грузия') THEN 'Грузия'
      WHEN country_to IN ('Казахстан') THEN 'Казахстан'
      WHEN country_to IN ('Беларусь') THEN 'Беларусь'
      WHEN country_to IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
      WHEN country_to IN ('Россия') THEN 'Россия'
      WHEN country_to IN ('Армения') THEN 'Армения'
      WHEN country_to IN ('Великобритания') THEN 'Великобритания'
      WHEN country_to IN ('Польша') THEN 'Польша'
      WHEN country_to IN ('Турция') THEN 'Турция'
      WHEN country_to IN ('Франция') THEN 'Франция'
      WHEN country_to IN ('Украина') THEN 'Украина'
      WHEN country_to IN ('Узбекистан') THEN 'Узбекистан'
      WHEN country_to IN ('Израиль') THEN 'Израиль'
      WHEN country_to IN ('Латвия') THEN 'Латвия'
      WHEN country_to IN ('Эстония') THEN 'Эстония'
      WHEN country_to IN ('Чехия') THEN 'Чехия'
      WHEN country_to IN ('Литва') THEN 'Литва'
      WHEN country_to IN ('Кипр') THEN 'Кипр'
      WHEN country_to IN ('Кыргызстан') THEN 'Кыргызстан'
      ELSE 'other countries'
      END AS country_to,
    COUNT(DISTINCT purchase_id) AS purchases,
    COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id,NULL)) as first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(is_first_purchase = 1, user_id, NULL))) AS new_users, 
    SUM(product_price) as revenue,
    SUM(promo_sum_rub) AS promo_cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  WHERE LOWER(promocode) IN (SELECT LOWER(personal_promocode) FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`)
  GROUP BY 1,2
  UNION ALL
  SELECT --расходы по промокодам от российских блогеров
    DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS month,
    CASE   --страна расхода по промокоду
      WHEN country_to IN ('Испания') THEN 'Испания' 
      WHEN country_to IN ('Сербия') THEN 'Сербия'
      WHEN country_to IN ('Турция') THEN 'Турция'
      WHEN country_to IN ('Грузия') THEN 'Грузия'
      WHEN country_to IN ('Казахстан') THEN 'Казахстан'
      WHEN country_to IN ('Беларусь') THEN 'Беларусь'
      WHEN country_to IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
      WHEN country_to IN ('Россия') THEN 'Россия'
      WHEN country_to IN ('Армения') THEN 'Армения'
      WHEN country_to IN ('Великобритания') THEN 'Великобритания'
      WHEN country_to IN ('Польша') THEN 'Польша'
      WHEN country_to IN ('Турция') THEN 'Турция'
      WHEN country_to IN ('Франция') THEN 'Франция'
      WHEN country_to IN ('Украина') THEN 'Украина'
      WHEN country_to IN ('Узбекистан') THEN 'Узбекистан'
      WHEN country_to IN ('Израиль') THEN 'Израиль'
      WHEN country_to IN ('Латвия') THEN 'Латвия'
      WHEN country_to IN ('Эстония') THEN 'Эстония'
      WHEN country_to IN ('Чехия') THEN 'Чехия'
      WHEN country_to IN ('Литва') THEN 'Литва'
      WHEN country_to IN ('Кипр') THEN 'Кипр'
      WHEN country_to IN ('Кыргызстан') THEN 'Кыргызстан'
      ELSE 'other countries'
      END AS country_to, 
    COUNT(DISTINCT purchase_id) AS purchases,
    COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id,NULL)) as first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(is_first_purchase = 1, user_id, NULL))) AS new_users, 
    SUM(product_price) as revenue,
    SUM(promo_sum_rub) AS promo_cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  WHERE LOWER(promocode) IN (SELECT LOWER(personal_promocode) FROM `funnel-flowwow.BUSINESS_DM.influencers_gs_view`)
  AND country_from != 'Россия'
  GROUP BY 1,2
  UNION ALL
  SELECT -- транзакции по интеграциям без промокодов
    DATE_TRUNC(DATE(s.date),MONTH) AS month,
    CASE   --страна расхода по промокоду
      WHEN s.country_to IN ('Испания') THEN 'Испания' 
      WHEN s.country_to IN ('Сербия') THEN 'Сербия'
      WHEN s.country_to IN ('Турция') THEN 'Турция'
      WHEN s.country_to IN ('Грузия') THEN 'Грузия'
      WHEN s.country_to IN ('Казахстан') THEN 'Казахстан'
      WHEN s.country_to IN ('Беларусь') THEN 'Беларусь'
      WHEN s.country_to IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
      WHEN s.country_to IN ('Россия') THEN 'Россия'
      WHEN s.country_to IN ('Армения') THEN 'Армения'
      WHEN s.country_to IN ('Великобритания') THEN 'Великобритания'
      WHEN s.country_to IN ('Польша') THEN 'Польша'
      WHEN s.country_to IN ('Турция') THEN 'Турция'
      WHEN s.country_to IN ('Франция') THEN 'Франция'
      WHEN s.country_to IN ('Украина') THEN 'Украина'
      WHEN s.country_to IN ('Узбекистан') THEN 'Узбекистан'
      WHEN s.country_to IN ('Израиль') THEN 'Израиль'
      WHEN s.country_to IN ('Латвия') THEN 'Латвия'
      WHEN s.country_to IN ('Эстония') THEN 'Эстония'
      WHEN s.country_to IN ('Чехия') THEN 'Чехия'
      WHEN s.country_to IN ('Литва') THEN 'Литва'
      WHEN s.country_to IN ('Кипр') THEN 'Кипр'
      WHEN s.country_to IN ('Кыргызстан') THEN 'Кыргызстан'
      ELSE 'other countries'
      END AS country_to, 
    COUNT(DISTINCT s.transactionid) AS purchases,
    COUNT(DISTINCT IF(s.is_first_purchase = 1, s.transactionid,NULL)) AS first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(s.is_first_purchase = 1, user_id, NULL))) AS new_users,
    SUM(s.purchase_sum_rub) AS revenue,
    0 AS cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON s.transactionid = y.purchase_id AND s.product_id = y.product_id
  WHERE LOWER(campaign) IN (SELECT LOWER(campaign) FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`)
  AND s.promocode IN ('')
  GROUP BY 1,2
  UNION ALL
  SELECT -- транзакции по интеграциям российских блогеров без промокодов
    DATE_TRUNC(DATE(s.date),MONTH) AS month,
     CASE   --страна расхода по промокоду
      WHEN s.country_to IN ('Испания') THEN 'Испания' 
      WHEN s.country_to IN ('Сербия') THEN 'Сербия'
      WHEN s.country_to IN ('Турция') THEN 'Турция'
      WHEN s.country_to IN ('Грузия') THEN 'Грузия'
      WHEN s.country_to IN ('Казахстан') THEN 'Казахстан'
      WHEN s.country_to IN ('Беларусь') THEN 'Беларусь'
      WHEN s.country_to IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
      WHEN s.country_to IN ('Россия') THEN 'Россия'
      WHEN s.country_to IN ('Армения') THEN 'Армения'
      WHEN s.country_to IN ('Великобритания') THEN 'Великобритания'
      WHEN s.country_to IN ('Польша') THEN 'Польша'
      WHEN s.country_to IN ('Турция') THEN 'Турция'
      WHEN s.country_to IN ('Франция') THEN 'Франция'
      WHEN s.country_to IN ('Украина') THEN 'Украина'
      WHEN s.country_to IN ('Узбекистан') THEN 'Узбекистан'
      WHEN s.country_to IN ('Израиль') THEN 'Израиль'
      WHEN s.country_to IN ('Латвия') THEN 'Латвия'
      WHEN s.country_to IN ('Эстония') THEN 'Эстония'
      WHEN s.country_to IN ('Чехия') THEN 'Чехия'
      WHEN s.country_to IN ('Литва') THEN 'Литва'
      WHEN s.country_to IN ('Кипр') THEN 'Кипр'
      WHEN s.country_to IN ('Кыргызстан') THEN 'Кыргызстан'
      ELSE 'other countries'
      END AS country_to, 
    COUNT(DISTINCT s.transactionid) AS purchases,
    COUNT(DISTINCT IF(s.is_first_purchase = 1, s.transactionid,NULL)) AS first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(s.is_first_purchase = 1, user_id, NULL))) AS new_users,
    SUM(s.purchase_sum_rub) AS revenue,
    0 AS cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON s.transactionid = y.purchase_id AND s.product_id = y.product_id
  WHERE LOWER(campaign) IN (SELECT LOWER(campaign) FROM `funnel-flowwow.BUSINESS_DM.influencers_gs_view`)
  AND s.promocode IN ('') AND s.country_from != 'Россия'
  GROUP BY 1,2
  )
  , c AS (
  SELECT -- соединить данные по промокодам и данные по интеграциям
    month,
    country,
    0 AS users,
    0 AS new_users,
    0 AS purchases,
    0 AS first_purchases,
    0 AS revenue,
    cost
  FROM a
    UNION ALL
  SELECT 
    month,
    country_to,
    users,
    new_users,
    purchases,
    first_purchases,
    revenue,
    promo_cost AS cost
  FROM b
  )
  , d AS (
  SELECT
    month,
    country,
    SUM(users) AS users,
    SUM(new_users) AS new_users,
    SUM(purchases) AS purchases,
    SUM(first_purchases) AS first_purchases,
    SUM(revenue) AS revenue,
    SUM(cost) AS cost,
    SAFE_DIVIDE(SUM(cost) , SUM(new_users)) AS CAC_new_users,
    SAFE_DIVIDE(SUM(revenue) , SUM(new_users)) AS LTV_new_users
  FROM c
  GROUP BY 1,2
  ORDER BY country, month ASC
  )
  SELECT --разделить доход, покупки, пользователей между зарубежными и российскими блогерами 
    month,
    country,
    users,
    users_foreign_blogers,
    users_russian_blogers,
    new_users,
    new_users_foreign_blogers,
    new_users_russian_blogers,
    purchases,
    purchases_foreign_blogers,
    purchases_russian_blogers,
    first_purchases,
    first_purchases_foreign_blogers,
    first_purchases_russian_blogers,
    revenue,
    revenue_foreign_blogers,
    revenue_russian_blogers,
    cost,
    cost_foreign_blogers,
    cost_russian_blogers,
    CAC_new_users,
    CAC_new_users_foreign_blogers,
    CAC_new_users_russian_blogers,
    LTV_new_users,
    LTV_new_users_foreign_blogers,
    LTV_new_users_russian_blogers
  FROM d
  FULL JOIN (
      WITH e AS (
      SELECT --расходы по промокодам
        DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS month,
        CASE --страна расхода по промокоду
            WHEN country_to IN ('Испания') THEN 'Испания' 
            WHEN country_to IN ('Сербия') THEN 'Сербия'
            WHEN country_to IN ('Турция') THEN 'Турция'
            WHEN country_to IN ('Грузия') THEN 'Грузия'
            WHEN country_to IN ('Казахстан') THEN 'Казахстан'
            WHEN country_to IN ('Беларусь') THEN 'Беларусь'
            WHEN country_to IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
            WHEN country_to IN ('Россия') THEN 'Россия'
            WHEN country_to IN ('Армения') THEN 'Армения'
            WHEN country_to IN ('Великобритания') THEN 'Великобритания'
            WHEN country_to IN ('Польша') THEN 'Польша'
            WHEN country_to IN ('Турция') THEN 'Турция'
            WHEN country_to IN ('Франция') THEN 'Франция'
            WHEN country_to IN ('Украина') THEN 'Украина'
            WHEN country_to IN ('Узбекистан') THEN 'Узбекистан'
            WHEN country_to IN ('Израиль') THEN 'Израиль'
            WHEN country_to IN ('Латвия') THEN 'Латвия'
            WHEN country_to IN ('Эстония') THEN 'Эстония'
            WHEN country_to IN ('Чехия') THEN 'Чехия'
            WHEN country_to IN ('Литва') THEN 'Литва'
            WHEN country_to IN ('Кипр') THEN 'Кипр'
            WHEN country_to IN ('Кыргызстан') THEN 'Кыргызстан'
        ELSE 'other countries'
        END AS country_to,
        COUNT(DISTINCT purchase_id) AS purchases,
        COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id,NULL)) as first_purchases,
        COUNT(DISTINCT user_id) AS users,
        COUNT(DISTINCT (IF(is_first_purchase = 1, user_id, NULL))) AS new_users, 
        SUM(product_price) as revenue,
        SUM(promo_sum_rub) AS cost
    FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
    WHERE LOWER(promocode) IN (SELECT LOWER(personal_promocode) FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`)
    GROUP BY 1,2
    UNION ALL
    SELECT -- транзакции по интеграциям без промокодов
    DATE_TRUNC(DATE(s.date),MONTH) AS month,
    CASE   --страна расхода по промокоду
      WHEN s.country_to IN ('Испания') THEN 'Испания' 
      WHEN s.country_to IN ('Сербия') THEN 'Сербия'
      WHEN s.country_to IN ('Турция') THEN 'Турция'
      WHEN s.country_to IN ('Грузия') THEN 'Грузия'
      WHEN s.country_to IN ('Казахстан') THEN 'Казахстан'
      WHEN s.country_to IN ('Беларусь') THEN 'Беларусь'
      WHEN s.country_to IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
      WHEN s.country_to IN ('Россия') THEN 'Россия'
      WHEN s.country_to IN ('Армения') THEN 'Армения'
      WHEN s.country_to IN ('Великобритания') THEN 'Великобритания'
      WHEN s.country_to IN ('Польша') THEN 'Польша'
      WHEN s.country_to IN ('Турция') THEN 'Турция'
      WHEN s.country_to IN ('Франция') THEN 'Франция'
      WHEN s.country_to IN ('Украина') THEN 'Украина'
      WHEN s.country_to IN ('Узбекистан') THEN 'Узбекистан'
      WHEN s.country_to IN ('Израиль') THEN 'Израиль'
      WHEN s.country_to IN ('Латвия') THEN 'Латвия'
      WHEN s.country_to IN ('Эстония') THEN 'Эстония'
      WHEN s.country_to IN ('Чехия') THEN 'Чехия'
      WHEN s.country_to IN ('Литва') THEN 'Литва'
      WHEN s.country_to IN ('Кипр') THEN 'Кипр'
      WHEN s.country_to IN ('Кыргызстан') THEN 'Кыргызстан'
      ELSE 'other countries'
      END AS country_to, 
    COUNT(DISTINCT s.transactionid) AS purchases,
    COUNT(DISTINCT IF(s.is_first_purchase = 1, s.transactionid,NULL)) AS first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(s.is_first_purchase = 1, user_id, NULL))) AS new_users,
    SUM(s.purchase_sum_rub) AS revenue,
    0 AS cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON s.transactionid = y.purchase_id AND s.product_id = y.product_id
  WHERE LOWER(campaign) IN (SELECT LOWER(campaign) FROM `funnel-flowwow.BUSINESS_DM.influencers_glob_gs_view`)
  AND s.promocode IN ('')
  GROUP BY 1,2
  UNION ALL
  SELECT -- соединить данные по промокодам и данные по интеграциям
    month,
    country,
    0 AS users,
    0 AS new_users,
    0 AS purchases,
    0 AS first_purchases,
    0 AS revenue,
    cost
    FROM a
    )
    SELECT
      month,
      country_to AS country,
      SUM(users) AS users_foreign_blogers,
      SUM(new_users) AS new_users_foreign_blogers,
      SUM(purchases) AS purchases_foreign_blogers,
      SUM(first_purchases) AS first_purchases_foreign_blogers,
      SUM(revenue) AS revenue_foreign_blogers,
      SUM(cost) AS cost_foreign_blogers,
      SAFE_DIVIDE(SUM(cost) , SUM(new_users)) AS CAC_new_users_foreign_blogers,
      SAFE_DIVIDE(SUM(revenue) , SUM(new_users)) AS LTV_new_users_foreign_blogers
    FROM e
    GROUP BY 1,2
    ORDER BY month ASC 
  ) AS f USING (month,country)
  LEFT JOIN (
    WITH g AS (
    SELECT --расходы по промокодам от российских блогеров
    DATE_TRUNC(DATE(purchase_timestamp),MONTH) AS month,
   CASE   --страна расхода по промокоду
      WHEN country_to IN ('Испания') THEN 'Испания' 
      WHEN country_to IN ('Сербия') THEN 'Сербия'
      WHEN country_to IN ('Турция') THEN 'Турция'
      WHEN country_to IN ('Грузия') THEN 'Грузия'
      WHEN country_to IN ('Казахстан') THEN 'Казахстан'
      WHEN country_to IN ('Беларусь') THEN 'Беларусь'
      WHEN country_to IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
      WHEN country_to IN ('Россия') THEN 'Россия'
      WHEN country_to IN ('Армения') THEN 'Армения'
      WHEN country_to IN ('Великобритания') THEN 'Великобритания'
      WHEN country_to IN ('Польша') THEN 'Польша'
      WHEN country_to IN ('Турция') THEN 'Турция'
      WHEN country_to IN ('Франция') THEN 'Франция'
      WHEN country_to IN ('Украина') THEN 'Украина'
      WHEN country_to IN ('Узбекистан') THEN 'Узбекистан'
      WHEN country_to IN ('Израиль') THEN 'Израиль'
      WHEN country_to IN ('Латвия') THEN 'Латвия'
      WHEN country_to IN ('Эстония') THEN 'Эстония'
      WHEN country_to IN ('Чехия') THEN 'Чехия'
      WHEN country_to IN ('Литва') THEN 'Литва'
      WHEN country_to IN ('Кипр') THEN 'Кипр'
      WHEN country_to IN ('Кыргызстан') THEN 'Кыргызстан'
      ELSE 'other countries'
      END AS country_to, 
    COUNT(DISTINCT purchase_id) AS purchases,
    COUNT(DISTINCT IF(is_first_purchase = 1, purchase_id,NULL)) as first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(is_first_purchase = 1, user_id, NULL))) AS new_users, 
    SUM(product_price) as revenue,
    SUM(promo_sum_rub) AS cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company`
  WHERE LOWER(promocode) IN (SELECT LOWER(personal_promocode) FROM `funnel-flowwow.BUSINESS_DM.influencers_gs_view`)
  AND country_from != 'Россия'
  GROUP BY 1,2
  UNION ALL
  SELECT -- транзакции по интеграциям российских блогеров без промокодов
    DATE_TRUNC(DATE(s.date),MONTH) AS month,
     CASE   --страна расхода по промокоду
      WHEN s.country_to IN ('Испания') THEN 'Испания' 
      WHEN s.country_to IN ('Сербия') THEN 'Сербия'
      WHEN s.country_to IN ('Турция') THEN 'Турция'
      WHEN s.country_to IN ('Грузия') THEN 'Грузия'
      WHEN s.country_to IN ('Казахстан') THEN 'Казахстан'
      WHEN s.country_to IN ('Беларусь') THEN 'Беларусь'
      WHEN s.country_to IN ('Объединенные Арабские Эмираты') THEN 'Объединенные Арабские Эмираты'
      WHEN s.country_to IN ('Россия') THEN 'Россия'
      WHEN s.country_to IN ('Армения') THEN 'Армения'
      WHEN s.country_to IN ('Великобритания') THEN 'Великобритания'
      WHEN s.country_to IN ('Польша') THEN 'Польша'
      WHEN s.country_to IN ('Турция') THEN 'Турция'
      WHEN s.country_to IN ('Франция') THEN 'Франция'
      WHEN s.country_to IN ('Украина') THEN 'Украина'
      WHEN s.country_to IN ('Узбекистан') THEN 'Узбекистан'
      WHEN s.country_to IN ('Израиль') THEN 'Израиль'
      WHEN s.country_to IN ('Латвия') THEN 'Латвия'
      WHEN s.country_to IN ('Эстония') THEN 'Эстония'
      WHEN s.country_to IN ('Чехия') THEN 'Чехия'
      WHEN s.country_to IN ('Литва') THEN 'Литва'
      WHEN s.country_to IN ('Кипр') THEN 'Кипр'
      WHEN s.country_to IN ('Кыргызстан') THEN 'Кыргызстан'
      ELSE 'other countries'
      END AS country_to, 
    COUNT(DISTINCT s.transactionid) AS purchases,
    COUNT(DISTINCT IF(s.is_first_purchase = 1, s.transactionid,NULL)) AS first_purchases,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT (IF(s.is_first_purchase = 1, user_id, NULL))) AS new_users,
    SUM(s.purchase_sum_rub) AS revenue,
    0 AS cost
  FROM `funnel-flowwow.BUSINESS_DM.cm_date_source_medium_campaign_cities_categories_platform_transactions` s
  LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON s.transactionid = y.purchase_id AND s.product_id = y.product_id
  WHERE LOWER(campaign) IN (SELECT LOWER(campaign) FROM `funnel-flowwow.BUSINESS_DM.influencers_gs_view`)
  AND s.promocode IN ('') AND s.country_from != 'Россия'
  GROUP BY 1,2
  )
  SELECT
    month,
    country_to AS country,
    SUM(users) AS users_russian_blogers,
    SUM(new_users) AS new_users_russian_blogers,
    SUM(purchases) AS purchases_russian_blogers,
    SUM(first_purchases) AS first_purchases_russian_blogers,
    SUM(revenue) AS revenue_russian_blogers,
    SUM(cost) AS cost_russian_blogers,
    SAFE_DIVIDE(SUM(cost) , SUM(new_users)) AS CAC_new_users_russian_blogers,
    SAFE_DIVIDE(SUM(revenue) , SUM(new_users)) AS LTV_new_users_russian_blogers
    FROM g
    GROUP BY 1,2
    ORDER BY month ASC 
  ) AS h USING (month,country)
  ORDER BY month ASC
