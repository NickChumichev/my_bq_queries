WITH a AS( -- соединил тематическую подборку с сео группой, получил страну тематической подборки
  SELECT
    n.id AS selection_id,
    n.name AS selection_name,
    n.date_begin,
    n.date_end,
    s.name_stemmed AS seo_group_name,
    es.id_country,
    y.country
FROM `funnel-flowwow.BUSINESS_DM.f_thematic_selection` n 
  RIGHT JOIN `funnel-flowwow.BUSINESS_DM.f_seo_groups` s ON n.id_seo_groups = s.id
  LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_thematic_selection_counties` es ON n.id = es.id_thematic_selection 
  LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_country` y ON es.id_country = y.country_id
  WHERE  n.id IS NOT NULL
)
,b AS ( --разделил названия сео групп по словам
  SELECT
    selection_id,
    id_country,
    country,
    selection_name,
    date_begin,
    date_end,
    SPLIT(seo_group_name,' ') AS seo_group_name
  FROM a
)
,c AS ( -- распарсил массивы c названиями сео групп
  SELECT
    selection_id,
    selection_name,
    id_country,
    country,
    date_begin,
    date_end,
    seo_group_name
  FROM b,
    UNNEST(seo_group_name) AS seo_group_name
)
,d AS ( -- вывел id категорий, местонахождение магазинов
  SELECT
    y.id AS category_id,
    y.name AS category_name, --префиксы сео группы магазина
    y.shop_id,
    p.city_id, -- город нахождения магазина
    ry.country_id, -- страна нахождения магазина
    ry.country,
    y.created_at --дата создания категории
  FROM `funnel-flowwow.MYSQL_EXPORT.f_shop_thematic_category` y 
  LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_shop` p ON y.shop_id = p.id 
  LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_city_new` w ON p.city_id = w.id
  LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_country` ry ON ry.country_id = w.country_id
)
,e AS ( -- соединил сео группы с id категориями магазинов
SELECT
  DISTINCT
    c.selection_id,
    c.selection_name,
    d.country_id,
    d.country,
    c.date_begin,
    c.date_end,
    d.category_id,
    d.category_name,
    d.shop_id AS shops, -- id магазинов
    d.created_at
FROM c
  LEFT JOIN d ON LOWER(d.category_name) LIKE CONCAT('%', LOWER(c.seo_group_name), '%') 
  AND d.country_id = c.id_country
WHERE DATE(d.created_at) BETWEEN DATE(c.date_begin) AND DATE(c.date_end) 
)
,f AS ( --соединил тематическую подборку с id товарных категорий
SELECT
    e.selection_id,
    e.selection_name,
    e.date_begin,
    e.date_end,
    e.shops,
    IF(DATE(y.created_at) BETWEEN DATE(e.date_begin) AND DATE(e.date_end),y.product_id,NULL) AS product_id
FROM e LEFT JOIN `funnel-flowwow.MYSQL_EXPORT.f_product_category` y ON e.category_id = y.category_id
WHERE y.product_id IN (SELECT product_id FROM `funnel-flowwow.MYSQL_EXPORT.f_property`) -- товары тематической подборки
)
, g AS ( -- добавил первые покупки, покупки, доход, кол-во уникальных купленных товаров
SELECT --тематических подборок уже 38 тут, а не 50, потому что в bonus_company не нашлись оплаченные товары по всем подборкам
    f.selection_id,
    f.selection_name,
    f.date_begin,
    f.date_end,
    COUNT(y.purchase_id) AS purchases, --оплаченные заказы
    COUNT(IF(y.is_first_purchase = 1,y.purchase_id, NULL)) AS first_purchases,
    SUM(y.product_price) AS product_revenue,
    COUNT(DISTINCT y.product_id) AS unique_paid_product_id, -- количество уникальных купленных товаров
FROM f LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON f.product_id = y.product_id 
WHERE DATE(y.purchase_timestamp) BETWEEN  DATE(f.date_begin) AND DATE(f.date_end)
GROUP BY 1,2,3,4
ORDER BY selection_id ASC
)
,h AS ( --добавил средний чек
SELECT 
    selection_id,
    selection_name,
    date_begin,
    date_end,
    purchases, 
    first_purchases,
    product_revenue,
    SAFE_DIVIDE(product_revenue , purchases) AS AOV, --средний чек
    unique_paid_product_id, 
FROM g
)
, j AS ( -- посчитал доход по товару и долю товара в подборке
SELECT
    f.selection_id,
    f.selection_name,
    f.product_id,
    y.product_name,
    y.product_url,
    SUM(y.product_price) / g.product_revenue AS product_share,
    SUM(y.product_price) AS  product_revenue
FROM f LEFT JOIN `funnel-flowwow.BUSINESS_DM.cm_product_purchases_bonus_company` y ON f.product_id = y.product_id
INNER JOIN  g ON g.selection_id = f.selection_id
WHERE DATE(y.purchase_timestamp) BETWEEN  DATE(f.date_begin) AND DATE(f.date_end)
GROUP BY 1,2,3,4,5,g.product_revenue
ORDER BY product_revenue DESC
)
, k AS ( -- посчитал топ 10 товара в каждой подборке
SELECT
  selection_id,
  selection_name,
  ARRAY_AGG(product_name LIMIT 10) AS top_10_product_name,
  ARRAY_AGG(product_url LIMIT 10) AS top_10_product_url,
  ARRAY_AGG(product_revenue LIMIT 10) AS top_10_product_revenue,
  ARRAY_AGG(product_share LIMIT 10) AS top_10_product_share
FROM j
GROUP BY 1,2
)
,l AS ( --количество продуктов и магазинов в подборке
SELECT
    selection_id,
    selection_name,
    COUNT(DISTINCT shops) AS cnt_shops, -- магазины, находящиеся в подборке
    COUNT(DISTINCT product_id) AS cnt_products -- продукты, находящиеся в подборке
FROM f
GROUP BY 1,2
)
SELECT
    h.selection_id,
    h.selection_name,
    h.date_begin,
    h.date_end,
    l.cnt_shops,
    l.cnt_products,
    h.purchases,
    SAFE_DIVIDE(h.purchases, l.cnt_products) AS purchases_to_products,
    h.first_purchases,
    h.product_revenue,
    h.AOV,
    h.unique_paid_product_id,
    k.top_10_product_name,
    k.top_10_product_url,
    k.top_10_product_revenue,
    k.top_10_product_share
FROM h INNER JOIN k ON h.selection_id = k.selection_id
INNER JOIN l ON h.selection_id = l.selection_id
ORDER BY h.product_revenue DESC