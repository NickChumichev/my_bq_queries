WITH a AS ( --разделить _product_id_,_category_,_sub_category_
SELECT
  _event_name_, 
  _purchase_id_,
  SPLIT(_product_id_,',') AS _product_id_,
  -- SPLIT(_category_,',') AS _category_,
  -- SPLIT(_sub_category_,',') AS _sub_category_ 
  FROM `funnel-flowwow.ADJUST_RAW.clients_app` 
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP("2023-12-31") AND TIMESTAMP("2024-02-05") 
AND DATE(_created_at_) BETWEEN '2024-01-01' AND '2024-02-04'
AND LOWER(_network_name_) IN ("google ads aci","google ads ace") AND _event_name_ IN ('s2s_ecommerce_purchase_non_flowers_paid')
)
, b AS ( -- распарсить _product_id_,_category_,_sub_category_
SELECT
  _event_name_, 
  _product_id_,
  _purchase_id_,
  -- _category_,
  -- _sub_category_
FROM a,
  -- UNNEST(_category_) AS _category_
  -- UNNEST(_sub_category_) AS _sub_category_,
  UNNEST(_product_id_) AS _product_id_
)
SELECT  --соединить с названиями товаров, субкатегорий, категорий
  b._event_name_, 
  b._purchase_id_,
  b._product_id_,
  ve.name AS product_name,
  -- b._category_,
  -- p.name AS category_name,
  -- b._sub_category_,
  -- e.name AS sub_category_name,
FROM b 
  -- INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_range_group` p ON CAST(b._category_ AS STRING) = CAST(p.id AS STRING)
  -- INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_range_type` e ON CAST(b._sub_category_ AS STRING) = CAST(e.id AS STRING)
  INNER JOIN `funnel-flowwow.MYSQL_EXPORT.f_product` ve ON CAST(b._product_id_ AS STRING) = CAST(ve.id AS STRING)
 