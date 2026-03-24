-- Dim_Products
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.dim_products` AS
SELECT DISTINCT
  p.product_id,
  c.category_key,
  TRIM(p.product_category_name) AS product_category_name,
  c.product_category_name_english,
  SAFE_CAST(p.product_name_lenght AS INT64) AS product_name_lenght,
  SAFE_CAST(p.product_description_lenght AS INT64) AS product_description_lenght,
  SAFE_CAST(p.product_photos_qty AS INT64) AS product_photos_qty,
  SAFE_CAST(p.product_weight_g AS NUMERIC) AS product_weight_g,
  SAFE_CAST(p.product_length_cm AS NUMERIC) AS product_length_cm,
  SAFE_CAST(p.product_height_cm AS NUMERIC) AS product_height_cm,
  SAFE_CAST(p.product_width_cm AS NUMERIC) AS product_width_cm
FROM `bie-portfolio1.ecommerce.raw_products` p
LEFT JOIN `bie-portfolio1.ecommerce.dim_categories` c ON TRIM(p.product_category_name) = c.product_category_name
WHERE 1=1
  AND p.product_id IS NOT NULL;
