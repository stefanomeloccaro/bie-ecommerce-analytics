--------------------------------------------------------------------------------------------------
-- SUPPORT FACT: fct_order_lines
-- Grain: 1 row per order_id + order_item_id
--------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.fct_order_lines`
PARTITION BY order_purchase_date
CLUSTER BY product_id, seller_id, order_status AS
WITH base AS (
  SELECT
    FARM_FINGERPRINT(CONCAT(CAST(oi.order_id AS STRING), '-', CAST(oi.order_item_id AS STRING))) AS order_line_key,
    oi.order_id,
    SAFE_CAST(oi.order_item_id AS INT64) AS order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,

    TRIM(o.order_status) AS order_status,

    TIMESTAMP(o.order_purchase_timestamp) AS order_purchase_timestamp,
    DATE(TIMESTAMP(o.order_purchase_timestamp)) AS order_purchase_date,

    TIMESTAMP(oi.shipping_limit_date) AS shipping_limit_timestamp,
    DATE(TIMESTAMP(oi.shipping_limit_date)) AS shipping_limit_date,

    SAFE_CAST(oi.price AS NUMERIC) AS price,
    SAFE_CAST(oi.freight_value AS NUMERIC) AS freight_value,
    SAFE_CAST(oi.price AS NUMERIC) + SAFE_CAST(oi.freight_value AS NUMERIC) AS line_total_value
  FROM `bie-portfolio1.ecommerce.raw_order_items` oi
  INNER JOIN `bie-portfolio1.ecommerce.raw_orders` o
    ON oi.order_id = o.order_id
  WHERE oi.order_id IS NOT NULL
    AND oi.order_item_id IS NOT NULL
    AND o.customer_id IS NOT NULL
    AND oi.product_id IS NOT NULL
    AND oi.seller_id IS NOT NULL
)
SELECT
  order_line_key,
  order_id,
  order_item_id,
  customer_id,
  product_id,
  seller_id,
  order_status,

  order_purchase_timestamp,
  order_purchase_date,
  CAST(FORMAT_DATE('%Y%m%d', order_purchase_date) AS INT64) AS order_purchase_date_key,

  shipping_limit_timestamp,
  shipping_limit_date,
  CAST(FORMAT_DATE('%Y%m%d', shipping_limit_date) AS INT64) AS shipping_limit_date_key,

  FORMAT_DATE('%Y%m', order_purchase_date) AS purchase_yyyymm,
  FORMAT_DATE('%Y%m', shipping_limit_date) AS shipping_yyyymm,

  price,
  freight_value,
  line_total_value
FROM base;