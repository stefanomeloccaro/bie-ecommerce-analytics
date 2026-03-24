
--------------------------------------------------------------------------------------------------
-- FACT MAIN: fct_orders
-- Grain: 1 row per order_id
--------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.fct_orders`
PARTITION BY order_purchase_date
CLUSTER BY customer_id, order_status AS
WITH order_base AS (
  SELECT
    o.order_id,
    o.customer_id,
    TRIM(o.order_status) AS order_status,

    TIMESTAMP(o.order_purchase_timestamp) AS order_purchase_timestamp,
    TIMESTAMP(o.order_approved_at) AS order_approved_at,
    TIMESTAMP(o.order_delivered_carrier_date) AS order_delivered_carrier_timestamp,
    TIMESTAMP(o.order_delivered_customer_date) AS order_delivered_customer_timestamp,
    TIMESTAMP(o.order_estimated_delivery_date) AS order_estimated_delivery_timestamp,

    DATE(TIMESTAMP(o.order_purchase_timestamp)) AS order_purchase_date,
    DATE(TIMESTAMP(o.order_approved_at)) AS order_approved_date,
    DATE(TIMESTAMP(o.order_delivered_carrier_date)) AS order_delivered_carrier_date,
    DATE(TIMESTAMP(o.order_delivered_customer_date)) AS order_delivered_customer_date,
    DATE(TIMESTAMP(o.order_estimated_delivery_date)) AS order_estimated_delivery_date
  FROM `bie-portfolio1.ecommerce.raw_orders` o
  WHERE o.order_id IS NOT NULL
    AND o.customer_id IS NOT NULL
),

line_agg AS (
  SELECT
    oi.order_id,
    COUNT(*) AS items_count,
    COUNT(DISTINCT oi.product_id) AS distinct_products_count,
    COUNT(DISTINCT oi.seller_id) AS distinct_sellers_count,
    SUM(SAFE_CAST(oi.price AS NUMERIC)) AS items_revenue,
    SUM(SAFE_CAST(oi.freight_value AS NUMERIC)) AS freight_total,
    SUM(SAFE_CAST(oi.price AS NUMERIC) + SAFE_CAST(oi.freight_value AS NUMERIC)) AS order_gross_value
  FROM `bie-portfolio1.ecommerce.raw_order_items` oi
  WHERE oi.order_id IS NOT NULL
  GROUP BY oi.order_id
),

payment_agg AS (
  SELECT
    op.order_id,
    COUNT(*) AS payment_lines_count,
    COUNT(DISTINCT TRIM(LOWER(op.payment_type))) AS distinct_payment_types_count,
    MAX(SAFE_CAST(op.payment_sequential AS INT64)) AS max_payment_sequential,
    MAX(SAFE_CAST(op.payment_installments AS INT64)) AS max_installments,
    SUM(SAFE_CAST(op.payment_value AS NUMERIC)) AS payment_value_total
  FROM `bie-portfolio1.ecommerce.raw_order_payments` op
  WHERE op.order_id IS NOT NULL
  GROUP BY op.order_id
),

review_agg AS (
  SELECT
    r.order_id,
    COUNT(*) AS reviews_count,
    AVG(SAFE_CAST(r.review_score AS NUMERIC)) AS avg_review_score,
    MAX(SAFE_CAST(r.review_score AS INT64)) AS max_review_score,
    MIN(SAFE_CAST(r.review_score AS INT64)) AS min_review_score
  FROM `bie-portfolio1.ecommerce.raw_order_reviews` r
  WHERE r.order_id IS NOT NULL
  GROUP BY r.order_id
)

SELECT
  ob.order_id,
  ob.customer_id,
  ob.order_status,

  ob.order_purchase_timestamp,
  ob.order_approved_at,
  ob.order_delivered_carrier_timestamp,
  ob.order_delivered_customer_timestamp,
  ob.order_estimated_delivery_timestamp,

  ob.order_purchase_date,
  ob.order_approved_date,
  ob.order_delivered_carrier_date,
  ob.order_delivered_customer_date,
  ob.order_estimated_delivery_date,

  CAST(FORMAT_DATE('%Y%m%d', ob.order_purchase_date) AS INT64) AS order_purchase_date_key,
  CAST(FORMAT_DATE('%Y%m%d', ob.order_approved_date) AS INT64) AS order_approved_date_key,
  CAST(FORMAT_DATE('%Y%m%d', ob.order_delivered_carrier_date) AS INT64) AS order_delivered_carrier_date_key,
  CAST(FORMAT_DATE('%Y%m%d', ob.order_delivered_customer_date) AS INT64) AS order_delivered_customer_date_key,
  CAST(FORMAT_DATE('%Y%m%d', ob.order_estimated_delivery_date) AS INT64) AS order_estimated_delivery_date_key,

  COALESCE(la.items_count, 0) AS items_count,
  COALESCE(la.distinct_products_count, 0) AS distinct_products_count,
  COALESCE(la.distinct_sellers_count, 0) AS distinct_sellers_count,
  COALESCE(la.items_revenue, 0) AS items_revenue,
  COALESCE(la.freight_total, 0) AS freight_total,
  COALESCE(la.order_gross_value, 0) AS order_gross_value,

  COALESCE(pa.payment_lines_count, 0) AS payment_lines_count,
  COALESCE(pa.distinct_payment_types_count, 0) AS distinct_payment_types_count,
  COALESCE(pa.max_payment_sequential, 0) AS max_payment_sequential,
  COALESCE(pa.max_installments, 0) AS max_installments,
  COALESCE(pa.payment_value_total, 0) AS payment_value_total,

  COALESCE(ra.reviews_count, 0) AS reviews_count,
  ra.avg_review_score,
  ra.max_review_score,
  ra.min_review_score,

  DATE_DIFF(ob.order_approved_date, ob.order_purchase_date, DAY) AS days_to_approval,
  DATE_DIFF(ob.order_delivered_carrier_date, ob.order_purchase_date, DAY) AS days_to_carrier,
  DATE_DIFF(ob.order_delivered_customer_date, ob.order_purchase_date, DAY) AS days_to_customer_delivery,
  DATE_DIFF(ob.order_estimated_delivery_date, ob.order_purchase_date, DAY) AS estimated_delivery_lead_days,
  DATE_DIFF(ob.order_delivered_customer_date, ob.order_estimated_delivery_date, DAY) AS delivery_delay_days,

  CASE
    WHEN ob.order_delivered_customer_date IS NOT NULL
     AND ob.order_estimated_delivery_date IS NOT NULL
     AND ob.order_delivered_customer_date <= ob.order_estimated_delivery_date
    THEN TRUE
    ELSE FALSE
  END AS delivered_on_time,

  CASE
    WHEN COALESCE(la.order_gross_value, 0) > 0 THEN TRUE
    ELSE FALSE
  END AS has_order_lines,

  CASE
    WHEN COALESCE(pa.payment_value_total, 0) > 0 THEN TRUE
    ELSE FALSE
  END AS has_payment,

  COALESCE(pa.payment_value_total, 0) - COALESCE(la.order_gross_value, 0) AS payment_order_gap
FROM order_base ob
LEFT JOIN line_agg la
  ON ob.order_id = la.order_id
LEFT JOIN payment_agg pa
  ON ob.order_id = pa.order_id
LEFT JOIN review_agg ra
  ON ob.order_id = ra.order_id;

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

--------------------------------------------------------------------------------------------------
-- SUPPORT FACT: fct_order_payments
-- Grain: 1 row per order_id + payment_sequential
--------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.fct_order_payments`
CLUSTER BY payment_type, order_id AS
WITH base AS (
  SELECT
    FARM_FINGERPRINT(CONCAT(CAST(op.order_id AS STRING), '-', CAST(op.payment_sequential AS STRING))) AS payment_line_key,
    op.order_id,
    SAFE_CAST(op.payment_sequential AS INT64) AS payment_sequential,
    TRIM(LOWER(op.payment_type)) AS payment_type,
    SAFE_CAST(op.payment_installments AS INT64) AS payment_installments,
    SAFE_CAST(op.payment_value AS NUMERIC) AS payment_value
  FROM `bie-portfolio1.ecommerce.raw_order_payments` op
  WHERE op.order_id IS NOT NULL
    AND op.payment_sequential IS NOT NULL
    AND op.payment_value IS NOT NULL
    AND SAFE_CAST(op.payment_value AS NUMERIC) > 0
)
SELECT
  payment_line_key,
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
FROM base;