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