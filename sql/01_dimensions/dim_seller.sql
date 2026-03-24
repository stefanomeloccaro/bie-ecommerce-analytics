-- Dim_Sellers
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.dim_sellers` AS
SELECT DISTINCT
  s.seller_id,
  SAFE_CAST(s.seller_zip_code_prefix AS INT64) AS seller_zip_code_prefix,
  INITCAP(TRIM(s.seller_city)) AS seller_city,
  UPPER(TRIM(s.seller_state)) AS seller_state,
  g.city AS seller_geo_city,
  g.state_code AS seller_geo_state_code,
  g.state_name AS seller_state_name,
  g.country_name AS seller_country,
  g.latitude AS seller_latitude,
  g.longitude AS seller_longitude
FROM `bie-portfolio1.ecommerce.raw_sellers` s
LEFT JOIN `bie-portfolio1.ecommerce.dim_geolocation` g
  ON SAFE_CAST(s.seller_zip_code_prefix AS INT64) = g.zip_code_prefix
WHERE s.seller_id IS NOT NULL;


-- Dim_Dates
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.dim_dates` AS
WITH purchase_bounds AS (
  SELECT
    MIN(DATE(TIMESTAMP(order_purchase_timestamp))) AS min_purchase_date,
    MAX(DATE(TIMESTAMP(order_purchase_timestamp))) AS max_purchase_date
  FROM `bie-portfolio1.ecommerce.raw_orders`
),
shipping_bounds AS (
  SELECT
    MIN(DATE(TIMESTAMP(shipping_limit_date))) AS min_shipping_date,
    MAX(DATE(TIMESTAMP(shipping_limit_date))) AS max_shipping_date
  FROM `bie-portfolio1.ecommerce.raw_order_items`
),
boundaries AS (
  SELECT
    LEAST(min_purchase_date, min_shipping_date) AS min_date,
    GREATEST(max_purchase_date, max_shipping_date) AS max_date
  FROM purchase_bounds
  CROSS JOIN shipping_bounds
),
calendar AS (
  SELECT day AS date_day
  FROM boundaries,
  UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS day
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS date_key,
  date_day,
  EXTRACT(YEAR FROM date_day) AS year_num,
  EXTRACT(QUARTER FROM date_day) AS quarter_num,
  EXTRACT(MONTH FROM date_day) AS month_num,
  FORMAT_DATE('%B', date_day) AS month_name,
  FORMAT_DATE('%Y-%m', date_day) AS year_month,
  EXTRACT(WEEK FROM date_day) AS week_num,
  EXTRACT(DAY FROM date_day) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week_num,
  FORMAT_DATE('%A', date_day) AS day_of_week_name,
  CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar;