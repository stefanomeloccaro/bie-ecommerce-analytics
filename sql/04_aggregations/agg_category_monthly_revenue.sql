
--------------------------------------------------------------------------------------------------
-- Aggregated physical table: Category Monthly Revenue
-- Grain: month + category
-- Source: fct_order_lines
--
-- Portfolio note:
-- - Partition by month_start_date for pruning by time
-- - Cluster by product_category_name_english for category filtering
--------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.agg_category_monthly_revenue`
PARTITION BY month_start_date
CLUSTER BY product_category_name_english AS
SELECT
  DATE_TRUNC(f.order_purchase_date, MONTH) AS month_start_date,
  CAST(FORMAT_DATE('%Y%m%d', DATE_TRUNC(f.order_purchase_date, MONTH)) AS INT64) AS month_start_date_key,
  FORMAT_DATE('%Y-%m', DATE_TRUNC(f.order_purchase_date, MONTH)) AS year_month,
  EXTRACT(YEAR FROM f.order_purchase_date) AS year_num,
  EXTRACT(QUARTER FROM f.order_purchase_date) AS quarter_num,
  EXTRACT(MONTH FROM f.order_purchase_date) AS month_num,

  COALESCE(p.product_category_name_english, 'Unknown') AS product_category_name_english,

  SUM(f.line_total_value) AS revenue,
  SUM(f.price) AS items_revenue,
  SUM(f.freight_value) AS freight_revenue,
  COUNT(DISTINCT f.order_id) AS orders_count,
  COUNT(*) AS order_lines_count,
  COUNT(DISTINCT f.product_id) AS distinct_products_count,
  COUNT(DISTINCT f.customer_id) AS distinct_customers_count,
  COUNT(DISTINCT f.seller_id) AS distinct_sellers_count,
  SAFE_DIVIDE(SUM(f.line_total_value), COUNT(DISTINCT f.order_id)) AS avg_order_value
FROM `bie-portfolio1.ecommerce.fct_order_lines` f
LEFT JOIN `bie-portfolio1.ecommerce.dim_products` p
  ON f.product_id = p.product_id
GROUP BY
  month_start_date,
  month_start_date_key,
  year_month,
  year_num,
  quarter_num,
  month_num,
  product_category_name_english;