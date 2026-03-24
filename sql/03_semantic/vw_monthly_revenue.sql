--------------------------------------------------------------------------------------------------
-- Semantic view: Monthly Revenue
-- Grain: month
-- Source: fct_orders
--------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `bie-portfolio1.ecommerce.vw_monthly_revenue` AS
SELECT
  d.year_num,
  d.quarter_num,
  d.month_num,
  d.month_name,
  d.year_month,
  SUM(f.order_gross_value) AS revenue,
  SUM(f.items_revenue) AS items_revenue,
  SUM(f.freight_total) AS freight_revenue,
  COUNT(DISTINCT f.order_id) AS orders_count,
  COUNT(DISTINCT f.customer_id) AS active_customers_count,
  SAFE_DIVIDE(SUM(f.order_gross_value), COUNT(DISTINCT f.order_id)) AS avg_order_value
FROM `bie-portfolio1.ecommerce.fct_orders` f
JOIN `bie-portfolio1.ecommerce.dim_dates` d
  ON f.order_purchase_date_key = d.date_key
GROUP BY
  d.year_num,
  d.quarter_num,
  d.month_num,
  d.month_name,
  d.year_month
ORDER BY
  d.year_num,
  d.month_num;