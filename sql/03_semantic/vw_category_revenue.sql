--------------------------------------------------------------------------------------------------
-- Semantic view: Category Revenue
-- Grain: category
-- Source: fct_order_lines
--------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `bie-portfolio1.ecommerce.vw_category_revenue` AS
SELECT
  COALESCE(p.product_category_name_english, 'Unknown') AS product_category_name_english,
  SUM(f.line_total_value) AS revenue,
  SUM(f.price) AS items_revenue,
  SUM(f.freight_value) AS freight_revenue,
  COUNT(DISTINCT f.order_id) AS orders_count,
  COUNT(*) AS order_lines_count,
  COUNT(DISTINCT f.product_id) AS distinct_products_count,
  SAFE_DIVIDE(SUM(f.line_total_value), COUNT(DISTINCT f.order_id)) AS avg_order_value
FROM `bie-portfolio1.ecommerce.fct_order_lines` f
LEFT JOIN `bie-portfolio1.ecommerce.dim_products` p
  ON f.product_id = p.product_id
GROUP BY
  COALESCE(p.product_category_name_english, 'Unknown')
ORDER BY
  revenue DESC;