-- Revenue consistency (orders vs lines)
SELECT
  SUM(order_gross_value) AS total_orders_revenue,
  (
    SELECT SUM(line_total_value)
    FROM `bie-portfolio1.ecommerce.fct_order_lines`
  ) AS total_lines_revenue
FROM `bie-portfolio1.ecommerce.fct_orders`;


-- Payment vs Orders
SELECT
  SUM(order_gross_value) AS orders_total,
  SUM(payment_value_total) AS payments_total,
  SUM(payment_order_gap) AS total_gap
FROM `bie-portfolio1.ecommerce.fct_orders`;
