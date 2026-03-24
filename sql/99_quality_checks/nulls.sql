-- Null critical fields
SELECT
  COUNTIF(order_id IS NULL) AS null_order_id,
  COUNTIF(customer_id IS NULL) AS null_customer_id,
  COUNTIF(order_purchase_date IS NULL) AS null_purchase_date
FROM `bie-portfolio1.ecommerce.fct_orders`;


SELECT
  COUNTIF(product_id IS NULL) AS null_product,
  COUNTIF(price IS NULL) AS null_price
FROM `bie-portfolio1.ecommerce.fct_order_lines`;