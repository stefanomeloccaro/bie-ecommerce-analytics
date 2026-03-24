-- Product integrity
SELECT COUNT(*) AS missing_products
FROM `bie-portfolio1.ecommerce.fct_order_lines` f
LEFT JOIN `bie-portfolio1.ecommerce.dim_products` p
  ON f.product_id = p.product_id
WHERE p.product_id IS NULL;


-- Customer integrity
SELECT COUNT(*) AS missing_customers
FROM `bie-portfolio1.ecommerce.fct_orders` f
LEFT JOIN `bie-portfolio1.ecommerce.dim_customers` c
  ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;


-- Seller integrity
SELECT COUNT(*) AS missing_sellers
FROM `bie-portfolio1.ecommerce.fct_order_lines` f
LEFT JOIN `bie-portfolio1.ecommerce.dim_sellers` s
  ON f.seller_id = s.seller_id
WHERE s.seller_id IS NULL;


-- Date integrity
SELECT COUNT(*) AS missing_dates
FROM `bie-portfolio1.ecommerce.fct_orders` f
LEFT JOIN `bie-portfolio1.ecommerce.dim_dates` d
  ON f.order_purchase_date_key = d.date_key
WHERE d.date_key IS NULL;