-- Negative values check
SELECT *
FROM `bie-portfolio1.ecommerce.fct_order_lines`
WHERE price < 0 OR freight_value < 0;


-- Orders without lines ***
SELECT *
FROM `bie-portfolio1.ecommerce.fct_orders`
WHERE has_order_lines = FALSE;


-- Orders without payments ***
SELECT *
FROM `bie-portfolio1.ecommerce.fct_orders`
WHERE has_payment = FALSE;


-- Delivery before purchase (data issue)
SELECT *
FROM `bie-portfolio1.ecommerce.fct_orders`
WHERE order_delivered_customer_date < order_purchase_date;