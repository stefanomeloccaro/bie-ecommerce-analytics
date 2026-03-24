-- fct_orders → 1 row per order_id
SELECT order_id, COUNT() AS cnt
FROM `bie-portfolio1.ecommerce.fct_orders`
GROUP BY order_id
HAVING COUNT()  1;


-- fct_order_lines → 1 row per order_id + order_item_id
SELECT order_id, order_item_id, COUNT() AS cnt
FROM `bie-portfolio1.ecommerce.fct_order_lines`
GROUP BY order_id, order_item_id
HAVING COUNT()  1;


-- fct_order_payments → 1 row per order_id + payment_sequential
SELECT order_id, payment_sequential, COUNT() AS cnt
FROM `bie-portfolio1.ecommerce.fct_order_payments`
GROUP BY order_id, payment_sequential
HAVING COUNT()  1;


-- dim_customers → unique customer_id
SELECT customer_id, COUNT() AS cnt
FROM `bie-portfolio1.ecommerce.dim_customers`
GROUP BY customer_id
HAVING COUNT()  1;
