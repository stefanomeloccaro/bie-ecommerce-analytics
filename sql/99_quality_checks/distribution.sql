-- Order status distribution
SELECT order_status, COUNT(*) AS cnt
FROM `bie-portfolio1.ecommerce.fct_orders`
GROUP BY order_status
ORDER BY cnt DESC;


-- Review score distribution
SELECT review_score, COUNT(*) AS cnt
FROM `bie-portfolio1.ecommerce.dim_reviews`
GROUP BY review_score
ORDER BY review_score;


-- Revenue distribution
SELECT
  APPROX_QUANTILES(order_gross_value, 10) AS revenue_percentiles
FROM `bie-portfolio1.ecommerce.fct_orders`;
