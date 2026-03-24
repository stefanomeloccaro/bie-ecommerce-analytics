-- Dim_Reviews
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.dim_reviews` AS
WITH base AS (
  SELECT
    r.review_id,
    r.order_id,
    SAFE_CAST(r.review_score AS INT64) AS review_score,
    TRIM(r.review_comment_title) AS review_comment_title,
    TRIM(r.review_comment_message) AS review_comment_message,

    TIMESTAMP(r.review_creation_date) AS review_creation_timestamp,
    TIMESTAMP(r.review_answer_timestamp) AS review_answer_timestamp,

    DATE(TIMESTAMP(r.review_creation_date)) AS review_creation_date,
    DATE(TIMESTAMP(r.review_answer_timestamp)) AS review_answer_date
  FROM `bie-portfolio1.ecommerce.raw_order_reviews` r
  WHERE r.review_id IS NOT NULL
    AND r.order_id IS NOT NULL
)
SELECT
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  review_creation_timestamp,
  review_answer_timestamp,
  review_creation_date,
  review_answer_date,

  CAST(FORMAT_DATE('%Y%m%d', review_creation_date) AS INT64) AS review_creation_date_key,
  CAST(FORMAT_DATE('%Y%m%d', review_answer_date) AS INT64) AS review_answer_date_key,

  CASE
    WHEN review_comment_title IS NOT NULL OR review_comment_message IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS has_written_comment,

  CASE
    WHEN review_score IN (1, 2) THEN 'Negative'
    WHEN review_score = 3 THEN 'Neutral'
    WHEN review_score IN (4, 5) THEN 'Positive'
    ELSE 'Unknown'
  END AS review_sentiment
FROM base;
