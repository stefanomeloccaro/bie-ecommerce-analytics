--------------------------------------------------------------------------------------------------
-- Dim customer
--------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `bie-portfolio1.ecommerce.dim_customers` AS
SELECT DISTINCT
  c.customer_id,
  c.customer_unique_id,
  SAFE_CAST(c.customer_zip_code_prefix AS INT64) AS customer_zip_code_prefix,
  INITCAP(TRIM(c.customer_city)) AS customer_city,
  UPPER(TRIM(c.customer_state)) AS customer_state,
  CASE UPPER(TRIM(c.customer_state))
    WHEN 'AC' THEN 'Acre'
    WHEN 'AL' THEN 'Alagoas'
    WHEN 'AP' THEN 'Amapa'
    WHEN 'AM' THEN 'Amazonas'
    WHEN 'BA' THEN 'Bahia'
    WHEN 'CE' THEN 'Ceara'
    WHEN 'DF' THEN 'Distrito Federal'
    WHEN 'ES' THEN 'Espirito Santo'
    WHEN 'GO' THEN 'Goias'
    WHEN 'MA' THEN 'Maranhao'
    WHEN 'MT' THEN 'Mato Grosso'
    WHEN 'MS' THEN 'Mato Grosso do Sul'
    WHEN 'MG' THEN 'Minas Gerais'
    WHEN 'PA' THEN 'Para'
    WHEN 'PB' THEN 'Paraiba'
    WHEN 'PR' THEN 'Parana'
    WHEN 'PE' THEN 'Pernambuco'
    WHEN 'PI' THEN 'Piaui'
    WHEN 'RJ' THEN 'Rio de Janeiro'
    WHEN 'RN' THEN 'Rio Grande do Norte'
    WHEN 'RS' THEN 'Rio Grande do Sul'
    WHEN 'RO' THEN 'Rondonia'
    WHEN 'RR' THEN 'Roraima'
    WHEN 'SC' THEN 'Santa Catarina'
    WHEN 'SP' THEN 'Sao Paulo'
    WHEN 'SE' THEN 'Sergipe'
    WHEN 'TO' THEN 'Tocantins'
    ELSE 'Unknown'
  END AS customer_state_name,
  g.geolocation_city AS customer_geo_city,
  g.geolocation_state AS customer_geo_state_code,
  g.geolocation_lat AS customer_latitude,
  g.geolocation_lng AS customer_longitude
FROM `bie-portfolio1.ecommerce.raw_customers` c
LEFT JOIN `bie-portfolio1.ecommerce.raw_geolocation` g
  ON SAFE_CAST(c.customer_zip_code_prefix AS INT64) = g.geolocation_zip_code_prefix
WHERE c.customer_id IS NOT NULL;


