-- dim_datetime.sql

-- Create a CTE to extract date and time components
WITH datetime_cte AS (  
  SELECT DISTINCT
    invoicedate AS datetime_id,
    CASE
      WHEN LENGTH(CAST(invoicedate AS VARCHAR)) = 16 THEN
        -- Date format: "DD/MM/YYYY HH:MI"
        TO_TIMESTAMP(CAST(invoicedate as TEXT ), 'DD/MM/YYYY HH24:MI')
      WHEN LENGTH(CAST(invoicedate AS VARCHAR)) <= 14 THEN
        -- Date format: "MM/DD/YY HH:MI"

        TO_TIMESTAMP(CAST(invoicedate as TEXT ), 'MM/DD/YY HH24:MI')
      ELSE
        NULL
    END AS date_part
  FROM sales
  WHERE invoicedate IS NOT NULL
)
SELECT
  datetime_id,
  date_part AS datetime,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  EXTRACT(DAY FROM date_part) AS day,
  EXTRACT(HOUR FROM date_part) AS hour,
  EXTRACT(MINUTE FROM date_part) AS minute,
  EXTRACT(ISODOW FROM date_part) AS weekday
FROM datetime_cte
