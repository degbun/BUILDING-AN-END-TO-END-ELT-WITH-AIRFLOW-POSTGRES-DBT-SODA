-- dim_customer.sql

-- Create the dimension table
WITH customer_cte AS (
	SELECT DISTINCT
	    {{ dbt_utils.generate_surrogate_key(['customerid', 'country']) }} as customer_id,
	    country AS country
	FROM sales
	WHERE customerid IS NOT NULL
)
SELECT
    t.*,
	cm.iso
FROM customer_cte t
LEFT JOIN country cm ON t.country = cm.nicename