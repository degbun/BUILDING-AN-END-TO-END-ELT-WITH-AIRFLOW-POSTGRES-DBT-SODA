-- dim_product.sql
-- StockCode isn't unique, a product with the same id can have different and prices
-- Create the dimension table
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['stockcode', 'description', 'price']) }} as product_id,
		stockcode AS stock_code,
    description AS description,
    price AS price
FROM sales
WHERE stockcode IS NOT NULL
AND Price > 0