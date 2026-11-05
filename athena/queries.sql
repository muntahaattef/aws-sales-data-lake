-- 1. Daily revenue and units
SELECT sale_date, SUM(revenue) AS revenue, SUM(quantity) AS units
FROM sales_lake.curated_sales
GROUP BY sale_date
ORDER BY sale_date;

-- 2. Highest-revenue products
SELECT product_id, SUM(revenue) AS revenue
FROM sales_lake.curated_sales
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- 3. Repeat customers across distinct sale dates
SELECT customer_id, COUNT(DISTINCT sale_date) AS active_days, SUM(revenue) AS revenue
FROM sales_lake.curated_sales
GROUP BY customer_id
HAVING COUNT(DISTINCT sale_date) > 1
ORDER BY revenue DESC;

-- 4. Average transaction revenue by day
SELECT sale_date, AVG(revenue) AS average_transaction_revenue
FROM sales_lake.curated_sales
GROUP BY sale_date
ORDER BY sale_date;

-- 5. Daily revenue with running total
WITH daily AS (
  SELECT sale_date, SUM(revenue) AS revenue
  FROM sales_lake.curated_sales
  GROUP BY sale_date
)
SELECT sale_date,
       revenue,
       SUM(revenue) OVER (ORDER BY sale_date ROWS UNBOUNDED PRECEDING) AS running_revenue
FROM daily
ORDER BY sale_date;

