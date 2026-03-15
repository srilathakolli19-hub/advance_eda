
--performance analysis
--analyze the yearly performamce of products by comparing their sales with the average sales of products and their previous years sales

WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year,
        p.product_name as product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    where f.order_date is not null
    GROUP BY p.product_name,YEAR(f.order_date)
)



select 
order_year,
product_name,
current_sales,
AVG(current_sales) over (partition by product_name) as avg_sales,
current_sales-AVG(current_sales) over (partition by product_name) as diff_avg,
case when current_sales-AVG(current_sales) over (partition by product_name) > 0 then 'above average'
     when current_sales-AVG(current_sales) over (partition by product_name) < 0 then 'below average'
     else 'avg'
end avg_change,
LAG(current_sales) over (partition by product_name order by order_year) py_sales,
current_sales-lag(current_sales) over (partition by product_name order by order_year) as diff_py,
case when current_sales-lag(current_sales) over (partition by product_name order by order_year) > 0 then 'increase'
     when current_sales-lag(current_sales) over (partition by product_name order by order_year) < 0 then 'decrease'
     else 'no change'
end py_change
from yearly_product_sales
order by product_name,order_year;
