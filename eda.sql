use DataWarehouse;
go


--change over time analysis(months)

select
DATETRUNC(month,order_date) as order_date,
SUM(sales_amount) as total_sales,
COUNT(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date)
order by order_date


--change over time analysis(years)

select
DATETRUNC(year,order_date) as order_date,
SUM(sales_amount) as total_sales,
COUNT(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by DATETRUNC(year,order_date)
order by order_date

--cummulative analysis

select 
order_date,
total_sales,
SUM(total_sales) over (partition by datetrunc(year,order_date) order by order_date) as rolling_total_sales,
average_sales,
AVG(average_sales) over (partition by datetrunc(year,order_date) order by order_date) as rolling_average
from(
select
DATETRUNC(month,order_date) as order_date,
SUM(sales_amount) as total_sales,
AVG(sales_amount) as average_sales
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date))t


--performance analysis

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

