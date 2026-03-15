
--cummulative analysis
--calculate total sales per month and the running total of sales over time

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
