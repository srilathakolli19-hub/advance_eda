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

