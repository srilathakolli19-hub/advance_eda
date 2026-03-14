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


--part to whole analysis
--which categories contribute most to the overall sales

with category_sales as (
select 
p.category as category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key=f.product_key
group by p.category)

select 
category,
total_sales,
SUM(total_sales) over() as overall_sales,
concat(round((cast(total_sales as float)/SUM(total_sales) over())*100 ,2),'%') as percentage_of_total
from category_sales
order by percentage_of_total desc;


--data segmentation
--segment products based on cost ranges and then group products to which segment they belong to
with product_segments as(
select
product_key,
product_name,
case when cost<100 then 'below 100'
     when cost between 100 and 500 then '100-500'
     when cost between 500 and 1000 then '500-1000'
     else 'above 1000'
end cost_range
from gold.dim_products)

select 
cost_range,
COUNT(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc;



--group customers into 3 segments as per their spending behavior:
--vip: customers with atleast 12 months of lifespan and spending more then 5000
--regular: customers with atleast 12 months lifespan and spending less than or equal to 5000
--new: customers with lifespan of less tha 12 months 
--and find the total number of customers among each group


with customer_spending as(
select
c.customer_key as customer_key,
sum(f.sales_amount) as total_sales,
min(f.order_date) as first_order,
MAX(f.order_date) as last_order,
datediff(month,min(f.order_date),max(f.order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key=f.customer_key
group by c.customer_key)

select
customer_segment,
count(customer_key) as total_customers
from 
(select
customer_key,
total_sales,
lifespan,
case when lifespan>=12 and total_sales>5000 then 'VIP'
     when lifespan>=12 and total_sales<=5000 then 'Regular'
     else 'New'
end customer_segment
from customer_spending)t
group by customer_segment;




