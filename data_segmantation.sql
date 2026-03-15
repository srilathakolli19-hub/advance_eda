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
