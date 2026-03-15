/*
==============================================================================
Product Report
==============================================================================

Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
        - total orders
        - total sales
        - total quantity sold
        - total customers (unique)
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last sale)
        - average order revenue (AOR)
        - average monthly revenue

==============================================================================
*/



create view gold.report_products as 

with base_query as(
/*
-------------------------------------------------------------------
1)Base query:getting all the required columns or feilds
-------------------------------------------------------------------
*/
select
p.product_key ,
p.product_id as product_id,
p.product_number as product_number,
p.product_name as product_name,
p.category as category,
p.subcategory as subcategory,
p.cost as cost,
f.order_number as order_number,
f.customer_key as customer_key,
f.order_date as order_date,
f.sales_amount as sales_amount,
f.quantity as quantity
from gold.fact_sales f
left join gold.dim_products p
on p.product_key=f.product_key
where f.order_date is not null)

, product_aggregation as(
select 
product_key,
product_id,
product_number,
product_name,
category,
subcategory,
cost,
count(distinct order_number) as total_orders,
count(distinct customer_key) as total_customers,
MAX(order_date) as last_order_date,
DATEDIFF(MONTH,min(order_date),MAX(order_date)) as lifespan,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity
from base_query
group by product_key,
product_id,
product_number,
product_name,
category,
subcategory,
cost)

select 
product_key,
product_id,
product_number,
product_name,
category,
subcategory,
cost,
total_orders,
total_customers,
last_order_date,
--recency
DATEDIFF(month,last_order_date,getdate()) as recency,
lifespan,
total_sales,
case 
     when total_sales>50000 then 'High performers'
     when total_sales>=10000 then 'Mid range'
     else 'low_performers'
end as product_segment,
total_quantity,
--average order revenue
case when total_orders=0 then 0
     else total_sales/total_orders
end as average_order_revenue,
--average monthly revenue
case when lifespan =0 then total_sales
     else total_sales/lifespan
end as average_monthly_revenue
from product_aggregation;


select * from gold.report_products
