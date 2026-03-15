use DataWarehouse;
go

/*
=============================================
customer report
=============================================
This report consolidates key customer metrics and behaviors

Highlights:
  1.Gathers essential fields such as names,ages, and transaction details.
  2.Segments customers into categories(vip,regilar,new) and age groups.
  3.Aggregates customer level metrics:
    -total orders
    -total sales
    -total quantity purchased
    -total products
    -lifespan(in months)
 4.Calculate valuable KPIs:
    -recency (months since last order)
    -average order value
    -average monthly spend
==================================================
*/


create view gold.report_customers as
with base_query as(
/*
---------------------------------------------------
1)Base query: retreive core columns from tables
---------------------------------------------------
*/
select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ',c.last_name) as customer_name,
DATEDIFF(year,c.birthdate,GETDATE()) as age
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
where order_date is not null)



,customer_aggregation as(
/*
-----------------------------------------------
2)customer aggregation: summarize key metrics at customer level
------------------------------------------------
*/
select 
customer_key,
customer_number,
customer_name,
age,
COUNT(distinct order_number) as total_orders,
SUM(sales_amount) as total_sales,
SUM(quantity) as total_quantity,
COUNT( distinct product_key) as total_products,
MAX(order_date) as last_order_date,
DATEDIFF(month,MIN(order_date),max(order_date)) as lifespan
from base_query
group by customer_key,
customer_number,
customer_name,
age)

select 
customer_key,
customer_number,
customer_name,
age,
case 
     when age<20 then 'under 20'
     when age between 20 and 29 then '20-29'
     when age between 30 and 39 then '30-39'
     when age between 40 and 49 then '40-49'
     else '50 and above'
end as age_group,
case 
     when lifespan>=12 and total_sales > 5000 then 'vip'
     when lifespan>=12 and total_sales<=5000 then 'regular'
     else 'new'
end as customer_segment,
total_orders,
total_sales,
total_quantity,
total_products,
last_order_date,
--recency
DATEDIFF(MONTH,last_order_date,GETDATE()) as recency,
lifespan,
--compute average order value(avo)
case when total_sales=0 then 0
     else total_sales/total_orders
end as avg_order_value,
--compute average monthly spend
case when lifespan=0 then total_sales
     else total_sales/lifespan
end as avg_monthly_spend
from customer_aggregation;


