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

