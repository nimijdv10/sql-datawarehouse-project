-- ***********************
-- Product Report
-- ***********************
/*
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
*/
create view gold.report_product as
with base_query_prod as(
select
f.order_number,
f.order_date,
f.sales_amount,
f.quantity,
f.customer_key,
p.product_key,
p.product_number,
p.product_name,
p.category,
p.sub_category,
p.product_cost
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null),
product_aggregation as(
select
product_key,
product_number,
product_name,
category,
sub_category,
product_cost,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity,  
max(order_date) as last_order,
TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) as lifespan,
round(avg(sales_amount/quantity),0) as avg_selling_price
from base_query_prod
group by 
	product_number,
	product_name,
	category,
	sub_category,
    product_cost
)
select 
product_key,
product_name,
category,
sub_category,
product_cost,
case
	when total_sales>100000 then 'High Performer'
	when total_sales between 5000 and 99999 then 'Mid Range'
    else 'Low Performer'
end as product_perfromance,
lifespan,
TIMESTAMPDIFF(MONTH, last_order , now()) as recency,
total_orders,
total_sales,
total_customers,
avg_selling_price,
case
	when total_orders =0 then 0
    else floor(total_sales/total_orders)
end as avg_prod_sales,
case
	when lifespan = 0 then total_sales
    else floor(total_sales/lifespan)
end as avg_monthly_spend
from product_aggregation
;
