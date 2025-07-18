-- ***********************
-- Customer Report
-- ***********************
/*
1. Gathers essential information like names, ages and transaction details.
2. Segments customers into categories (VIP, Regular and New) and age groups.
3. Aggregates customer-level metrices.
	- total orders
	- total sales
    - total quantity purchased
    - total products
    - lifespan
4. Calculate valuable KPIs:
	- recency
    - average order value
    - average monthly spend
*/
create view gold.report_customer as 
with base_query as(
-- Retrives core columns from the table
select
f.order_number,
f.product_key,
f.sales_amount,
f.order_date,
f.quantity,
c.customer_key,
c.customer_number,
concat(first_name,' ',last_name) as customer_name,
timestampdiff(year, c.birthdate, now()) as age
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where order_date is not null),
customer_aggretation as(
select
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order,
TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) as lifespan
from base_query
group by 1,2,3,4)
select 
customer_key,
customer_number,
customer_name,
age,
case
	when age<20 then 'Under 20'
    when age between 20 and 29 then '20-29'
    when age between 30 and 39 then '30-39'
    when age between 40 and 49 then '40-49'
    else '50 and above'
end as age_group,
case
		when lifespan>=12 and total_sales>5000 then 'VIP'
		when lifespan>=12 and total_sales<=5000 then 'Regular'
		else 'New'
	end as customer_segment,
timestampdiff(month, last_order, now()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan, 
-- Compute average order values
case
	when total_orders = 0 then 0
	else round(total_sales/total_orders,2)
end as avg_order_value,
-- Compute average monthly spend
case
	when lifespan = 0 then total_sales
    else round(total_sales/lifespan,2)
end as avg_monthly_spend
from customer_aggretation
;
