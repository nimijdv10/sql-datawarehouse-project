-- ***********************
-- Data Segmentation
-- ***********************
-- Segment products into cost ranges and count how many products fall into each category
with product_segments as(
  select
  product_key
  product_name,
  product_cost,
  case
  	when product_cost<100 then 'Below 100'
      when product_cost between 100 and 500 then '100-500'
      when product_cost between 500 and 1000 then '500-1000'
      else 'Above 1000'
  end as cost_range
  from gold.dim_products
)
select
  cost_range, 
  count(*) as total_products
from product_segments
group by cost_range
order by total_products desc;

/*
Group customers into 3 segments based on their spending behavior. (VIP, Regular, New)
VIP - at least 12 months of history and spending more than 5000
Regular - at least 12 months of history and spending less than or equal to 5000
New - lifespan less than 12 months
*/
with customer_spending as(
  select 
  c.customer_key,
  sum(s.sales_amount) as total_sales,
  MIN(order_date) as first_order,
  MAX(order_date) as last_order,
  TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) as lifespan -- calculating the lifespan by using the difference between the first order placed and the last order placed
  from gold.fact_sales s
  left join gold.dim_customers c
  on s.customer_key = c.customer_key
  group by s.customer_key
  )
select
customer_segment,
count(customer_key) as total_customers
from(
	select 
	customer_key, 
	lifespan,
	case
		when lifespan>=12 and total_sales>5000 then 'VIP'
		when lifespan>=12 and total_sales<=5000 then 'Regular'
		else 'New'
	end as customer_segment
	from customer_spending) t
group by customer_segment
order by total_customers desc;
