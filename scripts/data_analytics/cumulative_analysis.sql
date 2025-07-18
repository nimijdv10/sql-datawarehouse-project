-- ***********************
-- Cumulative Analysis
-- ***********************
-- Calculating the moving average for every month.
select
  order_date,
  total_sales,
  sum(total_sales) over(order by order_date) as running_total,
  floor(avg(avg_price) over(order by order_date)) as moving_avg
from (
  select
  DATE_FORMAT(order_date,'%Y-%m-01') as order_date, 
  sum(sales_amount) as total_sales,
  avg(price) as avg_price
  from gold.fact_sales
  where order_date is not null
  group by DATE_FORMAT(order_date,'%Y-%m-01')
) t;
