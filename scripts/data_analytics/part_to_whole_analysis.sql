-- ***********************
-- Part to Whole Analysis
-- ***********************
-- Which categories contributes the most to the overall sales?
/*
Analysis:
	- Bikes is the dominating category compared to accessories and clothing.
	- In reality, this type of distribution is problematic as it is dominating the sales by a huge percentage.
*/
with sales_category as(
  select
  p.category,
  sum(s.sales_amount) as total_sales
  from gold.dim_products p
  left join gold.fact_sales s
  on p.product_key = s.product_key
  where order_date is not null
  group by p.category
)
select
  category,
  total_sales,
  sum(total_sales) over() as overall_sales,
  concat(round(total_sales/sum(total_sales) over() * 100,2),'%') as perc_of_total
from sales_category
order by total_sales desc;
