-- ***********************
-- Performance Analysis
-- ***********************
/*
Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales
*/
with yearly_product_sales as(
	select
	year(s.order_date) as order_year,
	p.product_name,
	sum(s.sales_amount) as current_sales
	from gold.fact_sales s
	left join gold.dim_products p
	on s.product_key = p.product_key
	where s.order_date is not null
	group by year(s.order_date), p.product_name
) 
select
	order_year,
	product_name,
	current_sales,
	floor(avg(current_sales) over (partition by product_name)) as avg_sales,
	current_sales - floor(avg(current_sales) over (partition by product_name)) as diff_avg,
	case
		when current_sales - floor(avg(current_sales) over (partition by product_name))>0 then 'Above Avg'
		when current_sales - floor(avg(current_sales) over (partition by product_name))<0 then 'Below Avg'
		else 'Avg'
	end as avg_change,  -- checking the current and average sales per year for each product
	-- year-over-year analysis
	lag(current_sales) over(partition by product_name order by order_year) as py_sales,
	current_sales - lag(current_sales) over(partition by product_name order by order_year) as diff_py,
	case
		when current_sales - lag(current_sales) over(partition by product_name order by order_year)>0 then 'Increasing'
		when current_sales - lag(current_sales) over(partition by product_name order by order_year)<0 then 'Decreasing'
		else 'No change'
	end as py_change -- checking the current and previous year sales per year for each product
from yearly_product_sales
order by product_name, order_year;
