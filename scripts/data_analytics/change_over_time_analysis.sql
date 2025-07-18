/* 
**********************************************************
Change Over Time
**********************************************************
Purpose: Analyze how a measure evolves over time. 
Helps track trends and identify seasonality in the data
Analysis:
	- Overall year 2013 was the best performing year in terms of sales, customers and items purchased.
	- total_sales over time - Year 2013 had the best sales but decreased by a lot in the next year
*/
select
YEAR(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by YEAR(order_date)
order by YEAR(order_date);

/*
Analysis:
	- In terms of month, December had the best sales because of the holidays (Christmas and New Years)
    - Febrauary is the worst perfroming month.
*/
select
MONTH(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by MONTH(order_date)
order by MONTH(order_date);

-- Aggregating by month of every year
select
DATE_FORMAT(order_date,'%Y-%m-01') as order_date,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by DATE_FORMAT(order_date,'%Y-%m-01')
order by DATE_FORMAT(order_date,'%Y-%m-01');
