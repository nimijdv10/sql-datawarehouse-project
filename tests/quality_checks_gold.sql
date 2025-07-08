/*
===============================================================================
Quality Checks
===============================================================================
This script performs quality checks to validate the integrity, consistency, 
and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.
===============================================================================
*/
-- Checking the uniqueness of customer key in gold.dim_customers
select customer_key, count(*) as duplicate_key
from gold.dim_customers
group by customer_key
having count(*)>1;

-- Checking the uniqueness of product key in gold.dim_products
select product_key, count(*) as duplicate_key
from gold.dim_products
group by product_key
having count(*)>1;

-- Check the data model connectivity between facts and dimension tables.
select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null;
