/*
===============================================================
DDL Scripts: Create gold views
===============================================================
This script creates views for the gold leyer in the data warehouse.
The gold layer represents the final dimension and the facts tables
(Star Schema).

These views can be used directly for analytics and reporting.
*/
-- ==========================================================
-- Create dimension: gold.dim_customers
-- ==========================================================
DROP VIEW IF EXISTS gold.dim_customers;

create view gold.dim_customers as 
select 
row_number() over(order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case
	when ci.cst_gndr!='n/a' then ci.cst_gndr
    else coalesce(ca.gen,'n/a')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;

-- ==========================================================
-- Create dimension: gold.dim_products
-- ==========================================================
DROP VIEW IF EXISTS gold.dim_products;

create view gold.dim_products as
select
row_number() over(order by prd_id) as product_key,
pi.prd_id as product_id,
pi.prd_key as product_number,
pi.prd_nm as product_name,
pi.cat_id as category_id,
pcg.cat as category,
pcg.subcat as sub_category,
pcg.maintenance,
pi.prd_cost as product_cost,
pi.prd_line as product_line,
pi.prd_start_dt as product_start_line
from silver.crm_prd_info as pi
left join silver.erp_px_cat_g1v2 as pcg
on pi.cat_id=pcg.id
where prd_end_dt is null;

-- ==========================================================
-- Create fact: gold.fact_sales
-- ==========================================================
DROP VIEW IF EXISTS gold.fact_sales;

create view gold.fact_sales as 
select
sd.sls_ord_num as norder_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id;
