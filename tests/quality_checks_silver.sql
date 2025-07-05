/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
/*
=========================================
Checking 'silver.crm_cust_info'
=========================================
*/
-- check for NULL or duplicates for the primary key cst_id
select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;

-- check for unwanted spaces in the string
select cst_firstname
from silver.crm_cust_info
where cst_firstname!=trim(cst_firstname);

-- data standardization and consistency
select distinct cst_gndr
from silver.crm_cust_info;

/*
=========================================
Checking 'silver.crm_prd_info'
=========================================
*/
-- check for duplicate or null primary key
select prd_id, count(*)
from silver.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null;

-- check for NULL or negative costs
select prd_cost
from silver.crm_prd_info
where prd_cost<0 or prd_cost is null;

-- data standardization and consistency
select distinct prd_line
from silver.crm_prd_info;

-- check for invalid date
select *
from silver.crm_prd_info
where prd_end_dt<prd_start_dt;

/*
=========================================
Checking 'silver.crm_sales_details'
=========================================
*/
-- check for invalid dates
select nullif(sls_due_dt,0) as sls_due_dt
from silver.crm_sales_details
where sls_due_dt<=0;

-- check for invalid dates
-- order date should be greater than the order ship and due date
select *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt
or sls_order_dt > sls_due_dt;

-- check for data consistency : Sales = Quantity * Price
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

/*
=========================================
Checking 'silver.erp_cust_az12'
=========================================
*/
-- check for invalid birth dates
select distinct bdate
from silver.erp_cust_az12
where bdate > NOW();

-- Data standardization and consistency
SELECT 
  DISTINCT gen
FROM silver.erp_cust_az12;

/*
=========================================
Checking 'silver.erp_loc_a101'
=========================================
*/
-- Data standardization and consistency
select distinct cntry
from silver.erp_loc_a101;

/*
=========================================
Checking 'silver.erp_px_cat_g1v2'
=========================================
*/
-- Check for Unwanted Spaces
select * 
from silver.erp_px_cat_g1v2
where cat != TRIM(cat) 
   or subcat != TRIM(subcat) 
   or maintenance != TRIM(maintenance);

-- Data standardization and consistency
select distinct 
maintenance
from silver.erp_px_cat_g1v2;
