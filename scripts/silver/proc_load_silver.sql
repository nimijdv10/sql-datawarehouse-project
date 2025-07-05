/*
=============================================================
Stored Procedure: Load Silver Layer (Bronze --> Silver)
=============================================================
Script Purpose:
	This stored procedure performs ETl process to populate
    the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
	Truncates Silver tables.
	Inserts transformed and cleaned data from Bronze into
    Silver tables.
Parameters: None
Usage Example: USE silver.load_silver;
*/
DROP PROCEDURE IF EXISTS silver.load_silver;

DELIMITER //

CREATE procedure silver.load_silver()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE start_time_batch DATETIME;
    DECLARE end_time DATETIME;
    DECLARE end_time_batch DATETIME;
    DECLARE duration_seconds INT;
    
  -- Loading crm_cust_info
  SET start_time = NOW();
	SET start_time_batch = NOW();
    
	TRUNCATE TABLE silver.crm_cust_info;
	insert into silver.crm_cust_info (cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select cst_id, 
	cst_key, 
	trim(cst_firstname) as cst_firstname, 
	trim(cst_lastname) as cst_lastname, 
	case
		when upper(trim(cst_marital_status)) = 'S' then 'Single'
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
		else 'n/a'
	end cst_marital_status, -- Normalize marital status values
	case
		when upper(trim(cst_gndr)) = 'M' then 'Male'
		when upper(trim(cst_gndr)) = 'F' then 'Female'
		else 'n/a'
	end cst_gndr, -- Normalize gender status values
	cst_create_date
	from(
	select *, row_number() over(partition by cst_id order by cst_create_date desc) as rnk
	from bronze.crm_cust_info) t where rnk=1; -- Select the most recent record per customer
    
    SET end_time = NOW();
    SET duration_seconds = TIMESTAMPDIFF(SECOND, start_time, end_time);
	INSERT INTO silver.load_log (step_name, table_name, start_time, end_time, duration_seconds, status)
    VALUES ('Step 1', 'crm_cust_info', start_time, end_time, duration_seconds, 'crm_cust_info loaded successfully');

	-- Loading crm_prd_info
    SET start_time = NOW();
	TRUNCATE TABLE silver.crm_prd_info;
	insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select 
	prd_id,
	replace(substring(prd_key,1,5),'-','_') as cat_id, -- extract catergory ID
	substring(prd_key,7,length(prd_key)) as prd_key, -- extract product key
	prd_nm,
	prd_cost,
	case upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a'
	end prd_line, -- map product line codes to deescriptive values
	cast(prd_start_dt as DATE) as prd_start_dt,
	cast(
		DATE_SUB(
			LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt), INTERVAL 1 DAY
		) as DATE
	) as prd_end_dt -- calculate end date as one day before the next start date
	from bronze.crm_prd_info;
	SET end_time = NOW();
    SET duration_seconds = TIMESTAMPDIFF(SECOND, start_time, end_time);
	INSERT INTO silver.load_log (step_name, table_name, start_time, end_time, duration_seconds, status)
    VALUES ('Step 2', 'crm_prd_info', start_time, end_time, duration_seconds, 'crm_prd_info loaded successfully');

	-- Loading crm_sales_details
	SET start_time = NOW();
	TRUNCATE TABLE silver.crm_sales_details;
	insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
			when sls_order_dt<=0 or length(sls_order_dt)!=8 then NULL
			else cast(cast(sls_order_dt as CHAR) as date)
		end as sls_order_dt,
		case 
			when sls_ship_dt<=0 or length(sls_ship_dt)!=8 then NULL
			else cast(cast(sls_ship_dt as CHAR) as date)
		end as sls_ship_dt,
		case 
			when sls_due_dt<=0 or length(sls_due_dt)!=8 then NULL
			else cast(cast(sls_due_dt as CHAR) as date)
		end as sls_due_dt,
		case 
			when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity*abs(sls_price) then sls_quantity*abs(sls_price)
			else sls_sales end as sls_sales,
		sls_quantity,
		cast(
			case 
				when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
				else sls_price
			end as signed)
		as sls_price
	FROM bronze.crm_sales_details;
    SET end_time = NOW();
    SET duration_seconds = TIMESTAMPDIFF(SECOND, start_time, end_time);
	INSERT INTO silver.load_log (step_name, table_name, start_time, end_time, duration_seconds, status)
    VALUES ('Step 3', 'crm_sales_details', start_time, end_time, duration_seconds, 'crm_sales_details loaded successfully');

	-- Loading erp_cust_az12
	SET start_time = NOW();
	TRUNCATE TABLE silver.erp_cust_az12;
	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	select 
	case 
		when cid like 'NAS%' then substring(cid,4, length(cid)) -- Remove 'NAS' prefix if present
		else cid 
	end as cid, 
	case
		when bdate > NOW() then NULL
		else bdate
	end as bdate, -- Set future dates to null
	case
		when upper(trim(gen)) like 'F%' then "Female"
		when upper(trim(gen)) like 'M%' then "Male"
		else "n/a"
	end as gen -- Normalize the gender values
	from bronze.erp_cust_az12;
    SET end_time = NOW();
    SET duration_seconds = TIMESTAMPDIFF(SECOND, start_time, end_time);
	INSERT INTO silver.load_log (step_name, table_name, start_time, end_time, duration_seconds, status)
    VALUES ('Step 4', 'erp_cust_az12', start_time, end_time, duration_seconds, 'erp_cust_az12 loaded successfully');

	-- Loading erp_loc_a101
	SET start_time = NOW();
	TRUNCATE TABLE silver.erp_loc_a101;
	insert into silver.erp_loc_a101(
	cid, cntry
	)
	select replace(cid,'-','') as cid, 
	case 
		when TRIM(REPLACE(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''), '\t', '')) = 'DE' then 'Germany'
		when TRIM(REPLACE(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''), '\t', '')) in ('US','USA') then 'United States'
		WHEN TRIM(REPLACE(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''), '\t', '')) = '' THEN 'n/a'
		else TRIM(REPLACE(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''), '\t', ''))
	end as cntry
	from bronze.erp_loc_a101;
    SET end_time = NOW();
    SET duration_seconds = TIMESTAMPDIFF(SECOND, start_time, end_time);
	INSERT INTO silver.load_log (step_name, table_name, start_time, end_time, duration_seconds, status)
    VALUES ('Step 5', 'erp_loc_a101', start_time, end_time, duration_seconds, 'erp_loc_a101 loaded successfully');

	-- Loading erp_px_cat_g1v2
	SET start_time = NOW();
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	insert into silver.erp_px_cat_g1v2(
	id, cat, subcat, maintenance
	)
	select 
	id,
	cat,
	subcat,
	 TRIM(
	    REPLACE(
	      REPLACE(
	        REPLACE(maintenance, CHAR(13), ''),  
	        CHAR(10), ''                     
	      ),
	      CHAR(9), ''                           
	    )
		) AS maintenance
	from bronze.erp_px_cat_g1v2;
    SET end_time = NOW();
    SET duration_seconds = TIMESTAMPDIFF(SECOND, start_time, end_time);
	INSERT INTO silver.load_log (step_name, table_name, start_time, end_time, duration_seconds, status)
    VALUES ('Step 6', 'erp_px_cat_g1v2', start_time, end_time, duration_seconds, 'erp_px_cat_g1v2 loaded successfully');

	SET end_time_batch = NOW();
	SET duration_seconds = TIMESTAMPDIFF(SECOND, start_time_batch, end_time_batch);
	INSERT INTO silver.load_log (step_name, table_name, start_time, end_time, duration_seconds, status)
    VALUES ('Step 7', 'All 6 tables', start_time_batch, end_time_batch, duration_seconds, 'Data loaded successfully');

END //
DELIMITER ;
