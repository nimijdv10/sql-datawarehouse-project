/*
===============================================================
Load Bronze Layer
===============================================================
This script loads data into the 'bronze' schema from external
CSV files. It truncates the bronze tables before lading the data
and uses BULK INSERT command to load the data from the csv
files to bronze tables.
*/
-- Set table name
SET @table_name := 'crm_cust_info';

-- Capture start time
SET @start_time := NOW();
SET @start_time_batch := NOW();

TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA LOCAL INFILE '/Users/nimishajadav/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Capture end time
SET @end_time := NOW();

-- Calculate and display duration
SELECT 
    @table_name AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds,
    'crm_cust_info loaded successfully' AS status;

-- Set table name
SET @table_name := 'crm_prd_info';

-- Capture start time
SET @start_time := NOW();

TRUNCATE TABLE bronze.crm_prd_info;
LOAD DATA LOCAL INFILE '/Users/nimishajadav/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Capture end time
SET @end_time := NOW();

-- Calculate and display duration
SELECT 
    @table_name AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds,
    'crm_prd_info loaded successfully' AS status;

-- Set table name
SET @table_name := 'crm_sales_details';

-- Capture start time
SET @start_time := NOW();

TRUNCATE TABLE bronze.crm_sales_details;
LOAD DATA LOCAL INFILE '/Users/nimishajadav/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Capture end time
SET @end_time := NOW();

-- Calculate and display duration
SELECT 
    @table_name AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds,
    'crm_sales_details loaded successfully' AS status;

-- Set table name
SET @table_name := 'erp_cust_az12';

-- Capture start time
SET @start_time := NOW();

TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA LOCAL INFILE '/Users/nimishajadav/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Capture end time
SET @end_time := NOW();

-- Calculate and display duration
SELECT 
    @table_name AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds,
    'erp_cust_az12 loaded successfully' AS status;

-- Set table name
SET @table_name := 'erp_loc_a101';

-- Capture start time
SET @start_time := NOW();

TRUNCATE TABLE bronze.erp_loc_a101;
LOAD DATA LOCAL INFILE '/Users/nimishajadav/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Capture end time
SET @end_time := NOW();

-- Calculate and display duration
SELECT 
    @table_name AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds,
    'erp_loc_a101 loaded successfully' AS status;

-- Set table name
SET @table_name := 'erp_px_cat_g1v2';

-- Capture start time
SET @start_time := NOW();

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE '/Users/nimishajadav/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Capture end time
SET @end_time := NOW();

-- Calculate and display duration
SELECT 
    @table_name AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds,
    'erp_px_cat_g1v2 loaded successfully' AS status;

SET @end_time_batch := NOW();
SELECT 
    @start_time_batch AS start_time,
    @end_time_batch AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time_batch, @end_time_batch) AS duration_seconds,
    'Batch Loading Successful...' AS status;
