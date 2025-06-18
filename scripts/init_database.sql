/*
================================
Create database and schemas
================================
This script creates a new database called 'DataWarehouse' after checking if it already exists. If the database 
already exists then we drop and recreate it. The script sets up 3 schemas within the database: 'bronze', 'silver' 
and 'gold'
*/

-- Drop the database if it already exists
DROP DATABASE IF EXISTS `DataWarehouse`;

-- Create the new database 'DataWarehouse'
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
