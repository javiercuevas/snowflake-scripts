-- https://medium.com/@vivekmcm1/how-to-make-your-snowflake-database-case-insensitive-at-the-database-and-table-level-77bae90fd3af

-- Creating a case-insensitive database
CREATE DATABASE my_database 
  COLLATE = 'en-ci';  -- Case Insensitive collation


CREATE TABLE my_table (
    id INT,
    name STRING COLLATE = 'en-ci'  -- Case Insensitive for the 'name' column
);


-- Check the collation of the database
SELECT DATABASE_NAME, COLLATION
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME = 'my_database';

-- Check the collation of columns in the table
SELECT TABLE_NAME, COLUMN_NAME, COLLATION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'my_table';


