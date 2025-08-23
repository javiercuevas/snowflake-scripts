--https://medium.com/snowflake/tips-for-clustering-in-snowflake-d83aa32f4a6f

INSERT OVERWRITE INTO YOUR_TABLE 
SELECT * FROM YOUR_TABLE ORDER BY <YOUR NEW CLUSTER CHOICE>;
