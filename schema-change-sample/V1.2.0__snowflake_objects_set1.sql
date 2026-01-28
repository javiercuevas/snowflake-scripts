USE DATABASE MYDB;

-- create a storage integration object
CREATE OR REPLACE STORAGE INTEGRATION INT_AWS_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::555064756008:role/aws_s3_snowflake_intg'
  STORAGE_ALLOWED_LOCATIONS = ('s3://awss3bucketjana/csv/', 's3://awss3bucketjana/json/','s3://awss3bucketjana/pipes/csv/')
  COMMENT = 'Integration with aws s3 buckets' ;

-- Create a file format object of csv type
CREATE OR REPLACE file format mydb.public.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    empty_field_as_null = TRUE;

-- Create a stage object using storage integration
CREATE OR REPLACE stage mydb.public.stage_aws_pipes
    URL = 's3://awss3bucketjana/pipes/csv/'
    STORAGE_INTEGRATION = INT_AWS_S3
    FILE_FORMAT = mydb.public.csv_fileformat;

-- Create a table
CREATE OR REPLACE TABLE mydb.public.emp_data 
(
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
);

-- Create a stream
CREATE OR REPLACE STREAM public.stream_empl ON TABLE public.emp_data;

-- Want to Hide Phone number
CREATE OR REPLACE MASKING POLICY customer_phone 
    as (val string) returns string->
CASE WHEN CURRENT_ROLE() in ('ACCOUNT_ADMIN', 'SYSADMIN') THEN val
    ELSE '##-###-###-'||SUBSTRING(val,12,4) 
    END;
	
-- Create secure view
CREATE OR REPLACE SECURE VIEW public.SEC_VW_CUSTOMER
AS
SELECT CST.* FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER CST
INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.NATION NTN
ON CST.C_NATIONKEY = NTN.N_NATIONKEY
INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.REGION RGN
ON NTN.N_REGIONKEY = RGN.R_REGIONKEY
WHERE RGN.R_NAME='AMERICA';