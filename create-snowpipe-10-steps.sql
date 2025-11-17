/*

https://calogica.com/sql/snowflake/2019/04/04/snowpipes.html


 The Steps
1) Set up a separate database
2) Set up a schema to hold our source data
3) Create a Table
4) Create the File Format
5) Create an external stage pointing to your s3 location
6) Review staged files and select data from the files
7) Test loading data into the table
8) Create the Snowpipe
9) Force a pipe refresh
10) Monitor data loads

*/

---------------------------------------------------------------------
--1. Set up a separate database
---------------------------------------------------------------------
create database etl;
use etl;

---------------------------------------------------------------------
--2. Set up a schema to hold our source data
---------------------------------------------------------------------
create schema src;

---------------------------------------------------------------------
--3. Create a Table
---------------------------------------------------------------------
create table src.my_source_table
(
	col_1 varchar,
	col_2 varchar
);

---------------------------------------------------------------------
--4. Create the File Format
---------------------------------------------------------------------
create or replace file format my_csv_format
  type = csv field_delimiter = ',' skip_header = 1
  field_optionally_enclosed_by = '"'
  null_if = ('NULL', 'null') 
  empty_field_as_null = true;
  
  
 --check 
 show file formats;
  
---------------------------------------------------------------------
--5. Create an external stage pointing to your s3 location
---------------------------------------------------------------------

create or replace stage my_stage url='s3://my_bucket/key/key/'
  credentials=(aws_key_id='KEY' aws_secret_key='SECRET')
  file_format = my_csv_format;
  
  
 -- or using IAM role
 create or replace stage my_stage url='s3://my_bucket/key/key/'
  credentials=(aws_role='aws_iam_role=arn:aws:iam::XXXXXXX:role/XXXX')
  file_format = my_csv_format;
  

---------------------------------------------------------------------
--6. Review staged files and select data from the files
---------------------------------------------------------------------

list @my_stage;


select t.$1, t.$2
from @my_stage (file_format => my_csv_format) t;

---------------------------------------------------------------------
--7. Test loading data into the table
---------------------------------------------------------------------

copy into src.my_source_table
  from @my_stage
  file_format = my_csv_format
  pattern='.*sales.*.csv';
;

--sample: with error handling
copy into src.my_source_table
    from @my_stage
    file_format = my_csv_format
    on_error='continue'

---------------------------------------------------------------------
--8. Create the Snowpipe
---------------------------------------------------------------------

create pipe if not exists my_pipe as
copy into src.my_source_table from @my_stage;


---------------------------------------------------------------------
--9. Force a pipe refresh
---------------------------------------------------------------------

alter pipe my_pipe refresh;

---------------------------------------------------------------------
--10. Monitor data loads
---------------------------------------------------------------------

select system$pipe_status('my_pipe');

select *
from table(information_schema.copy_history(table_name=>'MY_SOURCE_TABLE', 
  start_time=> dateadd(hours, -24, current_timestamp())));
  
 
use role accountadmin;
use snowflake;

select
    convert_timezone('America/Los_Angeles', h.last_load_time)::timestamp_ntz::date as load_date,
    max(convert_timezone('America/Los_Angeles', h.last_load_time)::timestamp_ntz) as max_load_time,
    sum(h.row_count) as rows_loaded,
    sum(h.error_count) as errors
from account_usage.copy_history h
where table_name = 'MY_SOURCE_TABLE'
group by 1
order by 1;