select *
from RAW_ORDERS;

truncate table RAW_ORDERS;

create or replace file format CSV_FORMAT
  type = csv field_delimiter = ',' skip_header = 1
  field_optionally_enclosed_by = '"'
  null_if = ('NULL', 'null') 
  empty_field_as_null = true;


list @stg_test_02;


select t.$1, t.$2, METADATA$FILENAME as filename
from @stg_test_02/landing/orders (file_format => csv_format) t
order by t.$1


select t.id, t.product
from @stg_test_02/landing/orders (file_format => csv_format) t;


use schema MISC_DEV;

copy into RAW_ORDERS
  from @stg_test_02
  file_format = my_csv_format
  pattern='.*orders.*.csv';
;

-- CREATE PIPE PIPE_ORDERS
-- AUTO_INGEST = TRUE
-- INTEGRATION = 'MY_INT'
-- AS
COPY INTO RAW_ORDERS 
FROM @stg_test_02/landing/orders
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = CSV_HEADER_FORMAT
INCLUDE_METADATA = (ETL_EXTRACT_DATETIME = METADATA$START_SCAN_TIME, FILEPATH = METADATA$FILENAME);

--alter pipe PIPE_ORDERS refresh;

select *
from RAW_ORDERS;


truncate table RAW_ORDERS;



--'@"WKSP_DTS"."RAW_PI"."STG_PI"/test/test


use schema RAW_PI;


create or replace TRANSIENT TABLE RAW_ORDERS (
	ID NUMBER(38,0),
	PRODUCT VARCHAR(255),
	PRICE NUMBER(38,0),
	FILEPATH VARCHAR(255),
	ETL_EXTRACT_DATETIME TIMESTAMP_NTZ(9)
);


CREATE OR REPLACE FILE FORMAT CSV_HEADER_FORMAT
	TYPE = csv
	PARSE_HEADER = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('NULL', 'null')
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
;

CREATE PIPE PIPE_ORDERS
AUTO_INGEST = TRUE
INTEGRATION = 'WKSP_DTS_QUEUE'
AS
COPY INTO RAW_ORDERS 
FROM @WKSP_DTS.RAW_PI.STG_PI/test/test
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = CSV_HEADER_FORMAT
INCLUDE_METADATA = (ETL_EXTRACT_DATETIME = METADATA$START_SCAN_TIME, FILEPATH = METADATA$FILENAME);


select *
from raw_orders;

truncate table raw_orders;

alter pipe PIPE_ORDERS refresh;



