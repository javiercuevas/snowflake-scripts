--https://blogs.perficient.com/2023/03/20/snowpipe-a-feature-of-snowflake-to-load-continuous-data/

// Preparation table first
CREATE OR REPLACE TABLE SNOW_DB.PUBLIC.employees2(
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
);
// Pause pipe
ALTER PIPE DATABASE.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = true;

// Verify pipe is paused and has pendingFileCount 0 
SELECT SYSTEM$PIPE_STATUS('DATABASE.pipes.employee_pipe');

// Recreate the pipe to change the COPY statement in the definition
CREATE OR REPLACE pipe DATABASE.pipes.employee_pipe
auto_ingest = TRUE
AS
COPY INTO SNOW_DB.PUBLIC.employees2
FROM @DATABASE.external_stages.csv_folder;

ALTER PIPE  DATABASE.pipes.employee_pipe refresh;

// List files in stage
LIST @DATABASE.external_stages.csv_folder;

SELECT * FROM SNOW_DB.PUBLIC.employees2;

// Reload files manually that where aleady in the bucket
COPY INTO SNOW_DB.PUBLIC.employees2
FROM @DATABASE.external_stages.csv_folder;  

// Resume pipe
ALTER PIPE DATABASE.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = false;

// Verify pipe is running again
SELECT SYSTEM$PIPE_STATUS('DATABASE.pipes.employee_pipe') ;
