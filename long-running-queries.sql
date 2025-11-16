-- QUERY_HISTORY: Returns queries within a specified time range.
-- QUERY_HISTORY_BY_SESSION:  Returns queries within a specified session and time range.
-- QUERY_HISTORY_BY_USER:  Returns queries submitted by a specified user within a specified time range.
-- QUERY_HISTORY_BY_WAREHOUSE:  Returns queries executed by a specified warehouse within a specified time range.


-------------------------------------------------------------------------------------------------------------------------------
-- The below query retrieves the long running queries which ran more than 5 minutes in last 1 hour.
-------------------------------------------------------------------------------------------------------------------------------
select
    query_id,
    query_text,
    user_name,
    warehouse_name,
    start_time,
    end_time,
    datediff(second, start_time, end_time) as run_time_in_seconds
from table(information_schema.query_history())
where datediff(minute, start_time, end_time) > 5
and start_time > dateadd(hour, -1, current_timestamp())
order by start_time;


-------------------------------------------------------------------------------------------------------------------------------
-- Alternatively the above query can also written by passing arguments (END_TIME_RANGE_START, END_TIME_RANGE_END)  
-- to the QUERY_HISTORY table function as shown below.
-------------------------------------------------------------------------------------------------------------------------------
select
    query_id,
    query_text,
    user_name,
    warehouse_name,
    start_time,
    end_time,
    datediff(second, start_time, end_time) as run_time_in_seconds
from table(information_schema.query_history(
    END_TIME_RANGE_START => dateadd(hour,-1,current_timestamp()),
    END_TIME_RANGE_END => current_timestamp() ))
where datediff(minute, start_time, end_time) > 5
order by start_time;


-------------------------------------------------------------------------------------------------------------------------------
-- The below query retrieves the queries started in last 5 minutes and still running.
-------------------------------------------------------------------------------------------------------------------------------
select
    query_id,
    query_text,
    user_name,
    warehouse_name,
    start_time,
    datediff(second, start_time, current_timestamp) as run_time_in_seconds
from table(information_schema.query_history(END_TIME_RANGE_START => dateadd(minute, -5, current_timestamp)))
where execution_status='RUNNING'
order by start_time;


-------------------------------------------------------------------------------------------------------------------------------
-- Finding long running queries by User
-------------------------------------------------------------------------------------------------------------------------------
select
    query_id,
    query_text,
    user_name,
    warehouse_name,
    start_time,
    datediff(second, start_time, current_timestamp) as run_time_in_seconds
from table(information_schema.query_history_by_user(USER_NAME => 'TONY'))
where execution_status='RUNNING'
order by start_time;


-------------------------------------------------------------------------------------------------------------------------------
-- Finding long running queries by Warehouse
-------------------------------------------------------------------------------------------------------------------------------
select
    query_id,
    query_text,
    user_name,
    warehouse_name,
    start_time,
    datediff(second, start_time, current_timestamp) as run_time_in_seconds
from table(information_schema.query_history_by_warehouse(
    END_TIME_RANGE_START => dateadd(hour,-1,current_timestamp()),
    END_TIME_RANGE_END => current_timestamp(),
    WAREHOUSE_NAME => 'COMPUTE_WH'))
where execution_status='RUNNING'
order by start_time;


-------------------------------------------------------------------------------------------------------------------------------
-- Killing a long running query in Snowflake using SYSTEM$CANCEL_QUERY
-------------------------------------------------------------------------------------------------------------------------------
select system$cancel_query('<query_id>');
