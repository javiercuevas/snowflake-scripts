--https://stellans.io/snowflake-data-loading-best-practices-a-guide-to-copy-stages-and-snowpipe

--Here is a practical SQL snippet to check for failed loads in the last 24 hours:
SELECT
    file_name,
    status,
    first_error_message,
    load_time
FROM
    information_schema.copy_history
WHERE
    schema_name = 'YOUR_SCHEMA' AND table_name = 'YOUR_TABLE'
    AND last_load_time >= dateadd(hour, -24, current_timestamp())
    AND status = 'LOAD_FAILED'
ORDER BY
    last_load_time DESC;



--Use this query to check Snowpipe credit consumption over the last 7 days:
SELECT
    pipe_name,
    date_trunc('day', start_time) AS usage_day,
    sum(credits_used) AS total_credits
FROM
    snowflake.account_usage.pipe_usage_history
WHERE
    start_time >= dateadd(day, -7, current_timestamp())
GROUP BY
    1, 2
ORDER BY
    2 DESC, 3 DESC;
