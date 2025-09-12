SELECT distinct USER_NAME, max(start_time)::date as last_accessed_date
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE true
AND start_time between '2025-07-01' and '2025-09-11'
AND QUERY_TEXT ilike '%WEATHER_HYDRO_HOURLY%'
group by all
ORDER BY 1, 2
LIMIT 1000;
