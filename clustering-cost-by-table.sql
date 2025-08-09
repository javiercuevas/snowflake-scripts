select to_date(start_time), table_name, sum(credits_used) as credits
from snowflake.account_usage.automatic_clustering_history
where start_time > dateadd(day, -30, current_timestamp())
group by 1,2
order by 3 desc;
