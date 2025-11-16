SELECT query_id, object_name, transaction_id, blocker_queries
  FROM SNOWFLAKE.ACCOUNT_USAGE.LOCK_WAIT_HISTORY
  WHERE requested_at >= DATEADD('hours', -24, CURRENT_TIMESTAMP());
