create or replace procedure task_state_monitor(task_name string)
returns varchar not null
language SQL
AS
$$
DECLARE
    task_state string;
    c CURSOR FOR SELECT "state" from table(result_scan(last_query_id())) where "name" = ?;
BEGIN
    show tasks;
    open c USING (task_name);
    fetch c into task_state;
    IF(task_state = 'suspended') THEN 
        CALL SYSTEM$SEND_EMAIL(
            'my_email_int',
            'joe.doe@mydomain.com',
            'Email alert: Task is suspended!',
            'Please check the task state.' 
        );
        RETURN 'Email has been sent.';
     ELSE
         RETURN 'Task state is ok.';
     END IF;
END;
$$
;
