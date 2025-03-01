create or replace table stg_users
(
    user_id  int,
    user_name varchar(50),
    city varchar(50),
    state varchar(50)
);


create or replace table users
(
    user_id  int,
    user_name varchar(50),
    city varchar(50),
    state varchar(50),
    valid_from timestamp_ntz,
    valid_to timestamp_ntz,
    active_flag boolean,
    update_timestamp timestamp_ntz
);

insert into stg_users (user_id, user_name, city, state)
values
(1, 'Javi', 'Gilbert', 'AZ'),
(2, 'Jason', 'Glendale', 'AZ'),
(3, 'Consultant', 'New York', 'NY');

---------------------------------------
--sample select of stage table
---------------------------------------
select *
from stg_users;

---------------------------------------
--merge
---------------------------------------
MERGE INTO USERS as target
USING 
(
	select user_id as key_1, *, current_timestamp as valid_from
    from STG_USERS

    union all
    -------------------------------------------
    --inserts after invalidating old record
    -------------------------------------------
    select null as key_1, stg.*, current_timestamp as valid_from
    from STG_USERS stg
    join USERS tgt 
        on stg.user_id = tgt.user_id
        and tgt.valid_to = '9999-12-31'
    where 
        stg.user_name <> tgt.user_name
        or stg.city <> tgt.city
        or stg.state <> tgt.state
) as source
on target.user_id = source.key_1

-------------------------------------------------------
--set current_indicator = 0
-------------------------------------------------------
when matched and target.active_flag = 1 and 
    (
    target.user_name <> source.user_name or
    target.city <> source.city or
    target.state <> source.state
    )
    then update set
    target.valid_to = current_timestamp,
    target.active_flag = 0,
    target.update_timestamp = current_timestamp
-------------------------------------------------------
--insert new row
-------------------------------------------------------
when not matched then insert
    (user_id, user_name, city, state, valid_from, valid_to, active_flag, update_timestamp)
values
    (source.user_id, source.user_name, source.city, source.state, source.valid_from, '9999-12-31', 1, current_timestamp);


---------------------------------------
--check results
---------------------------------------
select *
from users
order by user_id, update_timestamp desc;


---------------------------------------
--edit stage table to test type 2 merge
---------------------------------------
update stg_users
set city = 'Chandler'
where user_id = 1;
