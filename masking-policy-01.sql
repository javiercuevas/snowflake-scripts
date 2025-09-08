create or replace masking policy creditcardno_mask as (val string) returns string ->
case
    when is_role_in_session('PI_ANALYTICS') then
        right(val, 4)
    else
        '***MASKED***'
end;
