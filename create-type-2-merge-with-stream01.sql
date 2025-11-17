--https://insightsthroughdata.com/how-to-build-a-history-table-with-snowflake-and-fivetran/


-- Initialize OPPORTUNITIES table in Production
CREATE OR REPLACE TABLE "LAKE_OF_DATA"."PRODUCTION"."OPPORTUNITIES" CLONE "LAKE_OF_DATA"."STAGING"."OPPORTUNITIES";

-- Initialize OPPORTUNITIES_HISTORY table
CREATE OR REPLACE TABLE "LAKE_OF_DATA"."PRODUCTION"."OPPORTUNITIES_HISTORY" AS
SELECT CURRENT_DATE as DATE_FROM,NULL::DATE as DATE_TO,1::BOOLEAN as IS_ACTIVE,OPPID::TEXT as OPPID, OPP_NAME,CLOSE_DATE::DATE as CLOSE_DATE,OWNER,ACCOUNT,AMOUNT
from "LAKE_OF_DATA"."STAGING"."OPPORTUNITIES";

--Create Stream to track changes
create or replace stream opportunity_stream on table "LAKE_OF_DATA"."PRODUCTION"."OPPORTUNITIES";

--Show content of Stream
select * from opportunity_stream order by OPPID;

-- confirm creation and definition
Show Streams;
DESCRIBE STREAM opportunity_stream;


--Tasks
-- Start with an update of the production opportunities Table
CREATE OR REPLACE TASK UPDATE_PRODUCTION_OPPORTUNITIES
  WAREHOUSE = DEV
   SCHEDULE ='USING CRON 30 23 * * * America/Los_Angeles' //11:30pm every day
   COMMENT = 'Update OPPORTUNITIES table with edits and new records from Staging OPPORTUNITIES flat file'
As
Merge into "LAKE_OF_DATA"."PRODUCTION"."OPPORTUNITIES" t1 using "LAKE_OF_DATA"."STAGING"."OPPORTUNITIES" t2 on t1.OPPID = t2.OPPID
    when matched then update set t1.OPP_NAME=t2.OPP_NAME,t1.CLOSE_DATE=t2.CLOSE_DATE,t1.OWNER=t2.OWNER,t1.AMOUNT=t2.AMOUNT
    when not matched then insert (OPPID,OPP_NAME,CLOSE_DATE,OWNER,ACCOUNT,AMOUNT) values (t2.OPPID,t2.OPP_NAME,t2.CLOSE_DATE,t2.OWNER,t2.ACCOUNT,t2.AMOUNT)
;
-- Then follow with an Update of the OPPORTUNITIES_HISTORY table
CREATE OR REPLACE TASK UPDATE_PRODUCTION_OPPORTUNITIES_HISTORY
  WAREHOUSE = DEV
   COMMENT = 'Update OPPORTUNITIES_HISTORY table with modified and new records from OPPORTUNITIES in Production using Stream'
   AFTER UPDATE_PRODUCTION_OPPORTUNITIES
      WHEN SYSTEM$STREAM_HAS_DATA('opportunity_stream')
As
Merge into "LAKE_OF_DATA"."PRODUCTION"."OPPORTUNITIES_HISTORY" t1 using opportunity_stream t2 on t1.OPPID = t2.OPPID AND t1.OPP_NAME = t2.OPP_NAME AND t1.CLOSE_DATE = t2.CLOSE_DATE AND t1.OWNER = t2.OWNER AND t1.AMOUNT = t2.AMOUNT
    when matched AND (t2.METADATA$ACTION='DELETE') then update set DATE_TO=CURRENT_DATE,IS_ACTIVE=0
      when not matched AND (t2.METADATA$ACTION='INSERT') then insert (DATE_FROM,DATE_TO,IS_ACTIVE,OPPID,OPP_NAME,CLOSE_DATE,OWNER,ACCOUNT,AMOUNT) values (CURRENT_DATE,NULL,1,t2.OPPID,t2.OPP_NAME,t2.CLOSE_DATE,t2.OWNER,t2.ACCOUNT,t2.AMOUNT)
;
-- END TASKS

-- Tests
show tasks;
SELECT * FROM "LAKE_OF_DATA"."PRODUCTION"."OPPORTUNITIES_HISTORY"
WHERE OPPID IN('OPP1000','OPP1001','OPP1002','OPP1050','OPP1051','OPP1052')
ORDER BY OPPID,DATE_FROM;
