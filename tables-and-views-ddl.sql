------------------------------------------------------------------------------------------------------------------------
--https://medium.com/@kaushalvishal228/ddl-extraction-list-all-table-views-ddls-in-single-query-f067cd7fdb03
------------------------------------------------------------------------------------------------------------------------

DECLARE
DDL_CUR CURSOR FOR SELECT CONCAT_WS('.',TABLE_CATALOG,TABLE_SCHEMA,CONCAT('"',TABLE_NAME,'"')) AS tablename
               FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_TYPE = 'BASE TABLE' -- Object type for which we need statements
                 AND TABLE_SCHEMA ILIKE 'Schema Name';
BEGIN
  CREATE OR REPLACE TEMP TABLE DDL_INFO(Tablename TEXT, definition TEXT);--Temp table to store data

  FOR record IN DDL_CUR DO   
    EXECUTE IMMEDIATE REPLACE('INSERT INTO DDL_INFO(Tablename, definition)
                        SELECT ''<view_name>'', GET_DDL(''TABLE'', ''<view_name>'')'
                        ,'<view_name>'
                        ,record.tablename);
 END FOR;

 LET Result RESULTSET := (SELECT * FROM DDL_INFO); -- Print the result 

 RETURN TABLE(Result);
END;
