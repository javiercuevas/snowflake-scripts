ALTER TABLE HRDATA.employees add column emp_status varchar(10);

CREATE TABLE hrdata.countries 
   ( country_id CHAR(2) NOT NULL 
   , country_name VARCHAR(40) 
   , region_id NUMBER 
   , PRIMARY KEY (country_id) 
   );
   
CREATE TABLE hrdata.regions
   ( region_id NUMBER NOT NULL 
   , region_name VARCHAR(25) 
   , PRIMARY KEY (region_id)
   );

update HRDATA.employees SET emp_status = 'Active';

CREATE VIEW hrdata.v_employees as select * from hrdata.employees;