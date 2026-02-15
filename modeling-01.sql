CREATE TABLE PERSON
(
    PERSON_ID number(8,0) NOT NULL,
    SOCIAL_SERCURITY_NUM number(9,0) NOT NULL,
    DRIVERS_LICENSE varchar(10) NOT NULL,
    NAME varchar NOT NULL,
    BIRTH_DATE date NOT NULL,
    CONSTRAINT PK_1 PRIMARY KEY ( PERSON_ID )
);
CREATE TABLE ACCOUNT
(
    PERSON_ID number(8,0) NOT NULL,
    ACCOUNT_ID varchar(12) NOT NULL,
    ACCOUNT_TYPE varchar(3) NOT NULL,
    IS_ACTIVE boolean NOT NULL,
    OPEN_DATE date NOT NULL,
    CLOSE_DATE date,
    CONSTRAINT PK_2 PRIMARY KEY ( PERSON_ID, ACCOUNT_ID ),
    CONSTRAINT FK_1 FOREIGN KEY ( PERSON_ID ) REFERENCES PERSON (    PERSON_ID )
);


CREATE OR REPLACE TABLE employee
(
    employee_skey      integer NOT NULL AUTOINCREMENT START 1     INCREMENT 1,
    employee_bkey      varchar(10) NOT NULL,
    name               varchar NOT NULL,
    social_security_id number(8,0) NOT NULL,
    healthcare_id      integer NOT NULL,
    birth_date         date NOT NULL,
    CONSTRAINT pk_employee_skey PRIMARY KEY ( employee_skey ),
    CONSTRAINT ak_employee_bkey UNIQUE ( employee_bkey ),
    CONSTRAINT ak_healthcare_id UNIQUE ( healthcare_id ),
    CONSTRAINT ak_ss_id         UNIQUE ( social_security_id )
);
CREATE OR REPLACE TABLE employee_of_the_month
(
    month         date NOT NULL,
    employee_bkey varchar(10) NOT NULL,
    awarded_for   varchar NOT NULL,
    comments      varchar NOT NULL,
    CONSTRAINT pk_employee_of_the_month_month PRIMARY KEY ( month ),
    CONSTRAINT fk_ref_employee FOREIGN KEY ( employee_bkey )
    REFERENCES employee ( employee_bkey )
);


CREATE TABLE PERSON
(
    PERSON_ID number(8,0) NOT NULL,
    SOCIAL_SERCURITY_NUM number(9,0) NOT NULL,
    DRIVERS_LICENSE varchar(10) NOT NULL,
    NAME varchar NOT NULL,
    BIRTH_DATE date NOT NULL,
    CONSTRAINT PK_1 PRIMARY KEY ( PERSON_ID )
);
CREATE TABLE ACCOUNT
(
    PERSON_ID number(8,0) NOT NULL,
    ACCOUNT_ID varchar(12) NOT NULL,
    ACCOUNT_TYPE varchar(3) NOT NULL,
    IS_ACTIVE boolean NOT NULL,
    OPEN_DATE date NOT NULL,
    CLOSE_DATE date,
    CONSTRAINT PK_2 PRIMARY KEY ( PERSON_ID, ACCOUNT_ID ),
    CONSTRAINT FK_1 FOREIGN KEY ( PERSON_ID ) REFERENCES PERSON (    PERSON_ID )
);


CREATE TABLE CUSTOMER
(
    CUSTOMER_ID number (38,0) NOT NULL,
    NAME varchar NOT NULL,
    ADDRESS varchar NOT NULL,
    LOCATION_ID number (38,0) NOT NULL,
    PHONE varchar NOT NULL,
    ACCOUNT_BALANCE number (12,2) NOT NULL,
    MARKET_SEGMENT varchar NOT NULL,
    COMMENT varchar,
    CONSTRAINT PK_CUSTOMER PRIMARY KEY ( CUSTOMER_ID ),
    CONSTRAINT FK_CUSTOMER_BASED_IN_LOCATION FOREIGN KEY ( LOCATION_ID ) REFERENCES LOCATION ( LOCATION_ID )
)
COMMENT = 'Registered customers';
