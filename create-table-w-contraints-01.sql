--https://medium.com/snowflake/secret-snowflake-data-modeling-features-you-need-to-know-about-1c0a1429c76b

CREATE OR REPLACE TABLE customer
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 location_id         number(38,0) NOT NULL,
 comment             varchar COMMENT 'yo dawg, I heard you like comments...',

 CONSTRAINT pk_customer  PRIMARY KEY ( customer_id ),
 CONSTRAINT FK_CUSTOMER_BASED_IN_LOCATION 
        FOREIGN KEY ( location_id ) REFERENCES location ( location_id ) RELY  
        COMMENT  'A customer is based in one and only one location. A location can have zero or many customers.'
)
COMMENT = 'Registered cusotmers'
;
