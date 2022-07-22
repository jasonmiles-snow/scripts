/* ************************* FEATURE-DEMO: Change Tracking ************************* */

/* What you will see :
Change Tracking adds a pair of hidden columns to the table and begins storing change tracking metadata. The values in these hidden CDC data columns provide the input for the stream metadata columns.
These hidden metadata column can be use in a scenario where you want to restore a table to older state. Essentially it shows a feature that can be use in a context of continuous data protection
*/

use role ACCOUNTADMIN;

/* create objects: database, schema and table for the demo */
create warehouse if not exists CHANGETRACKING_WH with warehouse_size = 'xsmall' auto_suspend = 300 initially_suspended = true;
create or replace database CHANGETRACKING_DEMO;
create or replace schema CHANGETRACKING_DEMO.CRM;

/* Set the context */
use database CHANGETRACKING_DEMO;
use schema CRM;

-- create a database from the share that should be available for every Snowflake Account (PS: you may need to modify "SFSALESSHARED.SFC_SAMPLES_AWS_EU_WEST_1." to reflect your own setup)
-- Or you can this via the UI:
-- 1. Go to Data --> Private Sharing and scrool down --> under "Direct Shares" you should see something called "SNOWFLAKE_SAMPLE"
-- 2. Click "Get Data" and change the name under "Database name" to "SNOWFLAKE_SAMPLE_DATA"
create database if not exists "SNOWFLAKE_SAMPLE_DATA" from SHARE SFSALESSHARED.SFC_SAMPLES_AWS_EU_WEST_1."SAMPLE_DATA";

-- create a table derived from pre-polulated data in every Snowflake account
-- Please note the last column "OPTIN" which could be associated with an opt-in option to check when subscribing or registering to a website/service
create or replace table CHANGETRACKING_DEMO.CRM.CUSTOMERS as (
    select 
        a.C_SALUTATION,
        a.C_FIRST_NAME,
        a.C_LAST_NAME,
        case UNIFORM(1,3,RANDOM()) when 1 then 'UK' when 2 then 'US' else 'FRANCE' end as C_BIRTH_COUNTRY,
        a.C_EMAIL_ADDRESS,
        b.CD_GENDER,
        b.CD_CREDIT_RATING,
        (case UNIFORM(1,3,RANDOM()) when 1 then 'YES' when 2 then 'NO' else NULL end)::varchar(3) as OPTIN
    from 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER a,
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_DEMOGRAPHICS b
    where
        a.C_CUSTOMER_SK = b.CD_DEMO_SK and 
        a.C_SALUTATION is not null and
        a.C_FIRST_NAME is not null and
        a.C_LAST_NAME is not null and
        a.C_BIRTH_COUNTRY is not null and
        a.C_EMAIL_ADDRESS is not null and 
        b.CD_GENDER is not null and
        b.CD_CREDIT_RATING is not null
    limit 200 )
;

-- We are alering the table to track the changes via the "CHANGE_TRACKING" feature on the table
alter table CHANGETRACKING_DEMO.CRM.CUSTOMERS SET CHANGE_TRACKING = TRUE;

-- verify the result
select * from CHANGETRACKING_DEMO.CRM.CUSTOMERS sample (10);

/*************************/
/* DELETION OF SOME ROWS */
/*************************/

-- Let us delete all the rows that have an OPTIN value of null
delete from CHANGETRACKING_DEMO.CRM.CUSTOMERS
      where OPTIN is null;

-- see the result
select * from CHANGETRACKING_DEMO.CRM.CUSTOMERS;

-- verify the number of rows in the table
select count(*) from CHANGETRACKING_DEMO.CRM.CUSTOMERS;

/********************************************************************************************************************/
/* CONTINUOUS DATA PROTECTION using CHANGETRACKING: Retrieve an older state of a table where changes have happened */
/********************************************************************************************************************/

-- get the query ID of the delete statement, hold the result in a SQL variable
set query_id =
  (select query_id
   from table(information_schema.query_history_by_user(result_limit => 100))
   where query_text like 'delete%' order by start_time desc limit 1);

select $query_id;

-- compare the pair - before and after
select
    (select count(*) from CHANGETRACKING_DEMO.CRM.CUSTOMERS) current_table_state,
    (select count(*) from CHANGETRACKING_DEMO.CRM.CUSTOMERS before (statement => $query_id)) earlier_table_state;

-- if we have made inserts/updates to the table since the delete
-- we can't just restore, we would lose our changes
-- we use change tracking on our table to identify the deleted records and
-- insert them back into the table

-- our table has change tracking turned on - so we know about the DELETEs
-- use this information to put the deleted records back
insert into CHANGETRACKING_DEMO.CRM.CUSTOMERS (
  select  C_SALUTATION
        , C_FIRST_NAME
        , C_LAST_NAME
        , C_BIRTH_COUNTRY
        , C_EMAIL_ADDRESS
        , CD_GENDER
        , CD_CREDIT_RATING
        , OPTIN
  from  CHANGETRACKING_DEMO.CRM.CUSTOMERS
    changes (information => default)
    before (statement => $query_id)
  where metadata$action = 'DELETE');

-- And we are back in business
select count(*) from CHANGETRACKING_DEMO.CRM.CUSTOMERS;

select * from CHANGETRACKING_DEMO.CRM.CUSTOMERS sample (10);


/* RESET */
drop database if exists CHANGETRACKING_DEMO;
drop database if exists SNOWFLAKE_SAMPLE_DATA;
drop warehouse if exists CHANGETRACKING_WH;
