
-- 10.0.0  Replication Using Failover Groups
--         This lab should take you approximately 45 minutes to complete.
--         For this Lab Exercise your instructor must assign your user the
--         ACCOUNTADMIN role.
--         By the end of this lab, you will be able to:
--         - Set up two Snowflake accounts for failover replication.
--         - Create a primary database to replicate
--         - Create a Failover Group for the database to be replicated
--         - Initiate the replication and demonstrate that the secondary
--         location is read-only
--         - Make changes at the primary location and demonstrate the changes
--         being made at the secondary location by replication
--         - Perform a failover to swap the Primary and Secondary locations
--         References used in this lab are:
--         - https://docs.snowflake.com/en/user-guide/account-replication-intro
--         - https://docs.snowflake.com/en/user-guide/account-replication-
--         considerations
--         - https://docs.snowflake.com/en/user-guide/account-replication-config
--         You will also perform the steps necessary to failover to the
--         secondary account for disaster recovery.
--         Before you can configure database replication, two or more accounts
--         must be linked to an organization. Your instructor will provide the
--         Primary and Secondary account information for this exercise.

-- 10.1.0  Set up Browsers for Database Replication

-- 10.1.1  Open two browser windows side-by-side.
--         You could also open two tabs, but the exercise is easier to complete
--         if you can see both browser windows at the same time. Two different
--         browsers would be easier to work with if possible.

-- 10.1.2  In browser 1, enter the Primary account URL (provided by your
--         instructor) and log in with your assigned credentials.

-- 10.1.3  Navigate to worksheets and load the script for this lab.

-- 10.1.4  Rename the worksheet to REPLICATION PRIMARY.
--         In the PRIMARY worksheet, you will only execute statements that are
--         surrounded by the PRIMARY comments, as shown below.

-- PRIMARY --
-- SQL statement 1;
-- SQL statement 2;
-- PRIMARY --


-- 10.1.5  In browser 2, enter the Secondary account URL and log in with your
--         assigned credentials.

-- 10.1.6  Open a new blank SQL worksheet and rename it to REPLICATION
--         SECONDARY.
--         In the SECONDARY worksheet, you will only execute statements that are
--         surrounded by the SECONDARY comments, as shown below.

-- SECONDARY --
-- SQL command 1;
-- SQL command 2;
-- SECONDARY --

--         This lab will not run successfully unless you change some of the text
--         within it. There are instances of the string [PRIMARY-ACCOUNT] and of
--         the string [SECONDARY-ACCOUNT] within this script, both of which you
--         will need to replace for the lab to run successfully.
--         Below are instructions on what you will need to do. Basically, you
--         are going to fix the SQL script in the PRIMARY account, and then copy
--         and paste the fixed SQL script over into a blank worksheet in the
--         SECONDARY account. After you’ve done so, both the PRIMARY and
--         SECONDARY accounts will have the corrected SQL for you to run.

-- 10.1.7  Replace text within your SQL script of the PRIMARY account.
--         From the URL of your Snowsight worksheet in the PRIMARY account, copy
--         the ACCOUNT_NAME portion of the URL (8 characters).
--         In your SQL script for this lab on the PRIMARY account, use the
--         search option (top right corner of editor - magnifying glass icon),
--         select the up arrow button, to find and replace all instances of the
--         string [PRIMARY-ACCOUNT] with your accountname you just copied from
--         the PRIMARY account: e.g., abc12345.

-- 10.1.8  Replace text within your SQL script for the SECONDARY account.
--         From the URL of your Snowsight worksheet in the SECONDARY account,
--         copy the ACCOUNT_NAME portion of the URL.
--         While still editing the script loaded in your PRIMARY account, use
--         the search option to replace all instances of the string [SECONDARY-
--         ACCOUNT] with your corrected accountname of the SECONDARY account:
--         e.g., xyz12345.
--         Once you have done the global find-and-replace actions for the
--         [PRIMARY-ACCOUNT] and [SECONDARY-ACCOUNT], your SQL script should be
--         ready to run. Copy and paste the entire corrected SQL script (right
--         click Select All, right click Copy) from your PRIMARY account into
--         the blank worksheet in your SECONDARY account, and proceed with the
--         remainder of this lab.
--         As you work through this lab, pay special attention to ensure you run
--         steps preceded by PRIMARY in the PRIMARY account, and steps preceded
--         by SECONDARY in the SECONDARY account.
--         For this lab, the primary and secondary accounts are in the same
--         region. However, you can have the secondary account on a different
--         cloud provider, or in a different region, from the primary account.
--         They must however be in the same organization.

-- 10.2.0  Set Up the Primary Database

-- 10.2.1  On the PRIMARY account, create a database and objects to replicate.

-- PRIMARY --
USE ROLE accountadmin;

CREATE OR REPLACE WAREHOUSE RABBIT_repl_wh  
   WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300;
CREATE OR REPLACE DATABASE RABBIT_repl_db;
CREATE SCHEMA IF NOT EXISTS repl_schema;
USE RABBIT_repl_db.repl_schema;

-- create a table with 1000 rows
CREATE OR REPLACE TABLE marketing_a
    ( cust_number INT, cust_name CHAR(50), cust_address VARCHAR(100),
      cust_purchase_date DATE ) CLUSTER BY (cust_purchase_date)
AS (  SELECT UNIFORM(1,999,RANDOM(10002)),
             UUID_STRING(),
             UUID_STRING(),
             CURRENT_DATE
      FROM TABLE(GENERATOR(ROWCOUNT => 1000))
);

-- create a procedure to insert 100 rows into the table
CREATE OR REPLACE PROCEDURE INSERT_MARKETING_ROWS()
RETURNS VARCHAR NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var result = "";
try {
    var sql_command =
        "INSERT INTO MARKETING_A SELECT UNIFORM(1,999,RANDOM(10002)), UUID_STRING(), UUID_STRING(), CURRENT_DATE FROM TABLE(GENERATOR(ROWCOUNT => 100))"
    stmt = snowflake.createStatement(
        {sqlText: sql_command});
    rs = stmt.execute();
    }
catch (err) {
    result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    }
return result;
$$
;
-- PRIMARY --


-- 10.2.2  View the accounts in your organization that have been linked for
--         replication.

SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER('ITHCHWU.SCB61968', 'ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');

SHOW ACCOUNTS;

drop REPLICATION GROUP replgrp_rnd_demo;

CREATE REPLICATION GROUP replgrp_rnd_demo
    OBJECT_TYPES = DATABASES
    ALLOWED_DATABASES = MOVIELENS,coderepo
    ALLOWED_ACCOUNTS = ITHCHWU.SECONDARY
    REPLICATION_SCHEDULE = '1 MINUTE';

alter REPLICATION GROUP replgrp_rnd_demo   SUSPEND ; 

alter REPLICATION GROUP replgrp_rnd_demo set ALLOWED_DATABASES = MOVIELENS,coderepo;

create schema tpch_1000 ;

create table tpch_1000.customer as select * from snowflake_sample_data.tpch_1000.customer;

select * from information_schema.tables  where table_schema='TPCH_SF1000';

create table tpch_1000.PARTSUPP as select * from snowflake_sample_data.TPCH_SF1000.PARTSUPP;
create table tpch_1000.CUSTOMER as select * from snowflake_sample_data.TPCH_SF1000.CUSTOMER;
create table tpch_1000.SUPPLIER as select * from snowflake_sample_data.TPCH_SF1000.SUPPLIER;
create table tpch_1000.ORDERS as select * from snowflake_sample_data.TPCH_SF1000.ORDERS;
create table tpch_1000.PART as select * from snowflake_sample_data.TPCH_SF1000.PART;
create table tpch_1000.NATION as select * from snowflake_sample_data.TPCH_SF1000.NATION;
create table tpch_1000.LINEITEM as select * from snowflake_sample_data.TPCH_SF1000.LINEITEM;
create table tpch_1000.REGION as select * from snowflake_sample_data.TPCH_SF1000.REGION;

insert into movielens.dev.my_first_dbt_model select max(id)+1 from movielens.dev.my_first_dbt_model;

SELECT *
    FROM TABLE(INFORMATION_SCHEMA.DATABASE_REFRESH_PROGRESS('MOVIELENS'));       

-- PRIMARY --
USE ROLE accountadmin;

SHOW  REPLICATION ACCOUNTS LIKE '%[PRIMARY-ACCOUNT]%';
SHOW  REPLICATION ACCOUNTS LIKE '%[SECONDARY-ACCOUNT]%';
-- PRIMARY --

--         Examine the results. Locate the organization_name and account_name
--         columns to verify they contain the Primary and Secondary accounts
--         provided by your instructor.

-- 10.2.3  Create a new role to act as the Replication Administrator role.
--         This new role is not strictly necessary, as all actions required for
--         Replication can be performed using the ACCOUNTADMIN role. We are
--         creating a new role to illustrate its possible use.

-- PRIMARY --
-- Create a new role to act as the Replication Administration Role
CREATE ROLE RABBIT_repl_role;
GRANT  ROLE RABBIT_repl_role to user RABBIT;

-- Grant the necessary privileges to this new role
GRANT USAGE ON DATABASE RABBIT_repl_db           TO ROLE RABBIT_repl_role;
GRANT USAGE ON SCHEMA RABBIT_repl_db.repl_schema TO ROLE RABBIT_repl_role;
GRANT USAGE ON WAREHOUSE RABBIT_repl_wh          TO ROLE RABBIT_repl_role;
GRANT SELECT ON ALL TABLES  IN SCHEMA repl_schema TO ROLE RABBIT_repl_role;
GRANT CREATE FAILOVER GROUP ON ACCOUNT            TO ROLE RABBIT_repl_role;
GRANT MONITOR  ON ACCOUNT                         TO ROLE RABBIT_repl_role;
-- PRIMARY --


-- 10.2.4  Create the Failover Group.
--         Replication groups and failover groups
--         A replication group is a defined collection of objects in a source
--         account that are replicated as a unit to one or more target accounts.
--         Replication groups provide read-only access for the replicated
--         objects.
--         A failover group is a replication group that can also fail over. A
--         secondary failover group in a target account provides read-only
--         access for the replicated objects. When a secondary failover group is
--         promoted to become the primary failover group, read-write access is
--         available. Any target account specified in the list of allowed
--         accounts in a failover group can be promoted to serve as the primary
--         failover group.
--         You will now use your new role to create the Failover Group to
--         replicate our database.

-- PRIMARY --
USE ROLE RABBIT_repl_role;

DROP FAILOVER GROUP IF EXISTS RABBIT_fg;
CREATE FAILOVER GROUP RABBIT_fg
   OBJECT_TYPES = DATABASES
   ALLOWED_DATABASES = RABBIT_repl_db
   ALLOWED_ACCOUNTS = [PRIMARY-ACCOUNT],[SECONDARY-ACCOUNT]
   IGNORE EDITION CHECK
   REPLICATION_SCHEDULE = '2 MINUTE';
-- PRIMARY --

--         The OBJECT_TYPES in the command above could also contain any of the
--         following (comma seperated):
--         ACCOUNT PARAMETERS: All account-level parameters. This includes
--         account parameters and parameters that can be set for your account.
--         DATABASES: Add database objects to the list of object types. If
--         database objects are included in the list of specified object types,
--         the ALLOWED_DATABASES parameter must be set.
--         INTEGRATIONS: Currently, only security, API, storage, external
--         access, and certain types of notification integrations are supported.
--         If integration objects are included in the list of specified object
--         types, the ALLOWED_INTEGRATION_TYPES parameter must be set.
--         NETWORK POLICIES: All network policies in the source account.
--         RESOURCE MONITORS: All resource monitors in the source account.
--         ROLES: All roles in the source account. Replicating roles implicitly
--         includes all grants for object types included in the replication
--         group. For example, if ROLES is the only object type that is
--         replicated, then only hierarchies of roles (i.e. roles granted to
--         other roles) are replicated to target accounts. If the USERS object
--         type is also included, then role grants to users are also replicated.
--         SHARES: Add share objects to the list of object types. If share
--         objects are included in the list of specified object types, the
--         ALLOWED_SHARES parameter must be set.
--         USERS: All users in the source account.
--         WAREHOUSES: All warehouses in the source account.
--         We will only be replicating DATABASES in this exercise, as all
--         students are using the same Account and the same objects cannot be
--         included in more than one Failover Group.

-- 10.2.5  Check that our Failover Group created successfully.

-- PRIMARY --
SHOW FAILOVER GROUPS;
SHOW DATABASES IN FAILOVER GROUP RABBIT_fg;

-- Check that we have data in our MARKETING_A table
SELECT COUNT(*) FROM RABBIT_repl_db.repl_schema.marketing_a;
-- 1,000 rows
-- PRIMARY --


-- 10.3.0  Create and Replicate to the Secondary Database
--         In this exercise, you will perform the steps to create the secondary
--         database and replicate the database from the primary account to the
--         secondary account.
--         For this part of the exercise you will be mostly working with your
--         script loaded in your Secondary Account’s worksheet.

-- 10.3.1  Prepare the Secondary Account.

-- SECONDARY --
USE ROLE accountadmin;

CREATE WAREHOUSE IF NOT EXISTS RABBIT_repl_wh  
   WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300;

USE WAREHOUSE RABBIT_repl_wh;
-- SECONDARY --


-- 10.3.2  Create the Failover Group on the Secondary Account.

-- SECONDARY --
USE ROLE ACCOUNTADMIN;

DROP FAILOVER GROUP IF EXISTS RABBIT_fg;
DROP DATABASE IF EXISTS RABBIT_repl_db;
CREATE FAILOVER GROUP RABBIT_fg
   AS REPLICA OF [PRIMARY-ACCOUNT].RABBIT_fg;
-- SECONDARY --

--         This should start the database replication on the 2 minute schedule
--         set in the Failover Group. When the primary database is replicated, a
--         snapshot of its database objects and data is transferred to the
--         secondary database.

-- 10.3.3  Check the Failover Group has created successfully.

-- SECONDARY --
SHOW FAILOVER GROUPS;
-- Should show the Failover Group in the source and the target

SHOW DATABASES IN FAILOVER GROUP RABBIT_fg;
-- Should just show the one database being replicated
-- SECONDARY --


-- 10.3.4  Query the secondary database to verify the replication has completed.
--         The COUNT(CUST_NUMBER) value should be 1000.

-- SECONDARY --
USE ROLE accountadmin;

SELECT COUNT(CUST_NUMBER) FROM RABBIT_repl_db.repl_schema.marketing_a;
-- 1,000 rows

-- If you do not get a result above, or are not prepared to wait for the 2 minute replication schedule, 
-- then use this command to force a refresh of the replication, then try the SELECT (above) again
ALTER FAILOVER GROUP RABBIT_fg REFRESH;

USE SCHEMA RABBIT_repl_db.repl_schema;

SHOW TABLES;                         
-- Should list the MARKETING_A table

SHOW PROCEDURES like '%MARKETING%';  
-- Should list the procedure INSERT_MARKETING_ROWS
-- SECONDARY --

--         If you are not getting the results indicated and you have refreshed
--         the Failover Group then seek assistance from your instructor

-- 10.3.5  Verify that the replica is read-only.

-- SECONDARY --
USE WAREHOUSE RABBIT_repl_wh;

-- Attempt to insert some rows into our table
CALL INSERT_MARKETING_ROWS();
-- NOTE: This will fail as the secondary copy is read-only

-- SECONDARY --


-- 10.4.0  Test the Replication
--         In this part of the exercise we will make changes to the Primary
--         database and observe the changes are being made to our Secondary
--         database.

-- 10.4.1  Create some new objects in our Primary database.

-- PRIMARY --
-- Set the context
USE ROLE accountadmin;
USE WAREHOUSE RABBIT_repl_wh;
USE SCHEMA RABBIT_repl_db.repl_schema;

-- We will create some new objects in our database
CREATE SCHEMA my_test_schema;
CREATE TABLE  my_test_table (c1 number);
CREATE STAGE  my_test_stage;

CREATE FUNCTION my_test_function()
RETURNS NUMBER(2,0)
as
$$
   select 1
$$;

CREATE FILE FORMAT my_test_ff 
   TYPE = JSON;

-- Now we will insert 100 more rows into our MARKETING_A table
USE SCHEMA RABBIT_repl_db.repl_schema;

CALL  INSERT_MARKETING_ROWS();
SELECT COUNT(*) FROM marketing_a;
-- 1,100 rows
-- PRIMARY --


-- 10.4.2  Check our changes replicated successfully.
--         You will now switch to the Secondary account to check whether our
--         changes to the Primary database have replicated to our Secondary
--         database.
--         Whilst on the worksheet in the secondary account, expand the
--         Databases browser on the left hand side. Use the circular arrow
--         Refresh button in the top right corner of the navigation pane.
--         Expand the RABBIT_repl_db and the MY_TEST_SCHEMA. Check that all of
--         the objects you created have replicated successfully.
--         If the replication hasn’t completed after a 2 minute wait then force
--         the replication with this command:

-- SECONDARY --
USE ROLE accountadmin;
ALTER FAILOVER GROUP RABBIT_fg REFRESH;
-- SECONDARY --


-- 10.4.3  Check the row count on our replicated table.

-- SECONDARY --
SELECT COUNT(*) FROM RABBIT_repl_db.repl_schema.marketing_a;
-- 1,100 rows
-- SECONDARY --


-- 10.4.4  Now we will delete some rows from our primary database.

-- PRIMARY --
-- Delete 200 rows from our Marketing_A table
USE ROLE accountadmin;

DELETE FROM RABBIT_repl_db.repl_schema.marketing_a a
   USING (select cust_name from RABBIT_repl_db.repl_schema.marketing_a SAMPLE (200 ROWS)) b
     WHERE a.cust_name = b.cust_name;
-- PRIMARY --


-- 10.4.5  Wait 2 minutes and then check the row count on our replicated table.

-- SECONDARY --
SELECT COUNT(*) FROM RABBIT_repl_db.repl_schema.marketing_a;
-- 900 rows
-- SECONDARY --


-- 10.5.0  Monitor Replication
--         In this exercise, you will perform the steps to determine the current
--         status of the initial database replication or a subsequent secondary
--         database refresh.
--         Most of this exercise will be done on the Secondary account.
--         Some of the queries performed here could have a lag of up to 3 hours.

-- 10.5.1  Check the progress and history.

-- SECONDARY --
USE ROLE accountadmin;

-- Check the progress
SELECT * 
  FROM TABLE 
(RABBIT_repl_db.information_schema.replication_group_refresh_progress('RABBIT_fg'));

-- Check the history
SELECT * 
  FROM TABLE 
(RABBIT_repl_db.information_schema.replication_group_refresh_history('RABBIT_fg'));

-- Check the total number of bytes that have been replicated
SELECT sum(value:totalBytesToReplicate) as sum_database_bytes
  FROM TABLE 
(RABBIT_repl_db.information_schema.replication_group_refresh_history('RABBIT_fg')) rh,
       LATERAL FLATTEN(input => rh.total_bytes:databases)
 WHERE rh.start_time >= current_date - interval '30 days';
-- SECONDARY --

--         These SNOWFLAKE.ACCOUNT_USAGE views also provide additional
--         information, but with a longer lag (up to 3 hours or more). These
--         queries may not produce any results until this has updated.

-- SECONDARY --
USE ROLE accountadmin;

-- See the details of credits consumed by replication
SELECT * 
  from snowflake.account_usage.replication_group_usage_history;

-- Get the total credits consumed by a failover group in the past 30 days
SELECT sum(credits_used) as credits_used, SUM(bytes_transferred) as bytes_transferred
  FROM snowflake.account_usage.replication_group_usage_history
 WHERE replication_group_name = 'RABBIT_FG'
   AND start_time >= current_date - interval '30 days';

-- SECONDARY --


-- 10.5.2  Monitor Replication from Snowsight Administration Dashboard.
--         Navigate to Admin -> Accounts -> Replication in the Snowsight
--         Dashboard using the ACCOUNTADMIN role.

-- 10.5.3  Promote the secondary database.

-- SECONDARY --
USE ROLE accountadmin;

-- Attempt to insert some rows into our table
CALL insert_marketing_rows();
-- This will fail as the secondary copy is still read-only

-- Switch the failover to make the secondary the PRIMARY
ALTER FAILOVER GROUP RABBIT_fg PRIMARY;

-- Now attempt to insert some rows into our table again
CALL insert_marketing_rows();
-- This should work because the secondary is now the primary and no longer read-only

select count(*) from RABBIT_repl_db.repl_schema.marketing_a;
-- 1,000

-- Use this command to check that the is_primary attribute has switched correctly
SHOW FAILOVER GROUPS;

-- SECONDARY --


-- 10.5.4  Check to see that is_primary is TRUE for the secondary account.
--         From the PRIMARY, notice that currently the failover is not bi-
--         directional. The old primary does not become a secondary
--         automatically. It must be refreshed.

-- PRIMARY --
USE ROLE accountadmin;
SELECT COUNT(*) FROM RABBIT_repl_db.repl_schema.marketing_a;
-- 900 rows (the new primary has 1000 rows now)

use role RABBIT_repl_role;
ALTER FAILOVER GROUP RABBIT_fg REFRESH;
-- Switch the failover to make the original PRIMARY the PRIMARY again
select count(*) from RABBIT_repl_db.repl_schema.marketing_a;
-- 1000 
-- fail them back
ALTER FAILOVER GROUP RABBIT_fg PRIMARY;
-- Use this command to check that the is_primary attribute has switched correctly
SHOW FAILOVER GROUPS;
-- PRIMARY --


-- 10.6.0  Clean Up

-- 10.6.1  Run the following statements on the ORIGINAL secondary account.

-- SECONDARY --
USE ROLE accountadmin;
DROP FAILOVER GROUP IF EXISTS RABBIT_fg;
DROP WAREHOUSE RABBIT_repl_wh;
-- SECONDARY --


-- 10.6.2  Run the following statements on the ORIGINAL primary account.

-- PRIMARY --
USE ROLE RABBIT_repl_role;
DROP FAILOVER GROUP IF EXISTS RABBIT_fg;
USE ROLE accountadmin;
DROP DATABASE IF EXISTS RABBIT_repl_db;
DROP WAREHOUSE RABBIT_repl_wh;
-- PRIMARY --


-- 10.6.3  Review Replication Considerations.
--         See the documentation at: https://docs.snowflake.com/en/user-
--         guide/account-replication-considerations

-- 10.7.0  Key Takeaways
--         REPLICATION can copy data from one account to another
--         Replication duplicates the data and objects across regions and/or
--         cloud vendors.
--         Failover can be performed bidirectionally as needed.
