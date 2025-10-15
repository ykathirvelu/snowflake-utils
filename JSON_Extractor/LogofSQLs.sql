USE ROLE ACCOUNTADMIN;
--lets create a new catalog integration connecting to the SNowflake Open Catalog
CREATE OR REPLACE CATALOG INTEGRATION polaris_catalog
   CATALOG_SOURCE=POLARIS 
   TABLE_FORMAT=ICEBERG 
   CATALOG_NAMESPACE='default' 
   REST_CONFIG = (
     CATALOG_URI ='https://ipfscuf-sfocyk.snowflakecomputing.com/polaris/api/catalog' 
     WAREHOUSE = 'demo_sf_open_catalog'
   )
   REST_AUTHENTICATION = (
     TYPE=OAUTH 
     OAUTH_CLIENT_ID='1J8Lz1d9ghwNzHqErdQql7CY2a8=' 
     OAUTH_CLIENT_SECRET='AKD0Bmx/MIZXWnroWXihxoNPVnTKNyjE61cyt22mdhI=' 
     OAUTH_ALLOWED_SCOPES=('PRINCIPAL_ROLE:ALL') 
   ) 
ENABLED=true;

describe CATALOG INTEGRATION polaris_catalog;
--provide access to sysadmin role
GRANT USAGE ON INTEGRATION polaris_catalog to ROLE SYSADMIN;

USE ROLE ICEBERG_LAB;
USE DATABASE ICEBERG_LAB;
USE SCHEMA ICEBERG_LAB;
select * from ICEBERG_CUSTOMER_POLARIS limit 100
--with the catlaog integration created,
--we can now create a table which will auto sync to Snowflake Open Catalog
CREATE OR REPLACE ICEBERG TABLE ICEBERG_CUSTOMER_POLARIS
  CATALOG='SNOWFLAKE'
  EXTERNAL_VOLUME='ext_vol_demo_sfmanaged_iceberg02'
  BASE_LOCATION='customer_plaris'
  CATALOG_SYNC = 'polaris_catalog' AS (
    select 
           --customer info
           ROW_NUMBER() OVER (ORDER BY seq4())  ID,
           UPPER(faker('en_US', 'bothify', '##########' )::string) CUSTOMER_NUM,
           faker('en_US', 'first_name', '')::string first_name,
           faker('en_US', 'last_name', '')::string last_name,
           faker('en_US', 'email', '')::string email,
           faker('en_US', 'city', '')::string city,
           faker('en_US', 'state', '')::string state,
           faker('en_US', 'current_country_code', '')::string country,
           faker('en_US', 'url', '')::string website,
           faker('en_US','phone_number', '')::string phone_1,
           faker('en_US','date_of_birth', '18')::date DOB,
           faker('en_US','company', '')::string company
           --profile
    from table(generator(rowcount => 5000)));

select * from information_schema.tables where table_name like '%ICE%'


---------------------------------------

# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark import functions as sf

def main(session: snowpark.Session): 
    # Create a DataFrame representing the 'orders' table
    df_orders = session.read.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS")

    # Perform aggregation on the DataFrame
    df_orders_agg = (
        df_orders
        .groupBy(df_orders.o_custkey)
        .agg(
            sf.count(df_orders.o_orderkey).alias("order_count"),
            sf.sum(df_orders.o_totalprice).alias("total_price")
        )
    )

    df_orders_agg = df_orders_agg.select("o_custkey", "order_count", "total_price")

    df_customer = session.read.table("ICEBERG_LAB.ICEBERG_LAB.CUSTOMER_ICEBERG")    
    df_nation = session.read.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION")

    df_nation_customer = df_customer.join(df_nation, df_customer.col("c_nationkey") == df_nation.col("n_nationkey")).select("c_custkey", df_nation["n_nationkey"].as_("nationkey"), df_nation["n_name"].as_("nation"), df_nation["n_regionkey"].as_("regionkey"))
    df_nation_customer_orders_agg = df_nation_customer.join(df_orders_agg, df_nation_customer.col("c_custkey") == df_orders_agg.col("o_custkey")).select("regionkey", "nationkey", "nation", df_nation_customer["c_custkey"].as_("custkey"), "order_count", "total_price")

    df_nation_customer_orders_agg = df_nation_customer_orders_agg.select("regionkey", "nationkey", "nation", "custkey", "order_count", "total_price")

    # Save result to iceberg table
    df_nation_customer_orders_agg.write.mode("append").save_as_table("nation_orders_iceberg")
    return df_nation_customer_orders_agg;

alter user sfadmin set disable_mfa=true;

create or replace table nation_test as select * from movielens.tpch_1000.nation where 1=2;

CREATE or replace STREAM strm_movilens_nation ON TABLE nation_test;  

insert into nation_test select * from movielens.tpch_1000.nation where n_nationkey between 1 and 4;

select * from strm_movilens_nation order by n_nationkey; 

update nation_test set N_NAME=lower(N_NAME) where n_nationkey between 1 and 4;

select * from strm_movilens_nation order by n_nationkey; 

update nation_test set N_NAME=upper(N_NAME) where n_nationkey between 1 and 4;

select * from strm_movilens_nation order by n_nationkey; 

delete from nation_test where n_nationkey between 0 and 2;

select * from strm_movilens_nation order by n_nationkey;  

create OR REPLACE temporary table t_nation as select N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT from strm_movilens_nation;

select * from strm_movilens_nation order by n_nationkey; 

insert into nation_test select * from movielens.tpch_1000.nation where n_nationkey between 0 and 2;

select * from strm_movilens_nation order by n_nationkey; 

update nation_test set N_NAME=lower(N_NAME) where n_nationkey between 3 and 4;

select * from strm_movilens_nation order by n_nationkey; 

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

select count(1) 
from snowflake_sample_data.tpch_sf1000.lineitem;

select count(li.l_orderkey)
from snowflake_sample_data.tpch_sf1000.lineitem li
inner join snowflake_sample_data.tpch_sf1000.orders ord on (li.l_orderkey=ord.o_orderkey)
inner join snowflake_sample_data.tpch_sf1000.part part on (li.l_partkey=part.p_partkey)
inner directed join snowflake_sample_data.tpch_sf1000.partsupp ps on (li.l_suppkey=ps.ps_suppkey and li.l_partkey=ps.ps_partkey)
where part.p_brand like 'Brand#1%'; --- 4m 16s

select count(li.l_orderkey)
from snowflake_sample_data.tpch_sf1000.part part 
inner directed join snowflake_sample_data.tpch_sf1000.lineitem li on (li.l_partkey=part.p_partkey)
inner directed join snowflake_sample_data.tpch_sf1000.orders ord on (li.l_orderkey=ord.o_orderkey)
inner directed join snowflake_sample_data.tpch_sf1000.partsupp ps on (li.l_suppkey=ps.ps_suppkey and li.l_partkey=ps.ps_partkey)
where part.p_brand like 'Brand#1%';  --3m 60s

select distinct part.p_brand from snowflake_sample_data.tpch_sf1000.part;

select ps_partkey,ps_suppkey from snowflake_sample_data.tpch_sf1000.partsupp group by ps_partkey,ps_suppkey having count(1)>1 limit 1000;


-----------------------

CREATE OR REPLACE TABLE MYTABLE2 (
COL1 NUMBER AUTOINCREMENT START 1 INCREMENT 1, 
COL2 VARCHAR, COL3 VARCHAR);

insert into MYTABLE2 (col2,col3) values ('c','y');

insert into MYTABLE2 (col1,col2,col3) values (4,'a','c');

select * from MYTABLE2;

with cte_hash
as
(
select 1 as col
union
select 10 as col
union
select 100 as col
union
select 1000 as col
union
select 10000 as col
union
select 100000 as col
union
select 1000000 as col
union
select 10000000 as col
)
select col,hash(col) as Hash_key,length(cast(col as varchar)) as length_of_col,length(cast(hash(col) as varchar)) as length_of_hash_key,
from cte_hash;

show schemas in database movielens;

SHOW TABLES in schema public;

SHOW TABLES in dev;


SHOW GRANTS TO USER sfadmin;

SELECT distinct ROLE
FROM
    SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
WHERE
    GRANTEE_NAME ='SFADMIN';

SHOW GRANTS TO USER "SFADMIN";    

SELECT distinct ROLE FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS WHERE GRANTEE_NAME ='SFADMIN';

USE SECONDARY ROLES ALL;

------------

 SELECT  replace(REGEXP_REPLACE(f.path, '\\[[0-9]+\\]', ''),'.',':') AS path,
  TYPEOF(f.value) AS JSON_Type,
  COUNT(*) AS "Count"
FROM raw_cypro_primary_quote,
LATERAL FLATTEN(payload, RECURSIVE=>true) f
GROUP BY 1, 2 ORDER BY 1, 2;

SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES
WHERE 1=1
ORDER BY 1,2,3;

insert overwrite into STG_CYPRO_PRIMARY_OPP_REL(src_file_name,
persistedInWormStorage,
createdBy,
created,
id,
submission_id,
submission_moduleId,
submission_userId,
submission_created,
submission_modified,
submission_platformVersionAtModified,
submission_moduleArchiveIdAtCreated,
submission_moduleArchiveIdAtModified,
accountNumber,
proposedEffectiveDate,
isRenewal,
isRenewable,
receivedDate,
status,
number_val,
statusDescription,
producerName,
receivedDateFormatted,
effectiveDateFormatted,
lastModifiedDate,
lastModifiedDateFormatted,
submissionType,
producerEmail,
daysToInception,
expiringPolicyNumber,
expiringSubmissionNumber,
producer,
archBranchId,
archBranch,
archBranchDescription,
competitorPrice,
termInMonths,
under_writer_email,
under_writer_fullName,
under_writer_userName,
naicsCode,
naicsDescription,
dunsNumber,
producer_POC_name,
producer_POC_email,
producer_POC_phoneNumber,
businessSubDivisionDescription,
businessSubDivisionExpired,
businessSubDivision,
agency,
agency_producerState,
agency_producerCountry,
limitDeductibleId,
productFamily,
productFamilyDescription,
insuranceType)
 select r.src_filename as src_file_name,
r.payload:persistedInWormStorage as persistedInWormStorage,
r.payload:createdBy::STRING as createdBy,
r.payload:created::STRING as created,
r.payload:id::STRING                                      as id,
r.payload:submission:id::STRING                           as submission_id,
r.payload:submission:moduleId::STRING                     as submission_moduleId,
r.payload:submission:userId::STRING                       as submission_userId,
r.payload:submission:created::STRING                      as submission_created,
r.payload:submission:modified::STRING                     as submission_modified,
r.payload:submission:platformVersionAtModified::STRING                     as submission_platformVersionAtModified,
r.payload:submission:moduleArchiveIdAtCreated::STRING                      as submission_moduleArchiveIdAtCreated,
r.payload:submission:moduleArchiveIdAtModified::STRING                     as submission_moduleArchiveIdAtModified,
r.payload:submission:"data":rawData:"accountNumber"::STRING                                    as accountNumber,
r.payload:submission:"data":rawData:"proposedEffectiveDate"::STRING                           as proposedEffectiveDate,
r.payload:submission:"data":rawData:"isRenewal"::STRING                            as isRenewal,
r.payload:submission:"data":rawData:"isRenewable"::STRING                       as isRenewable,
r.payload:submission:"data":rawData:"receivedDate"::STRING                            as receivedDate,
r.payload:submission:"data":rawData:"status"::STRING                            as status,
r.payload:submission:"data":rawData:"number"::STRING        as number_val,
r.payload:submission:"data":rawData:"statusDescription"::STRING       as statusDescription,
r.payload:submission:"data":rawData:"producerName"::STRING                          as producerName,
r.payload:submission:"data":rawData:"receivedDateFormatted"::STRING                           as receivedDateFormatted,
r.payload:submission:"data":rawData:"effectiveDateFormatted"::STRING                        as effectiveDateFormatted,
r.payload:submission:"data":rawData:"lastModifiedDate"::STRING                          as lastModifiedDate,
r.payload:submission:"data":rawData:"lastModifiedDateFormatted"::string  as lastModifiedDateFormatted,
r.payload:submission:"data":rawData:"submissionType"::string  as submissionType,
r.payload:submission:"data":rawData:"producerEmail"::string  as producerEmail,
r.payload:submission:"data":rawData:"daysToInception"::string  as daysToInception,
r.payload:submission:"data":rawData:"expiringPolicyNumber"::string  as expiringPolicyNumber,
r.payload:submission:"data":rawData:"expiringSubmissionNumber"::string  as expiringSubmissionNumber,
r.payload:submission:"data":rawData:"producer"::string  as producer,
r.payload:submission:"data":rawData:"archBranchId"::string  as archBranchId,
r.payload:submission:"data":rawData:"archBranch"::string  as archBranch,
r.payload:submission:"data":rawData:"archBranchDescription"::string  as archBranchDescription,
r.payload:submission:"data":rawData:"competitorPrice"::string  as competitorPrice,
r.payload:submission:"data":rawData:"termInMonths"::string  as termInMonths,
r.payload:submission:"data":rawData:"underwriter":email::string  as under_writer_email,
r.payload:submission:"data":rawData:underwriter:"fullName"::string  as under_writer_fullName,
r.payload:submission:"data":rawData:underwriter:"userName"::string  as under_writer_userName,
r.payload:submission:"data":rawData:"naicsDetails":naicsCode::string  as naicsCode,
r.payload:submission:"data":rawData:"naicsDetails":naicsDescription::string  as naicsDescription,
r.payload:submission:"data":rawData:"naicsDetails":dunsNumber::string  as dunsNumber,
r.payload:submission:"data":rawData:"producerPoc":name::string  as producer_POC_name,
r.payload:submission:"data":rawData:"producerPoc":email::string  as producer_POC_email,
r.payload:submission:"data":rawData:"producerPoc":phoneNumber::string  as producer_POC_phoneNumber,
r.payload:submission:"data":rawData:"productDetails":businessSubDivisionDescription::string  as businessSubDivisionDescription,
r.payload:submission:"data":rawData:"productDetails":businessSubDivisionExpired::string  as businessSubDivisionExpired,
r.payload:submission:"data":rawData:"productDetails":businessSubDivision::string  as businessSubDivision,
r.payload:submission:"data":rawData:"agencyObject":agency::string  as agency,
r.payload:submission:"data":rawData:"agencyObject":producerAddress:producerState::string  as agency_producerState,
r.payload:submission:"data":rawData:"agencyObject":producerAddress:producerCountry::string  as agency_producerCountry,
rd.value:limitDeductibleId  as limitDeductibleId,
rd.value:productFamily  as productFamily,
rd.value:productFamilyDescription  as productFamilyDescription,
rd.value:insuranceType  as insuranceType from RAW_CYPRO_PRIMARY_OPP r,
LATERAL FLATTEN(input => r.payload:submission:"data":rawData:limitDeductibles) rd;

-------------

CREATE or replace TABLE table_load_status (
    tbl_name STRING,
    status STRING,
    load_start_date timestamp,
    load_end_date timestamp
);

CREATE OR REPLACE PROCEDURE update_load_status_to_completed(table_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO table_load_status (tbl_name, status,load_start_date) VALUES (:table_name, 'L',current_timestamp());
    CALL SYSTEM$WAIT(60, 'SECONDS');
    UPDATE table_load_status SET status = 'C',load_end_date=current_timestamp() WHERE tbl_name = :table_name;

    RETURN 'Status updated to completed for ' || table_name;
END;
$$;

CREATE OR REPLACE PROCEDURE PROCESS_ARRAY_INPUT(INPUT_ARRAY ARRAY)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    RES RESULTSET DEFAULT (
        SELECT
            cast(VALUE as varchar) AS array_element -- Cast to VARIANT to handle mixed data types if needed
        FROM
            TABLE(FLATTEN(INPUT => :INPUT_ARRAY))
    );
    v_element varchar;
    V_RESULT_ARRAY varchar;
BEGIN
    FOR rec IN RES DO
        LET v_element := cast(rec.array_element as varchar);
        --insert into table_load_status select rec.array_element,'L';
        async(call update_load_status_to_completed(:v_element) );
   END FOR;
   AWAIT ALL;
    RETURN 'LOAD COMPLETED';
END;
$$;

CALL PROCESS_ARRAY_INPUT(ARRAY_CONSTRUCT('VW_SSI_DEPOSITS_COLLATERAL_PCTS_SPLIT', 'DIM_GT_PRODUCT', 'DIM_BALANCE_SHEET_TYPE'));

select * from table_load_status;


create database coderepo;
create schema coderepo;
 
CREATE STAGE MOVIELENS.MOVIES.ob_files
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

create table md_ob_extract_config
(
process_name varchar(100),
table_name varchar(100),
extract_location varchar(300),
file_prefix varchar(50),
extract_type varchar(20),
extract_sql varchar,
is_active varchar(1),
dt_created timestamp,
dt_modified timestamp
);

insert into md_ob_extract_config
select 'extInteractions', 'MOVIELENS.MOVIES.INTERACTIONS','MOVIELENS.MOVIES.ob_files','INTERACTIONS','T',null,'Y',current_timestamp,current_timestamp;

truncate table md_ob_extract_config;

call UNLOAD_FROM_CONFIG_BY_PROCESS_SQL('extInteractions');

list @MOVIELENS.MOVIES.ob_files;

CREATE OR REPLACE PROCEDURE UNLOAD_FROM_CONFIG_BY_PROCESS_SQL(IN_PROCESS_NAME varchar(100) )
RETURNS varchar
LANGUAGE SQL
AS
$$
DECLARE
    v_table_name varchar(100);
    v_stage_name varchar(300);
    v_file_prefix varchar(100);
    v_extract_type varchar(100);
    v_extract_sql varchar;
    v_date_folder varchar(100);
    v_copy_sql varchar; 
    my_exception EXCEPTION (-20002, 'My exception text');
BEGIN
    -- Step 1: Get config values
    SELECT table_name, extract_location, file_prefix, extract_type, extract_sql INTO 
           :v_table_name, :v_stage_name, :v_file_prefix, :v_extract_type, :v_extract_sql FROM MD_OB_EXTRACT_CONFIG 
    WHERE process_name = :IN_PROCESS_NAME AND is_active = 'Y';

    -- Step 2: Format date as DDMMYYYY
    select TO_CHAR(CURRENT_DATE, 'DDMMYYYY') into :v_date_folder;

    -- Step 3: Extract base table name
    LET v_clean_table varchar := SPLIT_PART(:v_table_name, '.', ARRAY_SIZE(SPLIT(:v_table_name, '.')));
 
    -- Step 4: Construct stage path
    LET v_stage_path varchar := '@' || :v_stage_name || '/' || :v_clean_table || '/' || :v_date_folder || '/' || :v_file_prefix;
    select :v_extract_type;
    -- Step 5: Construct COPY INTO SQL
    IF (:v_extract_type = 'T') THEN
        v_copy_sql := 'COPY INTO ' || :v_stage_path || '
            FROM ' || :v_table_name || '
            FILE_FORMAT = (TYPE = ''CSV'' FIELD_OPTIONALLY_ENCLOSED_BY = ''"'' COMPRESSION = ''GZIP'')
            OVERWRITE = TRUE
            HEADER = TRUE;
        ';
    ELSE
        IF (:v_extract_sql IS NULL OR TRIM(:v_extract_sql) = '') THEN
            RETURN 'extract_sql is missing for process: ' || :IN_PROCESS_NAME;
        END IF;
        v_copy_sql := 'COPY INTO ' || :v_stage_path || '
            FROM (' || :v_extract_sql || ')
            FILE_FORMAT = (TYPE = ''CSV'' FIELD_OPTIONALLY_ENCLOSED_BY = ''"'' COMPRESSION = ''GZIP'')
            OVERWRITE = TRUE
            HEADER = TRUE;
        ';
    END IF;
    --v_copy_sql_fnl := :v_copy_sql;
    -- Step 6: Execute the COPY INTO
    EXECUTE IMMEDIATE :v_copy_sql;
    -- Step 7: Return success
    RETURN 'Unload successful for process: ' || IN_PROCESS_NAME || ' to ' || v_stage_path;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Unload failed for process: ' || IN_PROCESS_NAME || '. Error:- '||SQLCODE || ': ' || SQLERRM;
END;
$$
;

CREATE OR REPLACE TABLE LETTERS (ID INT, LETTER STRING);
CREATE OR REPLACE STREAM STREAM_1 ON TABLE LETTERS;
CREATE OR REPLACE STREAM STREAM_2 ON TABLE LETTERS APPEND_ONLY = TRUE;

INSERT INTO LETTERS VALUES (1, 'A');
INSERT INTO LETTERS VALUES (2, 'B');
INSERT INTO LETTERS VALUES (3, 'C');
TRUNCATE TABLE LETTERS;
INSERT INTO LETTERS VALUES (4, 'D');
INSERT INTO LETTERS VALUES (5, 'E');
INSERT INTO LETTERS VALUES (6, 'F');
DELETE FROM LETTERS WHERE ID = 4;

SELECT COUNT (*) FROM STREAM_1;
SELECT COUNT (*) FROM STREAM_2;

select * from STREAM_1 order by id;

select SYSTEM$CLUSTERING_INFORMATION('LETTERS',3);

create or replace table MAIN_VW_DEP_CTE
(
LCR_LINE_ITEM varchar(20),
LEGAL_ENTITY_CODE varchar(2),
ISEOM varchar(1)
);

insert into MAIN_VW_DEP_CTE values ('73_940-73_1010','AB','Y');
insert into MAIN_VW_DEP_CTE values ('73_940-73_1010','AB','N');
insert into MAIN_VW_DEP_CTE values ('73_940-73_1011','XY','N');

create table VW_SSI_DEPOSITS_COLLATERAL_PCTS_SPLIT
(
LEGAL_ENTITY_CODE varchar(2),
ISEOM varchar(1)
);

insert into VW_SSI_DEPOSITS_COLLATERAL_PCTS_SPLIT
select LEGAL_ENTITY_CODE,ISEOM from
(select 'AB' LEGAL_ENTITY_CODE
union all
select 'XY'
union all 
select 'CD' ) as a ,
(select 'Y' as ISEOM
union all
select 'N' ) as b where 1=1;

select fact.*,COLLATERAL_PCTS.*
FROM MAIN_VW_DEP_CTE  FACT
       LEFT JOIN VW_SSI_DEPOSITS_COLLATERAL_PCTS_SPLIT COLLATERAL_PCTS            
                        ON  FACT.ISEOM = COLLATERAL_PCTS.ISEOM
                                    AND FACT.LEGAL_ENTITY_CODE = COLLATERAL_PCTS.LEGAL_ENTITY_CODE
                                    AND FACT.LCR_LINE_ITEM = '73_940-73_1010';

------------------------------------

create schema STARSP_FOSA_STAGE;
CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_FACT_CONTRACT_VALUES_ALL_DEPOSITS (
    AMT_MARKET_VAL STRING,
    AMT_PRINCIPAL_BOOK_VAL STRING,
    AMT_PRINCIPAL_NOTIONAL_VAL STRING,
    CUSTOMER_FTP_INTEREST_RATE STRING,
    DATADATE STRING,
    DATASOURCE STRING,
    ISEOM STRING,
    RATE_EXTERNAL_INTEREST STRING,
    RATE_FTP STRING,
    RECORD_TRADE_NUMBER STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_REFERENCE_RATE (
    INSTR_REFERENCE_RATE_IS_INTERNAL STRING,
    INSTR_REFERENCE_RATE_TENOR STRING,
    INSTR_RATE_TYPE_CODE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_FACT_OPERATIONAL_DEPOSIT (
    PCT_LCR_OD_RATIO STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_BAL_SHEET_ALM_LINE_ITEM (
    ALM_BS_LINE_ITEM_BK STRING,
    ALM_BS_LINE_ITEM_CODE STRING,
    ALM_BS_LINE_ITEM_DESC STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_DATE (
    DATE_FULL_DATE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_INSTRUMENT (
    INSTR_AMORT_AMOUNT STRING,
    INSTR_AMORT_FREQ_NUM STRING,
    INSTR_AMORT_FREQ_UNIT STRING,
    INSTR_AMORT_METHOD_CODE STRING,
    INSTR_CAP_MATURITY STRING,
    INSTR_CAP_RATE STRING,
    INSTR_CAP_START_DATE STRING,
    INSTR_DAY_COUNT_CONVENTION STRING,
    INSTR_FLOOR_MATURITY STRING,
    INSTR_FLOOR_RATE STRING,
    INSTR_FLOOR_START_DATE STRING,
    INSTR_HAS_MATERIAL_PENALTY STRING,
    INSTR_INTEREST_FLAG STRING,
    INSTR_INTR_PAYMENT_FREQ_NUM STRING,
    INSTR_INTR_PAYMENT_FREQ_UNIT STRING,
    INSTR_ISIN_EBA_ISSUER_CQS STRING,
    INSTR_LOCAL_REFERENCE_RATE_CD STRING,
    INSTR_LOAN_PERFORMING_LCL_CODE STRING,
    INSTR_PAYMENT_DAY_ADJUST_CODE STRING,
    INSTR_RATE_FIXING_FREQ_NUM STRING,
    INSTR_RATE_FIXING_FREQ_UNIT STRING,
    INSTR_RATE_TYPE_CODE STRING,
    INSTR_REMAINING_AT_MATURITY STRING,
    RECORD_SOURCE_SYSTEM STRING,
    INSTR_REFERENCE_RATE_SPREAD STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_BALANCE_SHEET_TYPE (
    BS_TYPE_ASSET_LIABILITY_CODE STRING,
    BS_TYPE_ON_OFF_BALANCE_CODE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_COUNTERPARTY (
    CPTY_COUNTRY STRING,
    CPTY_COUNTRY_EEA STRING,
    CPTY_ENTERPRISE_CAT STRING,
    CPTY_INTERNAL_IDENTIFIER STRING,
    CPTY_SECTOR_CAT_DESC STRING,
    CPTY_TRADING_PARTNER_LE_CODE STRING,
    CPTY_TRADING_PARTNER_NAME STRING,
    RECORD_CPTY_TYPE_COMMON STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_CURRENCY (
    CURRENCY_CODE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_MD_BUSINESS_ORGANISATION (
    ORG_IRRBB_BUSINESS_AREA STRING,
    ORG_LEVEL_3_CODE STRING,
    ORG_LEVEL_3_DESC STRING,
    ORG_LEVEL_4_CODE STRING,
    ORG_LEVEL_4_DESC STRING,
    ORG_LEVEL_5_CODE STRING,
    ORG_LEVEL_5_DESC STRING,
    ORG_LEVEL_6_CODE STRING,
    ORG_LEVEL_6_DESC STRING,
    ORG_LEVEL_7_CODE STRING,
    ORG_LEVEL_7_DESC STRING,
    ORG_LEVEL_8_CODE STRING,
    ORG_LEVEL_8_DESC STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_PORTFOLIO (
    PORTFOLIO_CODE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_TRADE_TYPE (
    TRADE_TYPE_CODE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_LEGAL_ENTITY (
    LEGAL_ENTITY_CODE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_GT_PRODUCT (
    GT_PRODUCT_COMMON_CODE STRING,
    GT_PRODUCT_LOCAL_CODE STRING,
    GT_PRODUCT_LOCAL_DESC STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_MD_PRODUCT (
    MD_PRODUCT_CLASS_DESC STRING,
    MD_PRODUCT_CODE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_COUNTERPARTY_ELIMINATION (
    CPTY_ELIM_CODE STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_DERIVED_CONTRACT_PROPERTIES_ALL_DEPOSITS (
    DEPOSIT_DGS_RATIO STRING,
    DEPOSIT_HIGH_VALUE_DEPOSIT STRING,
    DEPOSIT_IS_COVERED_BY_DGS STRING,
    DEPOSIT_IS_OPERATIONAL STRING,
    DEPOSIT_OPERATIONAL_RATIO STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_LCR_LINE_ITEM (
    LCR_LINE_ITEM STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_NSFR_LINE_ITEM (
    NSFR_LINE_ITEM STRING
);

CREATE OR REPLACE TABLE STARSP_FOSA_STAGE.SSI_DIM_IRRBB_CONTRACT_PROPERTIES (
    IRRBB_PRODUCT_TYPE STRING,
    IRRBB_RATE_INFO STRING,
    LT_COUNTERPARTY_TYPE_CODE STRING
);
-------------------------------

WITH CTE_FACT_BASE
AS
(;
select                  FACT.DATADATE,
                        FACT.ISEOM,
                        FACT.DATASOURCE,
						FACT.RATE_EXTERNAL_INTEREST,
						FACT.AMT_PRINCIPAL_BOOK_VAL,
                        FACT.AMT_MARKET_VAL,
                        FACT.AMT_PRINCIPAL_NOTIONAL_VAL,
                        FACT.RECORD_TRADE_NUMBER,
                        FACT.RATE_FTP,
                        FACT.CUSTOMER_FTP_INTEREST_RATE,	
						FACT.INSTRUMENT_ID,
						FACT.TRADE_NUMBER_ID,
						FACT.BALANCE_SHEET_ALM_LINE_ITEM_ID,
						FACT.NEXT_AMORT_PAYMENT_DATE_ID,
						FACT.BALANCE_SHEET_TYPE_ID,
						FACT.NEXT_INTEREST_PAYMENT_DATE_ID,
						FACT.COUNTERPARTY_ID,
						FACT.POSITION_CURRENCY_ID,
						FACT.MATURITY_DATE_ID,
						FACT.MD_BUSINESS_ORGANISATION_ID,
						FACT.PORTFOLIO_ID,
						FACT.NEXT_FIXING_DATE_ID,
						FACT.LAST_FIXING_DATE_ID,
						FACT.TRADE_TYPE_ID,
						FACT.LEGAL_ENTITY_ID,
						FACT.GT_PRODUCT_ID,
						FACT.MD_PRODUCT_ID,
						FACT.COUNTERPARTY_ELIMINATION_ID,
						FACT.START_DATE_ID,
						FACT.LCR_LINE_ITEM_ID,
						FACT.NSFR_LINE_ITEM_ID,
						FACT.IRRBB_PROPERTIES_ID,
						FACT.NEXT_RATE_RESET_DATE_ID,						
                        DIM_GT_PRODUCT.GT_PRODUCT_COMMON_CODE,
                        DIM_GT_PRODUCT.GT_PRODUCT_LOCAL_CODE,
                        DIM_GT_PRODUCT.GT_PRODUCT_LOCAL_DESC,                        
						DIM_BALANCE_SHEET_TYPE.BS_TYPE_ASSET_LIABILITY_CODE,
                        DIM_BALANCE_SHEET_TYPE.BS_TYPE_ON_OFF_BALANCE_CODE,
						DIM_MATURITY_DATE.DATE_FULL_DATE AS MATURITY_DATE,
FROM STARSP_FOSA_STAGE.SSI_FACT_CONTRACT_VALUES_ALL_DEPOSITS FACT 
LEFT JOIN STARSP_FOSA_STAGE.SSI_DIM_GT_PRODUCT DIM_GT_PRODUCT
ON (FACT.GT_PRODUCT_ID = DIM_GT_PRODUCT.GT_PRODUCT_ID
AND FACT.DATADATE = DIM_GT_PRODUCT.DATADATE
AND FACT.ISEOM = DIM_GT_PRODUCT.ISEOM)
LEFT JOIN STARSP_FOSA_STAGE.SSI_DIM_BALANCE_SHEET_TYPE DIM_BALANCE_SHEET_TYPE
ON FACT.BALANCE_SHEET_TYPE_ID = DIM_BALANCE_SHEET_TYPE.BS_TYPE_ID
AND FACT.DATADATE = DIM_BALANCE_SHEET_TYPE.DATADATE
AND FACT.ISEOM = DIM_BALANCE_SHEET_TYPE.ISEOM
LEFT JOIN STARSP_FOSA_STAGE.SSI_DIM_DATE DIM_MATURITY_DATE
ON FACT.MATURITY_DATE_ID = DIM_MATURITY_DATE.DATE_ID
AND FACT.DATADATE = DIM_MATURITY_DATE.DATADATE
AND FACT.ISEOM = DIM_MATURITY_DATE.ISEOM
WHERE 1=1
AND (FACT.GT_PRODUCT_COMMON_CODE IN ('DEPO', 'NOSTRO / LORO') OR FACT.DATASOURCE = 'WSST') 
AND FACT.BS_TYPE_ASSET_LIABILITY_CODE = 'L'
AND NVL(FACT.MATURITY_DATE, FACT.DATADATE) >= FACT.DATADATE
;

