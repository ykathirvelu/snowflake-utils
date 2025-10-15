-----Step 1
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS iceberg_lab;
CREATE ROLE IF NOT EXISTS iceberg_lab;
CREATE DATABASE IF NOT EXISTS iceberg_lab;
CREATE SCHEMA IF NOT EXISTS iceberg_lab;
GRANT ALL ON DATABASE iceberg_lab TO ROLE iceberg_lab WITH GRANT OPTION;
GRANT ALL ON SCHEMA iceberg_lab.iceberg_lab TO ROLE iceberg_lab WITH GRANT OPTION;;
GRANT ALL ON WAREHOUSE iceberg_lab TO ROLE iceberg_lab WITH GRANT OPTION;;

CREATE USER IF NOT EXISTS iceberg_lab
    PASSWORD='iceberg_lab1234',
    LOGIN_NAME='ICEBERG_LAB',
    MUST_CHANGE_PASSWORD=FALSE,
    DISABLED=FALSE,
    DEFAULT_WAREHOUSE='ICEBERG_LAB',
    DEFAULT_NAMESPACE='ICEBERG_LAB.ICEBERG_LAB',
    DEFAULT_ROLE='ICEBERG_LAB';

GRANT ROLE iceberg_lab TO USER iceberg_lab;
GRANT ROLE iceberg_lab TO USER ykathirvelu;
GRANT ROLE accountadmin TO USER iceberg_lab;

-----Step 2

CREATE OR REPLACE EXTERNAL VOLUME ext_vol_demo_sf_int_opencatalog
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'bkt-demo-sfmanaged-iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://bkt-demo-sfmanaged-iceberg/demo_sf_int_opencatalog/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::794038241382:role/yk-sf-open-catalog'
            STORAGE_AWS_EXTERNAL_ID = 'iceberg_table_external_id_20250328'
         )
      );

CREATE OR REPLACE EXTERNAL VOLUME ext_vol_demo_sf_ext_catalog
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'bkt-demo-sfmanaged-iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://bkt-demo-sfmanaged-iceberg/demo_sf_ext_catalog/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::794038241382:role/yk-sf-open-catalog'
            STORAGE_AWS_EXTERNAL_ID = 'iceberg_table_external_id_20250328'
         )
      );


desc external volume ext_vol_demo_sf_int_opencatalog;
desc external volume ext_vol_demo_sf_ext_catalog;
---- copy "STORAGE_AWS_IAM_USER_ARN":"arn:aws:iam::825765398327:user/hmix0000-s"

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ext_vol_demo_sf_int_opencatalog');
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ext_vol_demo_sf_ext_catalog');
------Step 3      

USE ROLE accountadmin;
GRANT ALL ON EXTERNAL VOLUME ext_vol_demo_sfmanaged_iceberg TO ROLE iceberg_lab WITH GRANT OPTION;
GRANT ALL ON EXTERNAL VOLUME ext_vol_demo_sfmanaged_iceberg02 TO ROLE iceberg_lab WITH GRANT OPTION;
USE ROLE iceberg_lab;
USE DATABASE iceberg_lab;
USE SCHEMA iceberg_lab;
CREATE OR REPLACE ICEBERG TABLE customer_iceberg (
    c_custkey INTEGER,
    c_name STRING,
    c_address STRING,
    c_nationkey INTEGER,
    c_phone STRING,
    c_acctbal INTEGER,
    c_mktsegment STRING,
    c_comment STRING
)  
    CATALOG='SNOWFLAKE'
    EXTERNAL_VOLUME='ext_vol_demo_sfmanaged_iceberg'
    BASE_LOCATION='customer_iceberg';

CREATE OR REPLACE ICEBERG TABLE employee_iceberg_IC (
    c_custkey INTEGER,
    c_name STRING,
    c_address STRING,
    c_nationkey INTEGER,
    c_phone STRING,
    c_acctbal INTEGER,
    c_mktsegment STRING,
    c_comment STRING
)  
    CATALOG='SNOWFLAKE'
    EXTERNAL_VOLUME='ext_vol_demo_sfmanaged_iceberg'
    BASE_LOCATION='employee_iceberg_IC';    

    use warehouse iceberg_lab;
    
INSERT INTO customer_iceberg
  SELECT * FROM snowflake_sample_data.tpch_sf1.customer limit 10000;

SELECT *
FROM customer_iceberg c
INNER JOIN snowflake_sample_data.tpch_sf1.nation n
    ON c.c_nationkey = n.n_nationkey;  

INSERT INTO customer_iceberg
    SELECT
        *
    FROM snowflake_sample_data.tpch_sf1.customer
    LIMIT 5;


SELECT
    count(*) AS after_row_count,
    before_row_count
FROM customer_iceberg
JOIN (
        SELECT count(*) AS before_row_count
        FROM customer_iceberg BEFORE(statement => LAST_QUERY_ID())
    )
    ON 1=1
GROUP BY 2;    

------------ Iceberg Governance

USE ROLE accountadmin;
CREATE ROLE IF NOT EXISTS tpch_us;
GRANT ROLE tpch_us TO USER ykathirvelu;
CREATE ROLE IF NOT EXISTS tpch_intl;
GRANT ROLE tpch_intl TO USER ykathirvelu;

USE ROLE iceberg_lab;
USE DATABASE iceberg_lab;
USE SCHEMA iceberg_lab;

CREATE OR REPLACE ROW ACCESS POLICY rap_nation
AS (nation_key number) RETURNS BOOLEAN ->
  ('TPCH_US' = current_role() and nation_key = 24) OR
  ('TPCH_INTL' = current_role() and nation_key != 24)
;

ALTER ICEBERG TABLE customer_iceberg
ADD ROW ACCESS POLICY rap_nation ON (c_nationkey);

GRANT ALL ON DATABASE iceberg_lab TO ROLE tpch_intl;
GRANT ALL ON SCHEMA iceberg_lab.iceberg_lab TO ROLE tpch_intl;
GRANT ALL ON ICEBERG TABLE iceberg_lab.iceberg_lab.customer_iceberg TO ROLE tpch_intl;
GRANT ALL ON DATABASE iceberg_lab TO ROLE tpch_us;
GRANT ALL ON SCHEMA iceberg_lab.iceberg_lab TO ROLE tpch_us;
GRANT ALL ON ICEBERG TABLE iceberg_lab.iceberg_lab.customer_iceberg TO ROLE tpch_us;
GRANT USAGE ON EXTERNAL VOLUME ext_vol_demo_sfmanaged_iceberg TO ROLE tpch_intl;
GRANT USAGE ON EXTERNAL VOLUME ext_vol_demo_sfmanaged_iceberg TO ROLE tpch_us;
GRANT USAGE ON WAREHOUSE iceberg_lab TO ROLE tpch_us;
GRANT USAGE ON WAREHOUSE iceberg_lab TO ROLE tpch_intl;

USE ROLE tpch_intl;
USE WAREHOUSE iceberg_lab;
SELECT
    count(*)
FROM iceberg_lab.iceberg_lab.customer_iceberg;

USE ROLE tpch_us;
USE WAREHOUSE iceberg_lab;
SELECT
    count(*)
FROM iceberg_lab.iceberg_lab.customer_iceberg;

----------coulmn Level Security

USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS tpch_analyst;
GRANT ROLE tpch_analyst TO USER ykathirvelu;

USE ROLE iceberg_lab;
ALTER ROW ACCESS POLICY rap_nation
SET body ->
  ('TPCH_US' = current_role() and nation_key = 24) or
  ('TPCH_INTL' = current_role() and nation_key != 24) or
  ('TPCH_ANALYST' = current_role()) or 
  ('ICEBERG_LAB' = current_role())
;

GRANT ALL ON DATABASE iceberg_lab TO ROLE tpch_analyst;
GRANT ALL ON SCHEMA iceberg_lab.iceberg_lab TO ROLE tpch_analyst;
GRANT ALL ON TABLE iceberg_lab.iceberg_lab.customer_iceberg TO ROLE tpch_analyst;
GRANT USAGE ON WAREHOUSE iceberg_lab TO ROLE tpch_analyst;
GRANT USAGE ON EXTERNAL VOLUME ext_vol_demo_sfmanaged_iceberg TO ROLE tpch_analyst;
USE ROLE iceberg_lab;

CREATE OR REPLACE MASKING POLICY pii_mask AS (val string) RETURNS string ->
    CASE
        WHEN 'TPCH_ANALYST' = current_role() THEN '*********'
        ELSE val
    END;

ALTER ICEBERG TABLE customer_iceberg MODIFY COLUMN c_name SET MASKING POLICY pii_mask;
ALTER ICEBERG TABLE customer_iceberg MODIFY COLUMN c_address SET MASKING POLICY pii_mask;
ALTER ICEBERG TABLE customer_iceberg MODIFY COLUMN c_phone SET MASKING POLICY pii_mask;

USE ROLE tpch_analyst;
SELECT
    *
FROM customer_iceberg;

-------------Spark

USE ROLE iceberg_lab;
USE DATABASE iceberg_lab;
USE SCHEMA iceberg_lab;
CREATE OR REPLACE ICEBERG TABLE nation_orders_iceberg (
    regionkey INTEGER,
    nationkey INTEGER,
    nation STRING,
    custkey INTEGER,
    order_count INTEGER,
    total_price INTEGER
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'ext_vol_demo_sfmanaged_iceberg'
    BASE_LOCATION = 'nation_orders_iceberg';

-------- Demo for Databricks

USE ROLE ICEBERG_LAB;

CREATE OR REPLACE FUNCTION FAKER("LOCALES" VARCHAR(16777216), "PROVIDER" VARCHAR(16777216), "PARAMETERS" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('faker')
HANDLER = 'compute'
AS '
import json, datetime, decimal
from faker import Faker
locale_list = [''en_US'',''en_us'', ''en_GB'',''de_DE'', ''de_AT'', ''de_CH'',''pl_PL'', ''fr_FR'', ''es_ES'', ''it_IT'', ''nl_NL'', ''dk_DK'', ''ru_RU'']
faker = Faker(locale_list)
def compute(locales, provider, parameters):
    try:
        fake = faker[locales]
    except:
        raise Exception (''Country not implemented.'')
    if len(parameters) > 2:
        data = fake.format(provider,parameters)
    else:
        data = fake.format(provider)
    data = json.loads(json.dumps(data, default=default_json_transform).replace(''\\\\n'','', ''))
    return data
# format incompatible data types
def default_json_transform(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError
';

CREATE OR REPLACE ICEBERG TABLE ICEBERG_CUSTOMER
  CATALOG='SNOWFLAKE'
  EXTERNAL_VOLUME='ext_vol_demo_sfmanaged_iceberg'
  BASE_LOCATION='customer' AS (
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
    from table(generator(rowcount => 50000)));
SELECT * FROM ICEBERG_CUSTOMER LIMIT 10;

--20 transactions per customer; 100M record Iceberg format table
CREATE OR REPLACE ICEBERG TABLE ICEBERG_TRANSACTION 
  CATALOG='SNOWFLAKE'
  EXTERNAL_VOLUME='ext_vol_demo_sfmanaged_iceberg'
  BASE_LOCATION='txn' AS ( 
   select  
           --customer info
           ROW_NUMBER() OVER (ORDER BY seq4())  ID,
           ICEBERG_CUSTOMER.ID AS CUSTOMER_ID,
           UPPER(faker('en_US', 'bothify', '?###########' )::string) TRANSACTION_NUM,
           faker('en_US', 'date_time_this_year', '')::datetime(6) TRANSACTION_DATE,
           uniform(0,30 , random(120)) QUANTITY,    
           REPLACE(LTRIM(faker('en_US', 'pricetag', '1'), '$'), ',', '')::float UNIT_PRICE,       
           faker('en_US', 'sentence', '4')::string TRANSACTION_DESC
           --profile
    from table(generator(rowcount => 10)), ICEBERG_CUSTOMER);

-- suspend the WH after use 
--ALTER WAREHOUSE COMPUTE_XL_WH SUSPEND;

--verify the tables are created 
SHOW TABLES LIKE 'ICEBERG%';

--view the newly generated files were created in your S3 bucket 
--LS @ICEBERG_STAGE;


------------------------------------------------
------------ Open Catalog integration

CREATE OR REPLACE CATALOG INTEGRATION my_open_catalog_int
  CATALOG_SOURCE = POLARIS
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'myOpenCatalogCatalogNamespace'
  REST_CONFIG = (
    CATALOG_URI = 'https://<orgname>-<my-snowflake-open-catalog-account-name>.snowflakecomputing.com/polaris/api/catalog'
    WAREHOUSE = 'myOpenCatalogExternalCatalogName'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = 'myClientId'
    OAUTH_CLIENT_SECRET = 'myClientSecret'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
  )