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

desc external volume ext_vol_demo_sf_int_opencatalog;
desc external volume ext_vol_demo_sf_ext_catalog;
---- copy "STORAGE_AWS_IAM_USER_ARN":"arn:aws:iam::825765398327:user/hmix0000-s"

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ext_vol_demo_sf_int_opencatalog');
------Step 3      

USE ROLE accountadmin;
GRANT ALL ON EXTERNAL VOLUME ext_vol_demo_sf_int_opencatalog TO ROLE iceberg_lab WITH GRANT OPTION;
USE ROLE iceberg_lab;
USE DATABASE iceberg_lab;
USE SCHEMA iceberg_lab;

CREATE OR REPLACE CATALOG INTEGRATION demo_open_catalog_int 
  CATALOG_SOURCE = POLARIS 
  TABLE_FORMAT = ICEBERG 
  CATALOG_NAMESPACE = 'ICEBERG_LAB'
  REST_CONFIG = (
    CATALOG_URI = 'https://ipfscuf-sfocyk.snowflakecomputing.com/polaris/api/catalog' 
    CATALOG_NAME = 'demo_sf_int_opencatalog'
  )
    REST_AUTHENTICATION = (
    TYPE = OAUTH 
    OAUTH_CLIENT_ID = '1J8Lz1d9ghwNzHqErdQql7CY2a8=' 
    OAUTH_CLIENT_SECRET = 'AKD0Bmx/MIZXWnroWXihxoNPVnTKNyjE61cyt22mdhI=' 
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL') 
      ) 
  ENABLED = TRUE;

use database iceberg_lab;



CREATE OR REPLACE ICEBERG TABLE test_table
  CATALOG = 'demo_open_catalog_int'
  EXTERNAL_VOLUME = 'ext_vol_demo_sf_int_opencatalog'
  CATALOG_TABLE_NAME = 'test_table'
  AUTO_REFRESH = TRUE;

grant all on table test_table to role iceberg_lab;  

SELECT * FROM iceberg_lab.public.test_table;  

insert into test_table values (5)