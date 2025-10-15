use role workshopadmin;
use DATABASE MOVIELENS;
CREATE SCHEMA MOVIELENS.integrations;
CREATE SCHEMA MOVIELENS.dev;
CREATE SCHEMA MOVIELENS.prod;

USE MOVIELENS.integrations;
CREATE OR REPLACE SECRET MOVIELENS.integrations.tb_dbt_git_secret
  TYPE = password
  USERNAME = '<>'
  PASSWORD = '<>';

CREATE OR REPLACE API INTEGRATION tb_dbt_git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/ykathirvelu')
  -- Comment out the following line if your forked repository is public
  ALLOWED_AUTHENTICATION_SECRETS = (MOVIELENS.integrations.tb_dbt_git_secret)
  ENABLED = TRUE;


  -- Create NETWORK RULE for external access integration

CREATE OR REPLACE NETWORK RULE dbt_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  -- Minimal URL allowlist that is required for dbt deps
  VALUE_LIST = (
    'hub.getdbt.com',
    'codeload.github.com'
    );

-- Create EXTERNAL ACCESS INTEGRATION for dbt access to external dbt package locations

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_ext_access
  ALLOWED_NETWORK_RULES = (dbt_network_rule)
  ENABLED = TRUE;

  MOVIELENS.PROD.DBTPRJCT