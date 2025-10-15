create database demo;
create schema demo_json;

CREATE OR REPLACE FILE FORMAT FF_DEMO_json_format
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE;
    
CREATE OR REPLACE STAGE SF_STAGE_demo_json
  FILE_FORMAT = FF_DEMO_json_format;

  LIST  @SF_STAGE_demo_json;

  SELECT *
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@sf_stage_demo_json/Sample_Cypro_Primary_Quote.json', -- Or a path to a directory for multiple files
            FILE_FORMAT => 'FF_DEMO_json_format'
        )
    );

SELECT PARSE_JSON($1) FROM @sf_stage_demo_json/Sample_Cypro_Primary_Opp.json;

CREATE TABLE TBL_json_Sample_Cypro_Primary_quote (
        id INT,
        json_data VARIANT
    );

truncate TABLE TBL_json_Sample_Cypro_Primary_quote;

COPY INTO TBL_json_Sample_Cypro_Primary_quote (json_data)
    FROM @sf_stage_demo_json/Sample_Cypro_Primary_Quote.json
    FILE_FORMAT = (FORMAT_NAME = FF_DEMO_json_format);

select f.value:moduleId as submission_id from TBL_json_Sample_Cypro_Primary_quote a,
LATERAL FLATTEN(INPUT => a.json_data:submission) f;

select * from TBL_json_Sample_Cypro_Primary_quote;

SELECT 
            value:id::STRING AS id,
            value:moduleId::STRING AS moduleId,
            value:userId::NUMBER AS userId
        FROM 
            LATERAL FLATTEN(
                INPUT => (
                    SELECT $1
                    FROM @sf_stage_demo_json/Sample_Cypro_Primary_Quote.json (FILE_FORMAT => 'FF_DEMO_json_format')
                    
                ),
                PATH => 'submission'
            );

SELECT
    src.$1:submission id
FROM
    @sf_stage_demo_json/Sample_Cypro_Primary_Quote.json (FILE_FORMAT => 'FF_DEMO_json_format') src,
    LATERAL FLATTEN(INPUT => src.$1:coverageBreakdown) AS coverage;

