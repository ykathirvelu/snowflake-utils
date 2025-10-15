use schema demo.demo_json;




-- 1) File format & stage
CREATE OR REPLACE FILE FORMAT ff_json_cypro TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE
  ENABLE_OCTAL = FALSE;

CREATE OR REPLACE STAGE stg_cypro FILE_FORMAT = ff_json_cypro;

-- 2) Upload the file from your machine (run in SnowSQL or UI):
-- PUT file://Sample_Cypro_Primary_Quote.json @stg_cypro AUTO_COMPRESS=TRUE;

-- 3) Raw landing table: one VARIANT payload
CREATE OR REPLACE TABLE raw_cypro_primary_quote (
  src_filename STRING,
  load_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  payload      VARIANT
);

CREATE OR REPLACE TABLE raw_Sample_MPL_Excess_Opp (
  src_filename STRING,
  load_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  payload      VARIANT
);

CREATE OR REPLACE TABLE raw_cypro_primary_opp (
  src_filename STRING,
  load_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  payload      VARIANT
);

CREATE OR REPLACE TABLE raw_Sample_MPL_Excess_quote (
  src_filename STRING,
  load_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  payload      VARIANT
);


-- 4) Load from stage
COPY INTO raw_cypro_primary_quote(payload, src_filename)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @stg_cypro
)
FILE_FORMAT = (FORMAT_NAME = ff_json_cypro)
ON_ERROR   = 'CONTINUE';

COPY INTO raw_Sample_MPL_Excess_Opp(payload, src_filename)
FROM (
  SELECT $1, METADATA$FILENAME as filename
  FROM @stg_cypro 
)
FILE_FORMAT = (FORMAT_NAME = ff_json_cypro)
PATTERN  = 'Sample_MPL_Excess_Opp.json'
ON_ERROR   = 'CONTINUE';


-- 4) Load from stage
COPY INTO raw_cypro_primary_opp(payload, src_filename)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @stg_cypro
)
FILE_FORMAT = (FORMAT_NAME = ff_json_cypro)
PATTERN  = 'Sample_Cypro_Primary_Opp.json'
ON_ERROR   = 'CONTINUE';

COPY INTO raw_Sample_MPL_Excess_quote (payload, src_filename)
FROM (
  SELECT $1, METADATA$FILENAME as filename
  FROM @stg_cypro 
)
FILE_FORMAT = (FORMAT_NAME = ff_json_cypro)
PATTERN  = 'Sample_MPL_Excess_Quote.json'
ON_ERROR   = 'CONTINUE';

list @stg_cypro;

SELECT  replace(REGEXP_REPLACE(f.path, '\\[[0-9]+\\]', ''),'.',':') AS path,
  TYPEOF(f.value) AS JSON_Type,
  COUNT(*) AS "Count"
FROM raw_cypro_primary_quote,
LATERAL FLATTEN(payload, RECURSIVE=>true) f
GROUP BY 1, 2 
ORDER BY 1, 2;

---- Primary Quote Getting the Json Element Path
 with cte_q
 as 
 (
 SELECT REGEXP_REPLACE(f.path, '\\[[0-9]+\\]', '[]') AS "Path",
  TYPEOF(f.value) AS "Type",
  COUNT(*) AS "Count"
FROM raw_cypro_primary_quote,
LATERAL FLATTEN(payload, RECURSIVE=>true) f
GROUP BY 1, 2 ORDER BY 1, 2
),
cte_mpl_opp
as
(
---- MPL_Excess_Opp Getting the JSON Element Path
 SELECT REGEXP_REPLACE(f.path, '\\[[0-9]+\\]', '[]') AS "Path",
  TYPEOF(f.value) AS "Type",
  COUNT(*) AS "Count"
FROM raw_Sample_MPL_Excess_Opp,
LATERAL FLATTEN(payload, RECURSIVE=>true) f
GROUP BY 1, 2 ORDER BY 1, 2
),
cte_mpl_q
as
(
---- MPL_Excess_Opp Getting the JSON Element Path
 SELECT REGEXP_REPLACE(f.path, '\\[[0-9]+\\]', '[]') AS "Path",
  TYPEOF(f.value) AS "Type",
  COUNT(*) AS "Count"
FROM DEMO.DEMO_JSON.RAW_SAMPLE_MPL_EXCESS_QUOTE,
LATERAL FLATTEN(payload, RECURSIVE=>true) f
GROUP BY 1, 2 ORDER BY 1, 2
),
cte_opp
as
(
---- MPL_Excess_Opp Getting the JSON Element Path
 SELECT REGEXP_REPLACE(f.path, '\\[[0-9]+\\]', '[]') AS "Path",
  TYPEOF(f.value) AS "Type",
  COUNT(*) AS "Count"
FROM DEMO.DEMO_JSON.RAW_CYPRO_PRIMARY_OPP,
LATERAL FLATTEN(payload, RECURSIVE=>true) f
GROUP BY 1, 2 ORDER BY 1, 2
)
select 'Quote',* from cte_q
union all
select 'opp',* from cte_opp
union all
select 'MPL_Excess_Opp',* from cte_mpl_opp
union all
select 'MPL_Excess_quote',* from cte_mpl_q
order by 1,2,3;

--------- Query to get whether the JSON SubSchema has data or attributes

SELECT
  TYPEOF(payload)                                AS payload_type,
  payload:submission:id::STRING                AS submission_id,
  CASE WHEN IS_ARRAY(payload:submission:metadata) AND ARRAY_SIZE(payload:submission:metadata) = 0 THEN TRUE
        ELSE FALSE
    END AS has_metadata,
  CASE WHEN IS_ARRAY(payload:submission:validationErrors) AND ARRAY_SIZE(payload:submission:validationErrors.value) = 0 THEN TRUE
        ELSE FALSE
    END as has_validationErrors,
    CASE WHEN IS_ARRAY(payload:submission:data:rawData:pricing) AND ARRAY_SIZE(payload:submission:data:rawData:pricing.value) = 0 THEN TRUE
        ELSE FALSE
    END,
  payload:submission:data:rawData:pricing IS NOT NULL       AS has_pricing,
  payload:submission:data:rawData:defaultForms IS NOT NULL  AS has_forms
FROM raw_cypro_primary_quote
LIMIT 5;

------------------------------------------------------------------------
insert into json_path_info(path_info)
select 'RAW_CYPRO_PRIMARY_OPP r,
LATERAL FLATTEN(input => r.payload:submission:"data":rawData:limitDeductibles) rd';

insert into json_config (json_path_info,column_alias)
select 'r.src_filename' as path,'src_file_name' as alias union all
select 'r.payload:persistedInWormStorage' as path,'persistedInWormStorage' as alias union all
select 'r.payload:createdBy::STRING' as path,'createdBy' as alias union all
select 'r.payload:created::STRING' as path,'created' as alias union all
select 'r.payload:id::STRING                                     ' as path,'id' as alias union all
select 'r.payload:submission:id::STRING                          ' as path,'submission_id' as alias union all
select 'r.payload:submission:moduleId::STRING                    ' as path,'submission_moduleId' as alias union all
select 'r.payload:submission:userId::STRING                      ' as path,'submission_userId' as alias union all
select 'r.payload:submission:created::STRING                     ' as path,'submission_created' as alias union all
select 'r.payload:submission:modified::STRING                    ' as path,'submission_modified' as alias union all
select 'r.payload:submission:formArchive::STRING                      ' as path,'submission_formArchive' as alias union all
select 'r.payload:submission:platformVersionAtCreated::STRING                     ' as path,'submission_platformVersionAtCreated' as alias union all
select 'r.payload:submission:platformVersionAtModified::STRING                    ' as path,'submission_platformVersionAtModified' as alias union all
select 'r.payload:submission:moduleArchiveIdAtCreated::STRING                     ' as path,'submission_moduleArchiveIdAtCreated' as alias union all
select 'r.payload:submission:moduleArchiveIdAtModified::STRING                    ' as path,'submission_moduleArchiveIdAtModified' as alias union all
select 'r.payload:submission:"data":rawData:"accountNumber"::STRING                                   ' as path,'accountNumber' as alias union all
select 'r.payload:submission:"data":rawData:"proposedEffectiveDate"::STRING                          ' as path,'proposedEffectiveDate' as alias union all
select 'r.payload:submission:"data":rawData:"isRenewal"::STRING                           ' as path,'isRenewal' as alias union all
select 'r.payload:submission:"data":rawData:"isRenewable"::STRING                      ' as path,'isRenewable' as alias union all
select 'r.payload:submission:"data":rawData:"receivedDate"::STRING                           ' as path,'receivedDate' as alias union all
select 'r.payload:submission:"data":rawData:"status"::STRING                           ' as path,'status' as alias union all
select 'r.payload:submission:"data":rawData:"number"::STRING       ' as path,'number_val' as alias union all
select 'r.payload:submission:"data":rawData:"statusDescription"::STRING      ' as path,'statusDescription' as alias union all
select 'r.payload:submission:"data":rawData:"producerName"::STRING                         ' as path,'producerName' as alias union all
select 'r.payload:submission:"data":rawData:"receivedDateFormatted"::STRING                          ' as path,'receivedDateFormatted' as alias union all
select 'r.payload:submission:"data":rawData:"effectiveDateFormatted"::STRING                       ' as path,'effectiveDateFormatted' as alias union all
select 'r.payload:submission:"data":rawData:"lastModifiedDate"::STRING                         ' as path,'lastModifiedDate' as alias union all
select 'r.payload:submission:"data":rawData:"lastModifiedDateFormatted"::string ' as path,'lastModifiedDateFormatted' as alias union all
select 'r.payload:submission:"data":rawData:"submissionType"::string ' as path,'submissionType' as alias union all
select 'r.payload:submission:"data":rawData:"producerEmail"::string ' as path,'producerEmail' as alias union all
select 'r.payload:submission:"data":rawData:"daysToInception"::string ' as path,'daysToInception' as alias union all
select 'r.payload:submission:"data":rawData:"expiringPolicyNumber"::string ' as path,'expiringPolicyNumber' as alias union all
select 'r.payload:submission:"data":rawData:"expiringSubmissionNumber"::string ' as path,'expiringSubmissionNumber' as alias union all
select 'r.payload:submission:"data":rawData:"producer"::string ' as path,'producer' as alias union all
select 'r.payload:submission:"data":rawData:"archBranchId"::string ' as path,'archBranchId' as alias union all
select 'r.payload:submission:"data":rawData:"archBranch"::string ' as path,'archBranch' as alias union all
select 'r.payload:submission:"data":rawData:"archBranchDescription"::string ' as path,'archBranchDescription' as alias union all
select 'r.payload:submission:"data":rawData:"competitorPrice"::string ' as path,'competitorPrice' as alias union all
select 'r.payload:submission:"data":rawData:"termInMonths"::string ' as path,'termInMonths' as alias union all
select 'r.payload:submission:"data":rawData:"underwriter":email::string ' as path,'under_writer_email' as alias union all
select 'r.payload:submission:"data":rawData:underwriter:"fullName"::string ' as path,'under_writer_fullName' as alias union all
select 'r.payload:submission:"data":rawData:underwriter:"userName"::string ' as path,'under_writer_userName' as alias union all
select 'r.payload:submission:"data":rawData:"naicsDetails":naicsCode::string ' as path,'naicsCode' as alias union all
select 'r.payload:submission:"data":rawData:"naicsDetails":naicsDescription::string ' as path,'naicsDescription' as alias union all
select 'r.payload:submission:"data":rawData:"naicsDetails":dunsNumber::string ' as path,'dunsNumber' as alias union all
select 'r.payload:submission:"data":rawData:"producerPoc":name::string ' as path,'producer_POC_name' as alias union all
select 'r.payload:submission:"data":rawData:"producerPoc":email::string ' as path,'producer_POC_email' as alias union all
select 'r.payload:submission:"data":rawData:"producerPoc":phoneNumber::string ' as path,'producer_POC_phoneNumber' as alias union all
select 'r.payload:submission:"data":rawData:"productDetails":businessSubDivisionDescription::string ' as path,'businessSubDivisionDescription' as alias union all
select 'r.payload:submission:"data":rawData:"productDetails":businessSubDivisionExpired::string ' as path,'businessSubDivisionExpired' as alias union all
select 'r.payload:submission:"data":rawData:"productDetails":businessSubDivision::string ' as path,'businessSubDivision' as alias union all
select 'r.payload:submission:"data":rawData:"agencyObject":agency::string ' as path,'agency' as alias union all
select 'r.payload:submission:"data":rawData:"agencyObject":producerAddress:producerState::string ' as path,'agency_producerState' as alias union all
select 'r.payload:submission:"data":rawData:"agencyObject":producerAddress:producerCountry::string ' as path,'agency_producerCountry' as alias union all
select 'rd.value:limitDeductibleId ' as path,'limitDeductibleId' as alias union all
select 'rd.value:productFamily ' as path,'productFamily' as alias union all
select 'rd.value:productFamilyDescription ' as path,'productFamilyDescription' as alias union all
select 'rd.value:insuranceType ' as path,'insuranceType' as alias ;

select * from json_path_info;


select * from json_config;

select 'insert overwrite into '||target_tbl||'('||LISTAGG(column_alias,',\n')||')\n select '||LISTAGG(json_path_info||' as '||column_alias,',\n') WITHIN GROUP (ORDER BY order_seq)||' from '||path_info||';' as col_list
from json_config 
join json_path_info on (json_config.path_info_id=json_config.path_info_id)
where is_active='Y'
group by json_path_info.path_info_id,path_info,target_tbl;

select * from STG_CYPRO_PRIMARY_OPP_REL;


select 'select '||LISTAGG(column_alias,',')||' from '||target_tbl||' \nminus \nselect '||LISTAGG(json_path_info||' as '||column_alias,',\n') WITHIN GROUP (ORDER BY order_seq)||' from '||path_info as col_list
from json_config 
join json_path_info on (json_config.path_info_id=json_config.path_info_id)
where is_active='Y'
group by json_path_info.path_info_id,path_info,target_tbl;

update json_config set is_active='Y' where order_seq>56;
