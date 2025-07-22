CREATE OR REPLACE PROCEDURE UNLOAD_FROM_CONFIG_BY_PROCESS_SQL(
    IN_PROCESS_NAME varchar(100)
)
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
        v_copy_sql := '
            COPY INTO ' || :v_stage_path || '
            FROM ' || :v_table_name || '
            FILE_FORMAT = (TYPE = ''CSV'' FIELD_OPTIONALLY_ENCLOSED_BY = ''"'' COMPRESSION = ''GZIP'')
            OVERWRITE = TRUE
            HEADER = TRUE;
        ';
    ELSE
        IF (:v_extract_sql IS NULL OR TRIM(:v_extract_sql) = '') THEN
            RETURN 'extract_sql is missing for process: ' || :IN_PROCESS_NAME;
        END IF;

        v_copy_sql := '
            COPY INTO ' || :v_stage_path || '
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
