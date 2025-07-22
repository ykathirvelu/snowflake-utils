CREATE OR REPLACE PROCEDURE UNLOAD_FROM_CONFIG_BY_PROCESS(
    IN_PROCESS_NAME STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    // Step 1: Query the control table to get active config for the process
    var config_query = `
        select PROCESS_NAME, TABLE_NAME, EXTRACT_LOCATION, FILE_PREFIX, EXTRACT_TYPE, EXTRACT_SQL
        from MOVIELENS.MOVIES.MD_OB_EXTRACT_CONFIG
        where IS_ACTIVE='Y' and PROCESS_NAME= ?`;

    var config_stmt = snowflake.createStatement({
        sqlText: config_query,
        binds: [IN_PROCESS_NAME]
    });

    var config_result = config_stmt.execute();

    if (!config_result.next()) {
        return `No active config found for process: ${IN_PROCESS_NAME}`;
    }

    var table_name = config_result.getColumnValue("TABLE_NAME");
    var stage_name = config_result.getColumnValue("EXTRACT_LOCATION");
    var file_prefix = config_result.getColumnValue("FILE_PREFIX");
    var extract_type = config_result.getColumnValue("EXTRACT_TYPE");
    var extract_sql = config_result.getColumnValue("EXTRACT_SQL");

    // Step 2: Format current date as DDMMYYYY
    var now = new Date();
    var dd = String(now.getDate()).padStart(2, '0');
    var mm = String(now.getMonth() + 1).padStart(2, '0');
    var yyyy = now.getFullYear();
    var date_folder = dd + mm + yyyy;

    // Step 3: Extract only the table name (if DB and schema are included)
    var clean_table = table_name.replace(/["']/g, '').split('.').pop();

    // Step 4: Build full stage path
    var stage_path = `@${stage_name}/${clean_table}/${date_folder}/${file_prefix}`;

    // Step 5: Generate and run COPY INTO command
    if (extract_type === 'T') 
    {
    var copy_sql = `COPY INTO ${stage_path}
                    FROM ${table_name}
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = 'GZIP')
                    OVERWRITE = TRUE
                    HEADER = TRUE;`;
    }
    else
    {
    var copy_sql = `COPY INTO ${stage_path}
                    FROM (${extract_sql})
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = 'GZIP')
                    OVERWRITE = TRUE
                    HEADER = TRUE;`;
     }               

    var copy_stmt = snowflake.createStatement({ sqlText: copy_sql });
    copy_stmt.execute();

    return `Unload successful for ${IN_PROCESS_NAME} to ${stage_path}`;
} catch (err) {
    return `Unload failed for ${IN_PROCESS_NAME}: ${err.message}`;
}
$$;
