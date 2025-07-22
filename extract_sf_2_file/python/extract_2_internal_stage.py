import os
import snowflake.connector
from dotenv import load_dotenv
import logging
import time

# Load environment variables
load_dotenv()

# Configure logging (best practice for monitoring and debugging)
logging.basicConfig(
    level=logging.INFO,  # Adjust log level as needed (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def connect_snowflake():
    try:
        logging.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
        logging.info("Connection successful.")
        return conn
    except Exception as e:
        logging.error("Failed to connect to Snowflake", exc_info=True)
        raise

def unload_data_to_internal_stage(cursor, table_name, stage_name="@%"+''+ "unload_stage"):
    try:
        logging.info(f"Unloading data from {table_name} to internal stage {stage_name}...")
        unload_query = f"""
            COPY INTO {stage_name}
            FROM {table_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION = 'NONE')
            HEADER = TRUE
            OVERWRITE = TRUE;
        """
        cursor.execute("USE SCHEMA MOVIELENS.MOVIES")
        cursor.execute("USE ROLE ACCOUNTADMIN")
        cursor.execute("create or replace stage my_temp_int_stage directory = (enable = true) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')")
        cursor.execute("alter stage my_temp_int_stage refresh")
        cursor.execute(unload_query)
        logging.info("Unload to internal stage completed.")
    except Exception as e:
        logging.error("Failed to unload data to internal stage", exc_info=True)
        raise

def download_files_from_stage(cursor, stage_name, local_dir="./downloaded_files"):
    try:
        os.makedirs(local_dir, exist_ok=True)
        logging.info(f"Downloading files from stage {stage_name} to {local_dir}...")
        cursor.execute("USE SCHEMA MOVIELENS.MOVIES")
        cursor.execute("USE ROLE ACCOUNTADMIN")
        list_query = f"LIST {stage_name};"
        cursor.execute(list_query)
        files = cursor.fetchall()

        for file in files:
            file_name = file[0]
            logging.info(f"file://{os.path.abspath(local_dir)}")
            get_query = f"GET {stage_name}/{file_name} file://{os.path.abspath(local_dir)}"
            logging.info(f"Downloading file: {file_name}")
            cursor.execute(get_query)

        logging.info("All files downloaded successfully.")
    except Exception as e:
        logging.error("Failed to download files from stage", exc_info=True)
        raise

def main():
    table_name = "INTERACTIONS"
    stage_name = "@my_temp_int_stage"  # or use @my_named_stage

    conn = connect_snowflake()
    try:
        cursor = conn.cursor()
        unload_data_to_internal_stage(cursor, table_name, stage_name)
        download_files_from_stage(cursor, stage_name)
    finally:
        cursor.close()
        conn.close()
        logging.info("Snowflake connection closed.")

if __name__ == "__main__":
    main()
