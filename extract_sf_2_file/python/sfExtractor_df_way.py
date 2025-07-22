import os
import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_from_snowflake(query: str) -> pd.DataFrame:
    try:
        logging.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )

        logging.info("Executing Snowflake query...")
        df = pd.read_sql(query, conn)
        logging.info(f"Fetched {len(df)} records from Snowflake.")
        return df

    except Exception as e:
        logging.error("Error fetching data from Snowflake", exc_info=True)
        raise
    finally:
        conn.close()

def load_to_sqlserver(df: pd.DataFrame, table_name: str):
    try:
        logging.info("Connecting to SQL Server...")
        conn_str = (
            f"mssql+pyodbc://{os.getenv('SQLSERVER_USER')}:{os.getenv('SQLSERVER_PASSWORD')}"
            f"@{os.getenv('SQLSERVER_SERVER')}/{os.getenv('SQLSERVER_DATABASE')}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        engine = create_engine(conn_str)
        logging.info("Uploading data to SQL Server...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info("Upload complete.")
    except Exception as e:
        logging.error("Error loading data into SQL Server", exc_info=True)
        raise

def save_to_csv(df: pd.DataFrame, file_path: str):
    try:
        logging.info(f"Saving DataFrame to CSV at {file_path}...")
        df.to_csv(file_path, index=False)
        logging.info("CSV file saved.")
    except Exception as e:
        logging.error("Error saving CSV file", exc_info=True)
        raise

def main():
    query = "SELECT * FROM MOVIELENS.movies.INTERACTIONS"
    table_name = "INTERACTIONS"
    csv_file_path = "INTERACTIONS.csv"

    df = fetch_from_snowflake(query)
    #load_to_sqlserver(df, table_name)
    save_to_csv(df, csv_file_path)

if __name__ == "__main__":
    main()
