import logging
import os

import duckdb
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def read_silver_layer(silver_files_path):
    logging.info(f"Reading silver layer from: {silver_files_path}")
    duckdb.sql(f"""
            CREATE TABLE silver_data AS
            SELECT * FROM read_parquet('{silver_files_path}', hive_partitioning = true); 
        """)
    logging.info("Silver layer data loaded successfully")


def create_gold_tables():
    logging.info("Creating gold_data_location table")
    duckdb.sql("""
            CREATE TABLE gold_data_location AS
            SELECT 
                --country,
                state,
                --city,
                COUNT(*) AS total_count
            FROM silver_data
               WHERE deleted_at IS NULL
            --GROUP BY country, state, city;
            GROUP BY state;
        """)
    logging.info("Table gold_data_location created successfully")

    logging.info("Creating gold_data_brewery_type table")
    duckdb.sql("""
            CREATE TABLE gold_data_brewery_type AS
            SELECT 
                brewery_type,
                COUNT(*) AS total_count
            FROM silver_data
               WHERE deleted_at IS NULL
            GROUP BY brewery_type;
        """)
    logging.info("Table gold_data_brewery_type created successfully")

    duckdb.sql("SELECT * FROM gold_data_location").show()
    duckdb.sql("SELECT * FROM gold_data_brewery_type").show()


def export_gold_tables(gold_files_path):
    logging.info(f"Exporting gold tables to: {gold_files_path}")
    duckdb.sql(f"""
            COPY (SELECT * FROM gold_data_location) TO '{gold_files_path}/location.parquet' (FORMAT PARQUET);
        """)
    duckdb.sql(f"""
            COPY (SELECT * FROM gold_data_brewery_type) TO '{gold_files_path}/brewery_type.parquet' (FORMAT PARQUET);
        """)
    logger.info("Gold tables created and exported successfully.")


def run_gold_pipeline():
    silver_files_path = os.path.join(
        "s3://", os.environ["S3_BUCKET_NAME"], "silver", "current_values", "*", "*.parquet"
    )
    gold_files_path = os.path.join("s3://", os.environ["S3_BUCKET_NAME"], "gold")

    read_silver_layer(silver_files_path)
    create_gold_tables()
    export_gold_tables(gold_files_path)


if __name__ == "__main__":
    run_gold_pipeline()
