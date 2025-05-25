import json
import logging
import os

import duckdb
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def read_silver_layer(silver_files_path):
    duckdb.sql(f"""
            CREATE TABLE silver_data AS
            SELECT * FROM read_parquet('{silver_files_path}', hive_partitioning = true); 
        """)


def create_gold_tables():
    duckdb.sql("""
            CREATE TABLE gold_data_location AS
            SELECT 
                country,
                state,
                city,
                COUNT(*) AS total_count
            FROM silver_data
               WHERE deleted_at IS NOT NULL
            --GROUP BY country, state, city;
            GROUP BY city;
        """)

    # Agregated table by type
    duckdb.sql("""
            CREATE TABLE gold_data_brewery_type AS
            SELECT 
                brewery_type,
                COUNT(*) AS total_count
            FROM silver_data
               WHERE deleted_at IS NOT NULL
            GROUP BY brewery_type;
        """)
    duckdb.sql("SELECT * FROM gold_data_location").show()
    duckdb.sql("SELECT * FROM gold_data_brewery_type").show()


def export_gold_tables(gold_files_path):
    # Export to parquet
    duckdb.sql(f"""
            COPY (SELECT * FROM gold_data_location) TO '{gold_files_path}/location.parquet' (FORMAT PARQUET);
        """)
    duckdb.sql(f"""
            COPY (SELECT * FROM gold_data_brewery_type) TO '{gold_files_path}/brewery_type.parquet' (FORMAT PARQUET);
        """)


def run_gold_pipeline():
    silver_files_path = os.path.join(
        "s3://", os.environ["S3_BUCKET_NAME"], "silver", "current_values", "*", "*.parquet"
    )
    gold_files_path = os.path.join("s3://", os.environ["S3_BUCKET_NAME"], "gold")

    read_silver_layer(silver_files_path)
    create_gold_tables()
    export_gold_tables(gold_files_path)
    logger.info("Gold tables created and exported successfully.")


if __name__ == "__main__":
    run_gold_pipeline()
