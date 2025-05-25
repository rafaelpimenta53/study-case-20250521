import duckdb
import logging
import os
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_all_settings():
    """Loads all necessary configuration files."""
    cloud_resources_path = os.path.join(os.path.dirname(__file__), "..", "config", "cloud-resources.json")
    with open(cloud_resources_path, "r") as f:
        cloud_res = json.load(f)
    return cloud_res


cloud_resources = load_all_settings()
silver_files_path = os.path.join(
    "s3://", cloud_resources["s3_bucket_name"], "silver", "current_values", "*", "*", "*", "*.parquet"
)
gold_files_path = os.path.join("s3://", cloud_resources["s3_bucket_name"], "gold")

duckdb.sql(f"""
        CREATE TABLE silver_data AS
        SELECT * FROM read_parquet('{silver_files_path}', hive_partitioning = true); 
    """)  ## TODO: remove limit

# Agregated table by location
duckdb.sql("""
        CREATE TABLE gold_data_location AS
        SELECT 
            country,
            state,
            city,
            COUNT(*) AS total_count
        FROM silver_data
        GROUP BY country, state, city;
    """)

# Agregated table by type
duckdb.sql("""
        CREATE TABLE gold_data_brewery_type AS
        SELECT 
            brewery_type,
            COUNT(*) AS total_count
        FROM silver_data
        GROUP BY brewery_type;
    """)

duckdb.sql("SELECT * FROM gold_data_location").show()
duckdb.sql("SELECT * FROM gold_data_brewery_type").show()

# Export to parquet
duckdb.sql(f"""
        COPY (SELECT * FROM gold_data_location) TO '{gold_files_path}/location.parquet' (FORMAT PARQUET);
    """)
duckdb.sql(f"""
        COPY (SELECT * FROM gold_data_brewery_type) TO '{gold_files_path}/brewery_type.parquet' (FORMAT PARQUET);
    """)
