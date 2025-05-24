import boto3
import duckdb
from datetime import datetime
import logging
import os
import glob
import json
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_all_settings():
    """Loads all necessary configuration files."""
    # silver_settings_path = os.path.join(os.path.dirname(__file__), "settings.yaml")
    # with open(silver_settings_path, "r") as f:
    #     silver_s = yaml.safe_load(f)

    global_settings_path = os.path.join(os.path.dirname(__file__), "..", "config", "project-settings.yaml")
    with open(global_settings_path, "r") as f:
        global_s = yaml.safe_load(f)

    cloud_resources_path = os.path.join(os.path.dirname(__file__), "..", "config", "cloud-resources.json")
    with open(cloud_resources_path, "r") as f:
        cloud_res = json.load(f)
    return global_s, cloud_res


def validate_table_schema(table_name, expected_schema):
    schema_changed = False
    actual_columns = duckdb.sql(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """).fetchall()

    actual_schema = {col[0]: col[1] for col in actual_columns}

    missing_columns = set(expected_schema.keys()) - set(actual_schema.keys())
    added_columns = set(actual_schema.keys()) - set(expected_schema.keys())
    type_mismatches = {}
    for col_name in expected_schema.keys():
        if expected_schema.get(col_name) != actual_schema.get(col_name):
            type_mismatches[col_name] = {
                "expected": expected_schema.get(col_name),
                "actual": actual_schema.get(col_name),
            }

    if missing_columns:
        logger.warning(f"Colunas faltando em {table_name}: {missing_columns}")
        schema_changed = True
    if added_columns:
        logger.warning(f"Colunas adicionais em {table_name}: {added_columns}")
        schema_changed = True
    if type_mismatches:
        logger.warning(f"Tipos de dados diferentes em {table_name}: {type_mismatches}")
        schema_changed = True

    if schema_changed:
        raise ValueError(
            f"""Schema changed for {table_name}. 
            Added columns to source: {list(added_columns)}
            Missing columns from source: {list(missing_columns)}
            Type mismatches: {type_mismatches}"""
        )

    logger.info(f"Schema validation for {table_name} passed.")
    return None


# Define expected bronze schema for later validation
BRONZE_SCHEMA = {
    "id": "UUID",
    "name": "VARCHAR",
    "brewery_type": "VARCHAR",
    "address_1": "VARCHAR",
    "address_2": "VARCHAR",
    "address_3": "VARCHAR",
    "city": "VARCHAR",
    "state_province": "VARCHAR",
    "postal_code": "VARCHAR",
    "country": "VARCHAR",
    "longitude": "DOUBLE",
    "latitude": "DOUBLE",
    "phone": "VARCHAR",
    "website_url": "VARCHAR",
    "state": "VARCHAR",
    "street": "VARCHAR",
}

SILVER_SCHEMA_BASE = {**BRONZE_SCHEMA, "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP", "deleted_at": "TIMESTAMP"}


# read data from bronze
def get_last_bronze_run_directory(cloud_resources, global_settings):
    """Get the last bronze run directory from S3."""
    s3_client = boto3.client("s3")
    s3_bucket = cloud_resources["s3_bucket_name"]
    s3_key = os.path.join("bronze", global_settings["last_run_metadata_bronze"])
    s3_client.download_file(s3_bucket, s3_key, "last_run_metadata_bronze.json")
    with open("last_run_metadata_bronze.json", "r") as f:
        last_run_metadata = json.load(f)
    last_run_directory = last_run_metadata["last_run_directory"]
    last_run_complete_directory = os.path.join("bronze", last_run_directory)
    return last_run_complete_directory


global_settings, cloud_resources = load_all_settings()
last_run_complete_directory = get_last_bronze_run_directory(cloud_resources, global_settings)
bronze_files_path = os.path.join(
    "s3://", cloud_resources["s3_bucket_name"], last_run_complete_directory, "breweries_page_*.json"
)
silver_files_path = os.path.join("s3://", cloud_resources["s3_bucket_name"], "silver", "current_values", "*.parquet")
print(f"Last run complete directory: {bronze_files_path}")

# # read all bronze files
# duckdb_conn = duckdb.connect(database=":memory:")
# duckdb_conn.execute(f"""
#     CREATE TABLE bronze_data AS
#     SELECT * FROM read_json('{bronze_files_path}', ignore_errors=true);
# """)
# duckdb_conn.execute("SELECT * FROM bronze_data").show()


duckdb.sql(f"""
    CREATE TABLE bronze_data AS
    SELECT * FROM read_json('{bronze_files_path}', ignore_errors=true);
""")
duckdb.sql("SELECT * FROM bronze_data").show()

try:
    duckdb.sql(f"""
        CREATE TABLE silver_data AS
        SELECT * FROM read_parquet('{silver_files_path}');
    """)
except duckdb.duckdb.IOException:
    print("No existing silver data found. Creating new silver data.")
    duckdb.sql("""
            CREATE TABLE silver_data AS
            SELECT *
            FROM bronze_data
            LIMIT 5;""")  # TODO: remove limit 5
    duckdb.sql("""
        ALTER TABLE silver_data ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        ALTER TABLE silver_data ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        ALTER TABLE silver_data ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;
    """)
    # silver_data = duckdb.sql(""" SELECT * FROM silver_data;""")

duckdb.sql("SELECT * FROM silver_data").show()
validate_table_schema("bronze_data", BRONZE_SCHEMA)
validate_table_schema("silver_data", SILVER_SCHEMA_BASE)
