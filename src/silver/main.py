import json
import logging
import os

import boto3
import duckdb
import yaml
from dotenv import load_dotenv

load_dotenv()

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

    return global_s


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
    duckdb.sql(f"SELECT * FROM {table_name}").show()
    return None


def test_bronze_silver_sync(silver_table_name="silver_data", bronze_table_name="bronze_data"):
    """
    Test function to validate bronze and silver data synchronization logic.
    """
    # Test scenario 1: Remove some records from silver to test INSERT
    duckdb.sql(f"DELETE FROM {silver_table_name} WHERE id IN (SELECT id FROM {silver_table_name} LIMIT 2)")
    logger.info("Removed 2 records from silver to test INSERT")

    # Test scenario 2: Update some fields in silver to test UPDATE
    duckdb.sql(f"""
        UPDATE {silver_table_name} 
        SET name = 'TEST_UPDATED_' || name,
            state = 'TEST_STATE_' || state,
        WHERE id IN (SELECT id FROM {silver_table_name} LIMIT 2)
    """)
    logger.info("Updated 2 records in silver to test UPDATE")

    # Test scenario 3: Remove some records from bronze to test SOFT DELETE
    duckdb.sql(f"DELETE FROM {bronze_table_name} WHERE id IN (SELECT id FROM {silver_table_name} LIMIT 2 OFFSET 5)")
    logger.info("Removed 2 records from bronze to test SOFT DELETE")

    # Test scenario 4: Mark some records as deleted in silver to test REINSTATEMENT
    duckdb.sql(f"""
        UPDATE {silver_table_name} 
        SET deleted_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id IN (SELECT id FROM {bronze_table_name} LIMIT 3 OFFSET 12)
    """)
    logger.info("Marked 3 records as deleted in silver to test REINSTATEMENT")


class S3Manager:
    def __init__(self, global_settings):
        self.s3_client = boto3.client("s3")
        self.bucket_name = os.environ["S3_BUCKET_NAME"]
        self.last_run_metadata_bronze_path = global_settings["last_run_metadata_bronze_path"]

    def get_last_bronze_run_directory(self):
        """Get the last bronze run directory from S3."""
        s3_client = boto3.client("s3")
        s3_key = os.path.join("bronze", self.last_run_metadata_bronze_path)
        response = s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
        last_run_metadata = json.loads(response["Body"].read().decode("utf-8"))
        last_run_directory = last_run_metadata["last_run_directory"]
        last_run_complete_directory = os.path.join("bronze", last_run_directory)
        return last_run_complete_directory

    def delete_silver_files(self):
        """Delete all files in the silver directory."""
        s3_client = boto3.client("s3")
        prefix = os.path.join("silver", "current_values")
        while True:
            response = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            if "Contents" in response:
                s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={
                        "Objects": [{"Key": obj["Key"]} for obj in response["Contents"]],
                        "Quiet": False,
                    },
                )
            else:
                break


def read_data_from_bronze_and_silver(bronze_files_path, silver_files_path_to_read):
    logging.info(f"Last run complete directory: {bronze_files_path}")
    duckdb.sql(f"""
            CREATE TABLE bronze_data AS
            SELECT * FROM read_json('{bronze_files_path}'); 
        """)

    try:
        duckdb.sql(f"""
            CREATE TABLE silver_data AS
            SELECT * FROM read_parquet('{silver_files_path_to_read}', hive_partitioning = true);
        """)
    except duckdb.duckdb.IOException:
        logging.info("No existing silver data found. Creating new silver data.")
        duckdb.sql("""
            CREATE TABLE silver_data AS
            SELECT *
            FROM bronze_data
            LIMIT 0;""")
        duckdb.sql("""
            ALTER TABLE silver_data ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
            ALTER TABLE silver_data ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
            ALTER TABLE silver_data ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;
        """)


def get_diff_between_bronze_and_silver(str_columns_to_select, str_columns_to_compare):
    duckdb.sql(f"""
        CREATE TABLE silver_bronze_diff AS
        SELECT COALESCE(b.id, s.id) AS id
            ,{str_columns_to_select}
            ,(CASE WHEN s.id IS NULL THEN true ELSE false END) AS __new_record
            ,(CASE WHEN b.id IS NULL AND s.deleted_at IS NULL THEN true ELSE false END) AS __deleted_record
            ,(CASE WHEN b.id IS NOT NULL AND s.deleted_at IS NOT NULL THEN true ELSE false END) AS __reinserted_record
            ,(CASE WHEN b.id IS NOT NULL AND s.id IS NOT NULL AND s.deleted_at IS NULL THEN true ELSE false END) AS __updated_record
        FROM bronze_data b
        FULL OUTER JOIN silver_data s ON b.id = s.id
        WHERE 
            b.id IS NULL 
            OR s.id IS NULL 
            OR (b.id IS NOT NULL AND s.deleted_at IS NOT NULL)
            OR ({str_columns_to_compare})
    """)

    logger.info("Diff between bronze and silver:")
    duckdb.sql("SELECT * FROM silver_bronze_diff").show()

    if duckdb.sql("SELECT COUNT(*) FROM silver_bronze_diff").fetchone()[0] == 0:
        logger.info("No changes detected between bronze and silver data. Exiting.")
        exit(0)


def update_silver_data_duckdb_table(str_columns_to_select, str_columns_to_update):
    duckdb.sql(f"""
            -- Insert new records into silver
            INSERT INTO silver_data
            (id, {str_columns_to_select.replace("b.", "")})
            SELECT b.id, {str_columns_to_select}
            FROM silver_bronze_diff b
            WHERE b.__new_record = true;

            -- Update existing records in silver
            UPDATE silver_data
            SET updated_at = CURRENT_TIMESTAMP,
                {str_columns_to_update}
            FROM silver_bronze_diff b
            WHERE silver_data.id = b.id
            AND b.__updated_record = true;

            -- Soft delete records in silver
            UPDATE silver_data
            SET updated_at = CURRENT_TIMESTAMP,
                deleted_at = CURRENT_TIMESTAMP
            FROM silver_bronze_diff b
            WHERE silver_data.id = b.id
            AND b.__deleted_record = true
            AND silver_data.deleted_at IS NULL;

            -- Reinserted records in silver
            UPDATE silver_data
            SET updated_at = CURRENT_TIMESTAMP,
                deleted_at = NULL,
                {str_columns_to_update}
            FROM silver_bronze_diff b
            WHERE silver_data.id = b.id
            AND b.__reinserted_record = true;

    """)


def export_silver_data_to_storage(s3_manager, silver_files_path_to_write):
    # DuckDB COPY command does not delete old files, sometimes overwriting only is not enough
    logging.info(f"Deleting old files from {silver_files_path_to_write}")
    s3_manager.delete_silver_files()
    logging.info("Old Files deleted")

    duckdb.sql(f"""
        COPY silver_data TO '{silver_files_path_to_write}'
        (FORMAT parquet, PARTITION_BY (state), OVERWRITE_OR_IGNORE);
        """)
    # If you want to set location as country, state and city, you can use the following line
    # (FORMAT parquet, PARTITION_BY (country, state, city), OVERWRITE_OR_IGNORE);

    logging.info("Data upload finished.")
    duckdb.sql("SELECT COUNT(*) as Number_of_Records_in_Silver_Data FROM silver_data").show()


def run_silver_pipeline():
    # Setting up constant values

    # File paths
    global_settings = load_all_settings()
    s3_manager = S3Manager(global_settings)
    last_run_complete_directory = s3_manager.get_last_bronze_run_directory()
    bronze_files_path = os.path.join(
        "s3://", os.environ["S3_BUCKET_NAME"], last_run_complete_directory, "breweries_page_*.json"
    )
    silver_files_path_to_write = os.path.join("s3://", os.environ["S3_BUCKET_NAME"], "silver", "current_values")
    silver_files_path_to_read = os.path.join(silver_files_path_to_write, "*", "*.parquet")

    # Schemas
    bronze_schema = global_settings["expected_bronze_schema"]
    silver_schema = {**bronze_schema, "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP", "deleted_at": "TIMESTAMP"}
    columns_to_compare = [key for key in bronze_schema.keys() if key not in ["id"]]
    str_columns_to_select = ", ".join([f"b.{col}" for col in columns_to_compare])
    str_columns_to_compare = " OR ".join([f"b.{col} IS DISTINCT FROM s.{col}" for col in columns_to_compare])
    str_columns_to_update = ", ".join([f"{col} = b.{col}" for col in columns_to_compare])

    # Main pipeline
    read_data_from_bronze_and_silver(bronze_files_path, silver_files_path_to_read)
    validate_table_schema("bronze_data", bronze_schema)
    validate_table_schema("silver_data", silver_schema)
    get_diff_between_bronze_and_silver(str_columns_to_select, str_columns_to_compare)
    update_silver_data_duckdb_table(str_columns_to_select, str_columns_to_update)
    export_silver_data_to_storage(s3_manager, silver_files_path_to_write)

    logging.info("Silver data written to S3. Pipeline completed successfully.")


if __name__ == "__main__":
    run_silver_pipeline()
