import requests
import yaml
import os
import time
import json
import boto3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bronze_pipeline.log")],
)
logger = logging.getLogger(__name__)

# Load bronze-specific settings
bronze_settings_path = os.path.join(os.path.dirname(__file__), "settings.yaml")
with open(bronze_settings_path, "r") as f:
    bronze_settings = yaml.safe_load(f)

# Load global settings
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# global_settings_path = os.path.join(project_root, "src", "config", "project-settings.yaml")
global_settings_path = os.path.join(os.path.dirname(__file__), "..", "config", "project-settings.yaml")
with open(global_settings_path, "r") as f:
    global_settings = yaml.safe_load(f)

# Load cloud resources configuration
cloud_resources_path = os.path.join(os.path.dirname(__file__), "..", "config", "cloud-resources.json")
with open(cloud_resources_path, "r") as f:
    cloud_resources = json.load(f)

logger.info("Starting the Bronze pipeline...")

# Generate timestamp for this run
run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
logger.info(f"Run timestamp: {run_timestamp}")

# Prepare local data storage
base_data_path = global_settings.get("local_data_path", "data")
bronze_data_dir = os.path.join(os.path.dirname(__file__), base_data_path, "bronze", run_timestamp)
if global_settings.get("output_env") == "local":
    os.makedirs(bronze_data_dir, exist_ok=True)
    logger.info(f"Local data directory: {bronze_data_dir}")

# Initialize S3 client if needed
s3_client = None
s3_bucket = None
if global_settings.get("output_env") == "s3":
    s3_client = boto3.client("s3")
    s3_bucket = cloud_resources["s3_bucket_name"]
    logger.info(f"Using S3 bucket: {s3_bucket}")

page = 1
all_data_fetched = False

while not all_data_fetched:
    logger.info(f"Fetching page {page}...")

    for int_attempt in range(bronze_settings["api_request_max_retries"]):
        r = requests.get(
            f"{bronze_settings['api_url']}",
            params={
                "per_page": bronze_settings["api_param_itens_per_page"],
                "sort": bronze_settings["api_param_sort_by"],
                "page": page,
            },
            headers={"From": bronze_settings["api_header_from"]},
        )
        if r.status_code == 200:
            break
        else:
            logger.warning(
                f"Error fetching page {page}, attempt {int_attempt + 1}: {r.status_code} : {r.text}. Retrying..."
            )
            time.sleep(bronze_settings["api_request_retry_delay_seconds"])
    else:
        raise Exception(f"Failed to fetch page {page} after {bronze_settings['api_request_max_retries']} retries.")

    data = r.json()

    if not data:
        logger.info("No more data found. Finishing fetching.")
        all_data_fetched = True
        break

    if global_settings.get("output_env") == "local":
        file_name = f"breweries_page_{page}.json"
        file_path = os.path.join(bronze_data_dir, file_name)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
        logger.info(f"Saved data from page {page} to {file_path}")
    elif global_settings.get("output_env") == "s3":
        s3_key = f"bronze/{run_timestamp}/breweries_page_{page}.json"
        s3_client.put_object(
            Bucket=s3_bucket, Key=s3_key, Body=json.dumps(data, indent=4), ContentType="application/json"
        )
        logger.info(f"Saved data from page {page} to S3: s3://{s3_bucket}/{s3_key}")

    page += 1
    time.sleep(bronze_settings.get("api_request_delay_seconds", 1))

    if page >= 4:
        logger.info("Stopping after 3 pages for testing purposes.")
        break

logger.info("Bronze pipeline finished.")
