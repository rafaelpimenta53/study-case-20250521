import json
import logging
import math
import os
import time
from datetime import datetime

import boto3
import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_all_settings():
    """Loads all necessary configuration files."""
    bronze_settings_path = os.path.join(os.path.dirname(__file__), "settings.yaml")
    with open(bronze_settings_path, "r") as f:
        bronze_s = yaml.safe_load(f)

    global_settings_path = os.path.join(os.path.dirname(__file__), "..", "config", "project-settings.yaml")
    with open(global_settings_path, "r") as f:
        global_s = yaml.safe_load(f)

    return bronze_s, global_s


class APIRequestHandler:
    def __init__(self, bronze_settings):
        self.headers = bronze_settings["api_request_header"]
        self.max_retries = bronze_settings["api_request_max_retries"]
        self.retry_delay_seconds = bronze_settings["api_request_retry_delay_seconds"]

    def make_request(self, url, params=None):
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                return response.text  # Return raw text instead of JSON
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Error during API request to {url} (attempt {attempt + 1}/{self.max_retries}): {e}. Retrying..."
                )
                if attempt + 1 == self.max_retries:
                    logger.error(f"Failed to fetch data from {url} after {self.max_retries} retries.")
                    raise
                time.sleep(self.retry_delay_seconds)
        return None


class DataSaver:
    def __init__(self, global_settings):
        self.run_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        logger.info(f"Run timestamp for DataSaver: {self.run_timestamp}")
        self.last_run_metadata_bronze_path = global_settings["last_run_metadata_bronze_path"]

        self.output_env = os.environ["OUTPUT_ENV"]
        if self.output_env == "local":
            base_data_path = global_settings.get("local_data_path", "data")
            # Assuming main.py is in src/bronze/
            # os.path.join(os.path.dirname(__file__), "..", "..") is project_root/
            project_root_for_data = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            self.base_local_path = os.path.join(project_root_for_data, base_data_path, "bronze", self.run_timestamp)
            os.makedirs(self.base_local_path, exist_ok=True)
            logger.info(f"Local data directory prepared: {self.base_local_path}")
        elif self.output_env == "s3":
            self.s3_client = boto3.client("s3")
            self.s3_bucket = os.environ["S3_BUCKET_NAME"]
            self.base_s3_key_prefix = f"bronze/{self.run_timestamp}"
            logger.info(f"Using S3 bucket: {self.s3_bucket}, base key prefix for this run: {self.base_s3_key_prefix}")
        else:
            logger.warning(f"Unknown output_env: {self.output_env}. Data saving might not work as expected.")

    def save_text(self, data, file_name):
        """Saves raw text data to local disk or S3 using pre-configured paths."""
        if self.output_env == "local":
            if not self.base_local_path:
                logger.error("Base local path not configured for local saving.")
                return
            local_file_path = os.path.join(self.base_local_path, file_name)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            with open(local_file_path, "w") as f:
                f.write(data)
            logger.info(f"Saved data to {local_file_path}")
        elif self.output_env == "s3":
            if not self.s3_client or not self.s3_bucket or not self.base_s3_key_prefix:
                logger.error("S3 client, bucket, or base key prefix not configured for S3 saving.")
                return
            s3_key = f"{self.base_s3_key_prefix}/{file_name}"
            self.s3_client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=data, ContentType="text/plain")
            logger.info(f"Saved data to S3: s3://{self.s3_bucket}/{s3_key}")
        else:
            logger.warning(f"Unknown output_env: {self.output_env}. Data not saved.")

    def save_last_run_metadata(self):
        """Saves metadata about the last run to the bronze directory."""
        metadata = {"last_run_directory": self.run_timestamp}

        if self.output_env == "local":
            bronze_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "bronze"))
            metadata_file_path = os.path.join(bronze_dir, self.last_run_metadata_bronze_path)
            with open(metadata_file_path, "w") as f:
                json.dump(metadata, f, indent=4)
            logger.info(f"Saved last run metadata to {metadata_file_path}")
        elif self.output_env == "s3":
            s3_key = os.path.join("bronze", self.last_run_metadata_bronze_path)
            self.s3_client.put_object(
                Bucket=self.s3_bucket, Key=s3_key, Body=json.dumps(metadata, indent=4), ContentType="application/json"
            )
            logger.info(f"Saved last run metadata to S3: s3://{self.s3_bucket}/{s3_key}")


def run_bronze_pipeline():
    """Main function to run the Bronze pipeline."""
    logger.info("Starting the Bronze pipeline...")

    bronze_settings, global_settings = load_all_settings()
    data_saver = DataSaver(global_settings=global_settings)
    api_handler = APIRequestHandler(bronze_settings=bronze_settings)

    logger.info("Fetching metadata...")
    meta_url = bronze_settings["meta_url"]
    metadata_raw = api_handler.make_request(meta_url)
    if not metadata_raw:
        logger.error("Failed to fetch metadata. Exiting.")
        raise Exception("Metadata fetch failed.")
    data_saver.save_text(data=metadata_raw, file_name=global_settings["metadata_file_name"])

    try:
        metadata = json.loads(metadata_raw)
        total_breweries = int(metadata["total"])
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to parse metadata JSON: {e}. Exiting.")
        raise Exception("Metadata parsing failed.")

    logger.info(f"Total breweries found: {total_breweries}")
    items_per_page = bronze_settings["api_param_itens_per_page"]
    total_pages = math.ceil(total_breweries / items_per_page)
    logger.info(f"Total pages to fetch: {total_pages} (based on {items_per_page} items per page)")

    # Main Loop - Fetch all brewery data
    for page in range(1, total_pages + 1):
        logger.info(f"Fetching page {page} of {total_pages}...")
        api_params = {
            "per_page": items_per_page,
            "sort": bronze_settings["api_param_sort_by"],
            "page": page,
        }
        page_data_raw = api_handler.make_request(bronze_settings["api_url"], params=api_params)

        if not page_data_raw:
            if page == 1:
                logger.error("No data found on the first page. The filters may not be working or the API may be down.")
                raise Exception("No data found on the first page.")
            logger.warning(f"No data found on page {page}. This might indicate an issue or end of data.")
            break

        page_file_name = global_settings["brewery_page_filename_template"].format(page_number=page)
        data_saver.save_text(data=page_data_raw, file_name=page_file_name)

        # if page >= 3:
        #     logger.info("Fetched 3 pages. Stopping further requests to avoid overloading the API.")
        #     break

        time.sleep(bronze_settings.get("api_request_delay_seconds", 1))

    data_saver.save_last_run_metadata()
    logger.info("Bronze pipeline finished.")


if __name__ == "__main__":
    run_bronze_pipeline()
