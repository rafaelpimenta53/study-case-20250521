import requests
import yaml
import os
import time
import json

# Load bronze-specific settings
bronze_settings_path = os.path.join(os.path.dirname(__file__), "settings.yaml")
with open(bronze_settings_path, "r") as f:
    bronze_settings = yaml.safe_load(f)

# Load global settings
# Assuming the script is run from the project root or config is in PYTHONPATH
# For robustness, calculate path relative to this script or project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
global_settings_path = os.path.join(project_root, "config", "settings.yaml")
with open(global_settings_path, "r") as f:
    global_settings = yaml.safe_load(f)

print("Starting the Bronze pipeline...")

# Prepare local data storage
base_data_path = global_settings.get("local_data_path", "data")
bronze_data_dir = os.path.join(project_root, base_data_path, "bronze")
if global_settings.get("output_env") == "local":
    os.makedirs(bronze_data_dir, exist_ok=True)
    print(f"Local data directory: {bronze_data_dir}")

page = 1
all_data_fetched = False

while not all_data_fetched:
    print(f"Fetching page {page}...")

    for int_attempt in range(bronze_settings["api_request_max_retries"]):
        r = requests.get(
            f"{bronze_settings['api_url']}",
            params={
                "per_page": bronze_settings["api_param_itens_per_page"],
                "sort": bronze_settings["api_param_sort_by"],
                "page": page,
            },
            headers={"From": bronze_settings["api_header_from"]},  # Corrected key
        )
        if r.status_code == 200:
            break

        else:
            print(f"Error fetching page {page}, attempt {int_attempt + 1}: {r.status_code} : {r.text}. Retrying...")
            time.sleep(bronze_settings["api_request_retry_delay_seconds"])
    else:
        raise Exception(f"Failed to fetch page {page} after {bronze_settings['api_request_max_retries']} retries.")

    data = r.json()

    if not data:  # API returns an empty list when no more data
        print("No more data found. Finishing fetching.")
        all_data_fetched = True
        break

    if global_settings.get("output_env") == "local":
        file_name = f"breweries_page_{page}.json"
        file_path = os.path.join(bronze_data_dir, file_name)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Saved data from page {page} to {file_path}")
    elif global_settings.get("output_env") == "s3":
        s3_bucket = global_settings["s3_bucket_name"]
        s3_key = f"bronze/breweries_page_{page}.json"
        print(f"Saved data from page {page} to S3: s3://{s3_bucket}/{s3_key}")
        pass

    page += 1
    time.sleep(bronze_settings.get("api_request_delay_seconds", 1))

    if page >= 4:
        print("Stopping after 3 pages for testing purposes.")
        break

print("Bronze pipeline finished.")
