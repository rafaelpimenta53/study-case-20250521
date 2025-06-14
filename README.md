# study-case-20250521

## Table of Contents

1. [How to Run the Application](#how-to-run-the-application)
2. [How it Works](#how-it-works)
   - [Bronze Layer (Raw Data Ingestion)](#bronze-layer-raw-data-ingestion)
   - [Silver Layer (Data Cleansing & CDC)](#silver-layer-data-cleansing--cdc)
   - [Gold Layer (Business Aggregations)](#gold-layer-business-aggregations)
3. [Monitoring/Alerting](#monitoringalerting)
4. [Future Improvements](#future-improvements)
   - [Possible improvements](#possible-improvements)
   - [Infrastructure & DevOps](#infrastructure--devops)
5. [Sample Results](#sample-results)
   

This project implements a complete data pipeline following the **medallion architecture** (Bronze, Silver, Gold layers) to process brewery data from the Open Brewery DB API. The pipeline demonstrates modern data engineering practices.

### Stack Used:

- **Python and DuckDB** – Data collection and transformation
- **Docker** – Containerization of each stage
- **AWS S3** – Data storage
- **Apache Airflow** – Workflow scheduling and orchestration

## How to Run the Application

- Clone repository: ```git clone -b dev https://github.com/rafaelpimenta53/study-case-20250521.git```
- Run ```cd study-case-20250521/```
- Copy .env-sample to .env and fill in AWS Keys and bucket name
- Run the ```source run.sh``` script. It will:
  - Start Docker service if not running yet
  - Build the images for the project
  - Start Airflow using Docker Compose
- Open Airflow at http://localhost:8080/
  - Username: airflow
  - Password: airflow
 
**Main Files and Folder Structure:**

```
project-root/
├── src/
│   ├── bronze/
│   │   ├── main.py
│   │   ├── Dockerfile
│   │   └── settings.yaml
│   ├── silver/
│   │   ├── main.py
│   │   └── Dockerfile
│   ├── gold/
│   │   ├── main.py
│   │   └── Dockerfile
│   └── config/
│       └── project-settings.yaml
├── .env
└── run.sh
```


## How it Works

### Bronze Layer (Raw Data Ingestion)

**Purpose**: Ingests raw data from the Open Brewery DB API with minimal processing.

**Inputs**:
- **API Endpoints**:
  - Metadata URL: `https://api.openbrewerydb.org/v1/breweries/meta` (to get total brewery count)
  - Breweries API: `https://api.openbrewerydb.org/v1/breweries` (paginated brewery data)
- **Configuration Files**:
  - `src/bronze/settings.yaml`: API request parameters, headers, retry logic, pagination settings
  - `src/config/project-settings.yaml`: File naming conventions, output paths

**Processing Steps**:
1. **Metadata Retrieval**: Fetches API metadata to determine total brewery count and calculate pagination
2. **Paginated Data Collection**: Iterates through all API pages, fetching brewery data with retry logic
3. **Raw Data Storage**: For each page, saves unprocessed JSON responses as text files
4. **Metadata Creation**: Creates a metadata file indicating the timestamp directory of the current run

**Outputs**:
- **Raw JSON Files**:
  - `s3://<bucket>/bronze/YYYYMMDD-HHMMSS/metadata.json`: API metadata response
  - `s3://<bucket>/bronze/YYYYMMDD-HHMMSS/breweries_page_<N>.json`: Raw brewery data for each page
  - `s3://<bucket>/bronze/last_run_metadata.json`: Name of the last directory created

**Validations**:
- API request retry mechanism (configurable max retries and delay)
  - When a retry is triggered, it logs a warning which can be monitored later 
- If the API stops working during data collection, `last_run_metadata.json` will not be updated, preventing incomplete data from being processed.

### Silver Layer (Data Cleansing & CDC)

**Purpose**: Transforms raw data into structured format with schema validation and Change Data Capture.

**Inputs**:
- **Bronze Layer Data**: JSON files from the latest bronze run (identified via `last_run_metadata.json`)
- **Existing Silver Data**: Parquet files from `s3://<bucket>/silver/current_values/`
- **Configuration**: Expected schemas from `src/config/project-settings.yaml`

**Processing Steps**:
1. **Data Loading**: 
   - Reads latest bronze JSON files into DuckDB `bronze_data` table
   - Loads existing silver parquet files into DuckDB `silver_data` table (or creates it if it doesn't exist)
2. **Schema Validation**: Validates both bronze and silver tables against expected schemas. If there is a schema change, it raises an error.
3. **Change Detection**: Creates a diff table identifying:
   - `__new_record`: Records in bronze but not in silver
   - `__deleted_record`: Records in silver (active) but not in bronze
   - `__reinserted_record`: Records in bronze that were previously soft-deleted in silver
   - `__updated_record`: Records with data changes between bronze and silver
4. **CDC Operations**:
   - **Inserts**: New records with `created_at` and `updated_at` timestamps
   - **Updates**: Modified records with refreshed `updated_at` timestamp
   - **Soft Deletes**: Missing records marked with `deleted_at` timestamp
   - **Reinstatements**: Previously deleted records restored (nullifying `deleted_at`)
5. **Data Export**: Writes updated silver data as partitioned parquet files

**Outputs**:
- **Structured Data**: Parquet files in `s3://<bucket>/silver/current_values/`
- **Partitioning**: Data partitioned by `city` for query optimization
- **CDC Columns**: Each record includes `created_at`, `updated_at`, `deleted_at` timestamps

**Validations**:
- Strict schema validation (fails on column additions/removals or type mismatches)
- CDC logic ensures data consistency and lineage tracking
- Early exit if no changes are detected between bronze and silver

### Gold Layer (Business Aggregations)

**Purpose**: Creates business-ready aggregated views for analytics and reporting.

**Inputs**:
- **Silver Layer Data**: Parquet files from `s3://<bucket>/silver/current_values/`

**Processing Steps**:
1. **Data Loading**: Reads all silver parquet files into DuckDB `silver_data` table
2. **Aggregation Creation**:
   - **Location Analysis**: Groups by `city` with brewery counts
   - **Brewery Type Analysis**: Groups by `brewery_type` with counts
3. **Data Export**: Exports aggregated tables as separate parquet files

**Outputs**:
- **Location Aggregates**: `s3://<bucket>/gold/location.parquet`
- **Brewery Type Aggregates**: `s3://<bucket>/gold/brewery_type.parquet`

**Validations**:
- Implicit validation through dependency on silver layer data quality

## Monitoring/Alerting

- **Monitor Errors and Warnings from AWS CloudWatch**: Airflow supports sending logs to CloudWatch, making it possible to create dashboards for monitoring records obtained, warnings, and set up email alerts when errors are logged.
- **Data Quality**: Data tests can be implemented with dbt or Great Expectations to run various tests. Examples:
  - Test that the "Name" column is never NULL
  - Test that columns are not 100% NULL

## Future Improvements

### Possible Improvements
- **Apply more data cleaning methods**: For example, trim every string and replace double spaces. I noticed that there are countries starting with a blank space.
- **Skip Gold Layer if Silver Unchanged**: Implement logic to detect if the silver layer processed any changes and conditionally skip gold processing
- **Compare results in Gold layer to Bronze metadata file**: Aggregates by city and location are included in the bronze metadata file
- **Schema Evolution**: Support controlled schema changes without pipeline failures
- **Investigate slowness of DuckDB COPY command**: The workaround that was implemented instead is not very efficient

### Infrastructure & DevOps
- **Complete IaC**: Finish Terraform implementation for ECS, EKS, and Airflow deployment
- **Cost Optimization**: Implement S3 lifecycle policies and resource usage monitoring

## Sample Results

![Sample Result by Brewery Type](documentation/Sample-Result-by-Bewery-Type.png)
![Sample Result By State](documentation/Sample-Result-By-State.png)
