This is a study case project to build a complete pipeline following the medallion archtecture.

- The main language is Python 3.10.
- The dependencies will be managed by poetry.
- The infrastructure will be hosted on AWS and managed by IaC on Terraform.
- All programs will run in Docker containers on the cloud.
- The stages will be orchestrated by Airflow, running on AWS EKS.
- Data will be saved to the S3 as parquet files
- The main library to do the transformations will be Polars but we might change to PySpark later

There will be 3 main python programs:
- The first one collects data from an API and saves to the bronze layer
- The second will read data from the first one and save in a columnar format to the silver layer
- The third one will read data from the silver layer, make agregations and save to the gold layer
- The folder structure for the stages will be the following:
    ```
    - src/
        - bronze/
            - __init__.py
            - main.py
        - silver/
            - __init__.py
            - main.py
        - gold/
            - __init__.py
            - main.py
        - config/
            - cloud-resources.json
            - project-settings.yaml
    ```


