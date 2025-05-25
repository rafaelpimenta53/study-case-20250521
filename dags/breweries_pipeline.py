from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="breweries_pipeline",
    start_date=datetime(2025, 5, 23),
    schedule="@daily",
    catchup=False,
    tags=["medallion", "pipeline", "breweries", "study", "case"],
) as dag:
    bronze = DockerOperator(
        task_id="bronze_stage",
        image="study-case-20250521-bronze-pipeline",
        auto_remove="force",
        command=None,
        docker_url="unix://opt/airflow/docker.sock",
        env_file="./.env",
        mount_tmp_dir=False,
    )

    silver = DockerOperator(
        task_id="silver_stage",
        image="study-case-20250521-silver-pipeline",
        auto_remove="force",
        command=None,
        docker_url="unix://opt/airflow/docker.sock",
        env_file="./.env",
        mount_tmp_dir=False,
    )

    gold = DockerOperator(
        task_id="gold_stage",
        image="study-case-20250521-gold-pipeline",
        auto_remove="force",
        command=None,
        docker_url="unix://opt/airflow/docker.sock",
        env_file="./.env",
        mount_tmp_dir=False,
    )

    bronze >> silver >> gold
