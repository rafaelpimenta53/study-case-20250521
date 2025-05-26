from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="breweries_pipeline",
    start_date=datetime(2025, 5, 23),
    schedule="@daily",
    catchup=False,
    tags=["medallion", "pipeline", "breweries", "study", "case"],
    max_active_runs=1,
    default_args=default_args,
) as dag:
    bronze = DockerOperator(
        task_id="bronze_stage",
        image="study-case-20250521-bronze-pipeline",
        auto_remove="force",
        command=None,
        docker_url="unix://opt/airflow/docker.sock",
        env_file="./.env",
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=10),
    )

    silver = DockerOperator(
        task_id="silver_stage",
        image="study-case-20250521-silver-pipeline",
        auto_remove="force",
        command=None,
        docker_url="unix://opt/airflow/docker.sock",
        env_file="./.env",
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=180),
    )

    gold = DockerOperator(
        task_id="gold_stage",
        image="study-case-20250521-gold-pipeline",
        auto_remove="force",
        command=None,
        docker_url="unix://opt/airflow/docker.sock",
        env_file="./.env",
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=60),
    )

    bronze >> silver >> gold
