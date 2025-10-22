from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="lakehouse-pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["spark", "iceberg", "nessie"],
) as dag:
    submit_iceberg_job = SparkSubmitOperator(
        task_id="submit_iceberg_job_from_s3",
        application="/opt/airflow/dags/spark_code/write_iceberg_sample.py",
        conn_id="spark-cluster",
        driver_memory="1g",
        num_executors=1, 
        executor_memory="1g",
        executor_cores=1,
    )
