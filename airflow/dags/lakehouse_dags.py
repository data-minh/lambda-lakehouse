from __future__ import annotations
import os
import logging
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from python_ingestion.vnstock import vnstock_ingestion
from python_ingestion.us import us_ingestion
from python_ingestion.jp import jp_ingestion
from utils.setup_env import setup_env

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

env = setup_env()

default_args = {
    "owner": "minhpn",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse-pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["lakehouse"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


    with TaskGroup(group_id="ingestion_group") as ingestion_group:
        run_vn_ingestion = PythonOperator(
            task_id="run_vn_ingestion",
            python_callable=vnstock_ingestion,
            op_kwargs={
                "CHUNK_SIZE_DETAIL": 30,
                "MAX_WORKERS": 1,
                "PAUSE_TIME_SUCCESS": 20,
                "PAUSE_TIME_CRASH": 45,
            }
        )

        run_us_ingestion = PythonOperator(
            task_id="run_us_ingestion",
            python_callable=us_ingestion,
        )

        run_jp_ingestion = PythonOperator(
            task_id="run_jp_ingestion",
            python_callable=jp_ingestion,
        )
        [run_vn_ingestion, run_us_ingestion, run_jp_ingestion]

    # processing stg stage
    with TaskGroup(group_id="processing_stg_group") as processing_stg_group:
        run_processing_vn_stg = SparkSubmitOperator(
            task_id="run_processing_vn_stg",
            application="/opt/airflow/dags/spark_code/stg/vn_stg_processing.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        run_processing_us_stg = SparkSubmitOperator(
            task_id="run_processing_us_stg",
            application="/opt/airflow/dags/spark_code/stg/us_stg_processing.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        run_processing_jp_stg = SparkSubmitOperator(
            task_id="run_processing_jp_stg",
            application="/opt/airflow/dags/spark_code/stg/jp_stg_processing.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        [run_processing_vn_stg, run_processing_us_stg, run_processing_jp_stg]

    # processing curated stage
    with TaskGroup(group_id="processing_curated_group") as processing_curated_group:
        run_processing_curated_dim_company = SparkSubmitOperator(
            task_id="run_processing_curated_dim_company",
            application="/opt/airflow/dags/spark_code/curated/dim_company.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        run_processing_curated_dim_currency = SparkSubmitOperator(
            task_id="run_processing_curated_dim_currency",
            application="/opt/airflow/dags/spark_code/curated/dim_currency.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        run_processing_curated_dim_date = SparkSubmitOperator(
            task_id="run_processing_curated_dim_date",
            application="/opt/airflow/dags/spark_code/curated/dim_date.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        run_processing_curated_dim_exchange = SparkSubmitOperator(
            task_id="run_processing_curated_dim_exchange",
            application="/opt/airflow/dags/spark_code/curated/dim_exchange.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        run_processing_curated_dim_trading_status = SparkSubmitOperator(
            task_id="run_processing_curated_dim_trading_status",
            application="/opt/airflow/dags/spark_code/curated/dim_trading_status.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        run_processing_curated_fact_stock_daily = SparkSubmitOperator(
            task_id="run_processing_curated_fact_stock_daily",
            application="/opt/airflow/dags/spark_code/curated/fact_stock_daily.py",
            conn_id="spark-cluster",
            driver_memory="1g",
            num_executors=1, 
            executor_memory="1g",
            executor_cores=1,
        )

        [
            run_processing_curated_dim_company, 
            run_processing_curated_dim_currency, 
            run_processing_curated_dim_date, 
            run_processing_curated_dim_exchange, 
            run_processing_curated_dim_trading_status
        ] >> run_processing_curated_fact_stock_daily


    start >> ingestion_group >> processing_stg_group >> processing_curated_group >> end