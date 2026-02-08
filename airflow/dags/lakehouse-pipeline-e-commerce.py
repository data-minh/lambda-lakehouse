from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "minhpn",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SPARK_STG_TASKS = [
    ("stg_customers_verified", "stg_customers_verified.py"),
    ("stg_geolocation_unique", "stg_geolocation_unique.py"),
    ("stg_order_items_clean", "stg_order_items_clean.py"),
    ("stg_orders_enriched", "stg_orders_enriched.py"),
    ("stg_products_en", "stg_products_en.py"),
    ("stg_sellers_verified", "stg_sellers_verified.py"),
]

# Đã sửa lại danh sách: (loại, task_id, file_name)
SPARK_CURATED_TASKS = [
    ("dim", "dim_customers", "dim_customers.py"), # Thêm .py nếu thiếu
    ("dim", "dim_date", "dim_date.py"),
    ("dim", "dim_products", "dim_products.py"),
    ("dim", "dim_sellers", "dim_sellers.py"),
    ("fact", "fact_orders", "fact_orders.py"),
]

with DAG(
    dag_id="lakehouse-pipeline-e-commerce",
    start_date=pendulum.datetime(2026, 2, 7, tz="UTC"),
    schedule="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["lakehouse"],
    params={
        "processing_date": Param(
            default="",
            type="string",
            pattern=r"^$|^\d{4}-\d{2}-\d{2}$",
            description="Định dạng YYYY-MM-DD. Để trống để dùng ngày chạy (ds)."
        )
    },
    render_template_as_native_obj=True,
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Jinja template cho ngày xử lý
    target_date = "{{ params.processing_date if params.processing_date else ds }}"

    with TaskGroup(group_id="ingestion_group") as ingestion_group:
        kagge_ingestion = EmptyOperator(task_id="kagge_ingestion")

    with TaskGroup(group_id="processing_stg_group") as processing_stg_group:
        stg_tasks = []
        for t_id, file_name in SPARK_STG_TASKS:
            task = SparkSubmitOperator(
                task_id=t_id,
                application=f"/opt/airflow/dags/spark_code/stg/e_commerce/{file_name}",
                conn_id="spark-cluster",
                # Giảm tài nguyên để tránh treo máy yếu
                driver_memory="512m",
                executor_memory="512m",
                executor_cores=1,
                num_executors=1,
                application_args=["--datadate", target_date],
            )
            stg_tasks.append(task)

    with TaskGroup(group_id="processing_curated_group") as processing_curated_group:
        dim_tasks = []
        fact_tasks = []
        
        # SỬA LỖI: Lặp đúng danh sách CURATED
        for t_type, t_id, file_name in SPARK_CURATED_TASKS:
            task = SparkSubmitOperator(
                task_id=t_id,
                application=f"/opt/airflow/dags/spark_code/curated/e_commerce/{file_name}",
                conn_id="spark-cluster",
                driver_memory="512m",
                executor_memory="512m",
                executor_cores=1,
                num_executors=1,
                application_args=["--datadate", target_date],
            )
            
            if t_type == 'dim':
                dim_tasks.append(task)
            else:
                fact_tasks.append(task) 

        dim_done = EmptyOperator(task_id="dim_done")
        # Luồng chạy: Xong hết Dim mới chạy Fact
        dim_tasks >> dim_done >> fact_tasks 

    # Định nghĩa luồng chính
    start >> ingestion_group >> processing_stg_group >> processing_curated_group >> end