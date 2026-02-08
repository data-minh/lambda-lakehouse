# dags/print_processing_date_params.py
from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from airflow.utils.types import DagRunType
from airflow.models.param import Param

@dag(
    dag_id="print_processing_date_params",
    schedule="0 1 * * *",  # 01:00 UTC
    start_date=pendulum.datetime(2025, 10, 30, tz="UTC"),
    catchup=False,
    tags=["demo", "params"],
    # Cho phép rỗng ("") hoặc YYYY-MM-DD -> tránh lỗi validate lúc parse DAG
    params={
        "processing_date": Param(
            default="",                          # KHÔNG dùng None
            type="string",
            pattern=r"^$|^\d{4}-\d{2}-\d{2}$",
            description="YYYY-MM-DD; để trống nếu muốn dùng ngày UTC hôm nay."
        )
    },
)
def print_processing_date_params():
    @task
    def print_date(**context):
        dr = context["dag_run"]
        run_type = getattr(dr, "run_type", None)
        is_manual = (run_type == DagRunType.MANUAL) or bool(getattr(dr, "external_trigger", False))

        # Lấy từ params (UI → Parameters)
        params = context.get("params", {}) or {}
        val = params.get("processing_date") or ""
        if isinstance(val, str):
            val = val.strip()

        if is_manual and val:
            processing_date = val               # Manual + có nhập params
        else:
            processing_date = pendulum.now("UTC").format("YYYY-MM-DD")  # Scheduled hoặc không nhập

        print("=== RUN INFO ===")
        print(f"run_id          : {dr.run_id}")
        print(f"run_type        : {run_type}")
        print(f"manual?         : {is_manual}")
        print("-----------------")
        print(f"processing_date : {processing_date}")

    print_date()

print_processing_date_params()
