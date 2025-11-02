import os
import pendulum
from airflow.decorators import dag, task
from airflow.utils.types import DagRunType
from airflow.models.param import Param

def setup_env() -> None:

    s3_config = {
    "AWS_S3_ENDPOINT": "http://minio:9000/",
    "S3_BUCKET": "lakehouse",
    "S3_PREFIX_VNSTOCK": "raw/vnstock/vnstock_finally"
    }

    print(f"Bắt đầu thiết lập {len(s3_config)} biến môi trường...")

    # os.environ.update() sẽ thêm mới hoặc ghi đè
    # các biến trong dict env_overrides vào môi trường hiện tại.
    os.environ.update(s3_config)

def get_processing_date(**context):
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

    return processing_date




