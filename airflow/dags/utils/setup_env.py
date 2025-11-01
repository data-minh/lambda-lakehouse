import os

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




