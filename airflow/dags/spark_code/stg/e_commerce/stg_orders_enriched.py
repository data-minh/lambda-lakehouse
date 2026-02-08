import os
import sys
import platform
from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
from pyspark.sql.functions import (
    col, avg, first, count, when, datediff, 
    to_timestamp, coalesce, lit, lpad
)
from dags.utils.init_spark import (
                                get_yesterday_string,
                                read_data_from_minio,
                                norm_sym,
                                as_double,
                                as_long,
                                as_long_clean,
                                as_double_clean,
                                format_stg,
                                write_iceberg_dynamic_partition
                                )
from dags.utils.setup_env import (
                                get_processing_date
                                )
# from dotenv import load_dotenv

NESSIE_URI = os.environ.get("NESSIE_URI") 
MINIO_ACCESS_KEY=os.environ.get("AWS_ACCESS_KEY_ID") 
MINIO_SECRET_KEY=os.environ.get("AWS_SECRET_ACCESS_KEY") 
MINIO_ENDPOINT=os.environ.get("AWS_S3_ENDPOINT") 

print("[ENV] NESSIE_URI           =", NESSIE_URI, flush=True)
print("[ENV] AWS_S3_ENDPOINT      =", MINIO_ENDPOINT, flush=True)

# Tạo SparkSession: Iceberg + Nessie + S3FileIO (không cần hadoop-aws cho thao tác Iceberg)
spark = (
    SparkSession.builder
        .appName("Iceberg-Nessie-rest-catalog")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type", "rest")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        ## Minio config
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
)

#HÀM NHẬN DATADATE TỪ THAM SỐ DÒNG LỆNH
import argparse
def get_args():
    parser = argparse.ArgumentParser()
    # Nhận tham số --datadate mà chúng ta đã cấu hình ở Airflow DAG
    parser.add_argument("--datadate", help="Processing date from Airflow")
    args, unknown = parser.parse_known_args()
    return args.datadate

# Lấy giá trị datadate
datadate = get_args()
# ==========================================
# 3. TẠO BẢNG STG_ORDERS_ENRICHED
# Mục tiêu: Chuyển đổi kiểu thời gian và tính toán các chỉ số hiệu suất (SLA) 
# ==========================================
print("Processing stg_orders_enriched...")
olist_orders_dataset = f's3a://lakehouse/raw/olist_orders_dataset.csv'
df_orders_raw = read_data_from_minio(
                                spark=spark,
                                path=olist_orders_dataset
                                )

# Chuyển đổi string sang timestamp
time_cols = [
    "order_purchase_timestamp", "order_approved_at", 
    "order_delivered_carrier_date", "order_delivered_customer_date", 
    "order_estimated_delivery_date"
]

df_orders_temp = df_orders_raw
for c in time_cols:
    df_orders_temp = df_orders_temp.withColumn(c, to_timestamp(col(c)))

# Logic: Tính toán độ trễ và thời gian giao hàng
stg_orders_enriched = df_orders_temp.withColumn(
    "delivery_days", 
    datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
).withColumn(
    "estimated_days",
    datediff(col("order_estimated_delivery_date"), col("order_approved_at"))
).withColumn(
    # Nếu ngày giao thực tế > ngày dự kiến -> Giao trễ (1), ngược lại (0)
    "is_late_delivery",
    when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1).otherwise(0)
).select(
    "order_id", "customer_id", "order_status",
    "order_purchase_timestamp", "order_approved_at", 
    "order_delivered_customer_date", "order_estimated_delivery_date",
    "delivery_days", "estimated_days", "is_late_delivery"
).withColumn("datadate", F.to_date(F.lit(datadate), "yyyy-MM-dd"))

write_iceberg_dynamic_partition(
    spark=spark,
    df=stg_orders_enriched,
    table_name='nessie.stg.stg_orders_enriched'
)

spark.stop()
print("[Done] ✅", flush=True)