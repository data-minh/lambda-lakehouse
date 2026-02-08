import os
import sys
import platform
from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
from pyspark.sql.functions import (
    monotonically_increasing_id, row_number, col, 
    lit, dayofweek, month, year, quarter, 
    date_format, sequence, to_date, explode, 
    sum as _sum, count as _count
)
from pyspark.sql.window import Window
from dags.utils.init_spark import (
                                get_yesterday_string,
                                read_data_from_minio,
                                norm_sym,
                                as_double,
                                as_long,
                                as_long_clean,
                                as_double_clean,
                                format_stg,
                                write_iceberg_dynamic_partition,
                                write_iceberg_no_partition
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

stg_customers_verified = spark.sql("select * from nessie.stg.stg_customers_verified").filter(F.col('datadate')==datadate)
stg_geolocation_unique = spark.sql("select * from nessie.stg.stg_geolocation_unique").filter(F.col('datadate')==datadate)
# --- 1.2 Dim_Customers ---
# Join với Geo để lấy tọa độ Centroid -> Phục vụ vẽ Heatmap
# Lưu ý: Dùng customer_unique_id làm gốc (theo báo cáo)
dim_customers = stg_customers_verified.join(
    stg_geolocation_unique,
    stg_customers_verified["customer_zip_code"] == stg_geolocation_unique["geolocation_zip_code_prefix"],
    "left"
).select(
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "geo_lat", 
    "geo_lng"
).distinct() # Loại bỏ trùng lặp nếu có

# Tạo customer_key
w_cust = Window.orderBy("customer_unique_id")
dim_customers = dim_customers.withColumn(
    "customer_key", row_number().over(w_cust)
)

write_iceberg_no_partition(
    spark=spark,
    df=dim_customers,
    table_name='nessie.curated.dim_customers'
)

spark.stop()
print("[Done] ✅", flush=True)