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
# 1. TẠO BẢNG STG_GEOLOCATION_UNIQUE
# Mục tiêu: Xử lý vấn đề nhân bản dòng bằng cách tính trọng tâm (Centroid) cho mỗi Zip Code 
# ==========================================
print("Processing stg_geolocation_unique...")
path = f's3a://lakehouse/raw/olist_geolocation_dataset.csv'
df_geo_raw = read_data_from_minio(
                                spark=spark,
                                path=path
                                )

# Logic: Group by Zip Code và tính trung bình tọa độ để lấy tâm điểm
stg_geolocation_unique = df_geo_raw.groupBy("geolocation_zip_code_prefix") \
    .agg(
        avg("geolocation_lat").alias("geo_lat"),
        avg("geolocation_lng").alias("geo_lng"),
        first("geolocation_city").alias("geo_city"), # Lấy thành phố đầu tiên làm đại diện
        first("geolocation_state").alias("geo_state")
    ).withColumn("datadate", F.to_date(F.lit(datadate), "yyyy-MM-dd"))

write_iceberg_dynamic_partition(
    spark=spark,
    df=stg_geolocation_unique,
    table_name='nessie.stg.stg_geolocation_unique'
)

spark.stop()
print("[Done] ✅", flush=True)