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

stg_products_en = spark.sql("select * from nessie.stg.stg_products_en").filter(F.col('datadate')==datadate)
# --- 1.1 Dim_Products ---
# Tạo khóa product_key (surrogate key)
w_prod = Window.orderBy("product_id")
dim_products = stg_products_en.withColumn(
    "product_key", row_number().over(w_prod)
).select(
    "product_key",
    "product_id", 
    "category_name",
    "product_weight_g",
    "product_length_cm", 
    "product_photos_qty"
)

write_iceberg_no_partition(
    spark=spark,
    df=dim_products,
    table_name='nessie.curated.dim_products'
)

spark.stop()
print("[Done] ✅", flush=True)