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
# 2. TẠO BẢNG STG_PRODUCTS_EN
# Mục tiêu: Dịch tên danh mục sang tiếng Anh để báo cáo chuẩn hóa 
# ==========================================
print("Processing stg_products_en...")
olist_products_dataset = f's3a://lakehouse/raw/olist_products_dataset.csv'
product_category_name_translation = f's3a://lakehouse/raw/product_category_name_translation.csv'
df_products_raw = read_data_from_minio(
                                spark=spark,
                                path=olist_products_dataset
                                )
df_translation = read_data_from_minio(
                                spark=spark,
                                path=product_category_name_translation
                                )

# 2. Đặt Alias (Tên giả) cho DataFrame để dễ gọi
p = df_products_raw.alias("p")      # p đại diện cho Product
t = df_translation.alias("t")       # t đại diện cho Translation

# 3. Join và Select dùng Alias
stg_products_en = p.join(
    t, 
    col("p.product_category_name") == col("t.product_category_name"), 
    "left"
).select(
    col("p.product_id"),
    
    # SỬA LỖI TẠI ĐÂY: Dùng p.product_category_name thay vì tên biến dài
    coalesce(
        col("t.product_category_name_english"), 
        col("p.product_category_name"), 
        lit("Unknown")
    ).alias("category_name"),
    
    col("p.product_weight_g"),
    col("p.product_length_cm"),
    col("p.product_height_cm"),
    col("p.product_width_cm"),
    col("p.product_photos_qty")
).withColumn("datadate", F.to_date(F.lit(datadate), "yyyy-MM-dd"))

write_iceberg_dynamic_partition(
    spark=spark,
    df=stg_products_en,
    table_name='nessie.stg.stg_products_en'
)

spark.stop()
print("[Done] ✅", flush=True)