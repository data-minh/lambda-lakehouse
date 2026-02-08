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
# --- 1.4 Dim_Date (Time Intelligence) ---
# Tự động sinh chuỗi ngày từ 2016 đến 2018
start_date = "2000-01-01"
end_date = "2099-12-31"

df_date_range = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date")

dim_date = df_date_range.withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int")) \
    .withColumn("year", year(col("full_date"))) \
    .withColumn("quarter", quarter(col("full_date"))) \
    .withColumn("month", month(col("full_date"))) \
    .withColumn("day_name", date_format(col("full_date"), "EEEE")) \
    .withColumn("is_weekend", F.when(dayofweek(col("full_date")).isin(1, 7), 1).otherwise(0)) # 1=Sun, 7=Sat

write_iceberg_no_partition(
    spark=spark,
    df=dim_date,
    table_name='nessie.curated.dim_date'
)

spark.stop()
print("[Done] ✅", flush=True)