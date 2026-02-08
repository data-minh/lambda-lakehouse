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

stg_orders_enriched = spark.sql("select * from nessie.stg.stg_orders_enriched").filter(F.col('datadate')==datadate)
stg_order_items_clean = spark.sql("select * from nessie.stg.stg_order_items_clean").filter(F.col('datadate')==datadate)

dim_products = spark.sql("select * from nessie.curated.dim_products")
dim_sellers = spark.sql("select * from nessie.curated.dim_sellers")
stg_customers_verified = spark.sql("select * from nessie.stg.stg_customers_verified")
dim_customers = spark.sql("select * from nessie.curated.dim_customers")
dim_date = spark.sql("select * from nessie.curated.dim_date")
# ==========================================
# 2. XÂY DỰNG FACT TABLE (BẢNG DỮ KIỆN)
# ==========================================

# Bước 1: Join Orders (Header) với Order Items (Detail)
# Đây là Fact giao dịch chi tiết (Transactional Fact)
fact_step1 = stg_orders_enriched.join(
    stg_order_items_clean,
    "order_id",
    "inner"
)

# Bước 2: Lookup Keys từ các bảng Dimension
# Để chuẩn mô hình Star Schema, ta thay thế ID gốc bằng Key (nếu cần tối ưu storage/performance)
# Tuy nhiên, ta vẫn giữ ID gốc (Degenerate Dimension) để dễ debug và đếm số lượng

fact_orders = fact_step1.alias("f") \
    .join(dim_products.alias("p"), col("f.product_id") == col("p.product_id"), "left") \
    .join(dim_sellers.alias("s"), col("f.seller_id") == col("s.seller_id"), "left") \
    .join(stg_customers_verified.alias("c_stg"), col("f.customer_id") == col("c_stg.customer_id"), "left") \
    .join(dim_customers.alias("c"), col("c_stg.customer_unique_id") == col("c.customer_unique_id"), "left") \
    .join(dim_date.alias("d"), to_date(col("f.order_purchase_timestamp")) == col("d.full_date"), "left") \
    .select(
        # --- Keys ---
        col("c.customer_key"),
        col("p.product_key"),
        col("s.seller_key"),
        col("d.date_key").alias("order_date_key"),
        
        # --- Degenerate Dimensions (IDs) ---
        col("f.order_id"),
        col("f.order_status"),
        col("f.customer_id"), # Giữ lại để tham chiếu giao dịch nếu cần
        
        # --- Metrics (Chỉ số) ---
        col("f.price"),
        col("f.freight_value"),
        (col("f.price") + col("f.freight_value")).alias("total_amount"),
        lit(1).alias("quantity"), # Mỗi dòng là 1 item
        
        # --- Calculated Metrics (Hiệu suất) ---
        col("f.delivery_days"),
        col("f.estimated_days"),
        col("f.is_late_delivery")
    )

write_iceberg_no_partition(
    spark=spark,
    df=fact_orders,
    table_name='nessie.curated.fact_orders'
)

spark.stop()
print("[Done] ✅", flush=True)