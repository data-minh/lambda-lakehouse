# file: write_iceberg_sample.py
import os
import sys
import platform
from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
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


def main():
    # load_dotenv()
    # get datedate: t-1
    # datadate = get_yesterday_string()
    datadate = '2025-10-24'

    print(f"Bắt đầu xử lý dữ liệu với ngày {datadate}")

    print("Start reading data from Minio")
    path = f's3a://lakehouse/raw/stock-data/us/usstock_finally/{datadate}.csv'
    df_us_raw = read_data_from_minio(
                                    spark=spark,
                                    path=path
                                    )

    print("Xử lý dữ liệu Stg")
    # US
    stg_us = (
    df_us_raw
    .withColumn("country", F.lit("US"))
    .withColumn("symbol", norm_sym("symbol"))
    .withColumn("datadate", F.to_date("datadate"))
    .withColumn("company_name", F.col("company_name"))
    .withColumn("sector", F.col("sector"))
    .withColumn("industry", F.col("industry"))
    .withColumn("website", F.col("website"))
    .withColumn("employees", as_long_clean("full_time_employees"))
    .withColumn("market_cap", as_double_clean("market_cap"))
    .withColumn("currency", F.col("currency"))
    .withColumn("exchange", F.lit(None).cast("string"))
    .withColumn("current_price", as_double_clean("current_price"))
    .withColumn("previous_close", as_double_clean("previous_close"))
    .select("symbol","country","datadate","company_name","sector","industry","website",
            "employees","market_cap","currency","exchange","current_price","previous_close")
    .where(F.col("symbol").isNotNull() & (F.col("symbol") != ""))
    )

    # Chuẩn hóa Stg
    print("Chuẩn hóa lại Stg")
    stg_us = format_stg(stg_us)

    # Ghi dữ liệu
    write_iceberg_dynamic_partition(
        spark=spark,
        df=stg_us,
        table_name='nessie.stg.stg_stock'
    )

    spark.stop()
    print("[Done] ✅", flush=True)

if __name__ == "__main__":
    main()
