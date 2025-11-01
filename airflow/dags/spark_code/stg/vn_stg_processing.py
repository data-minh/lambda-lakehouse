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
    path = f's3a://lakehouse/raw/stock-data/vnstock/vnstock_finally/{datadate}.csv'
    df_vn_raw = read_data_from_minio(
                                    spark=spark,
                                    path=path
                                    )

    print("Xử lý dữ liệu Stg")
    # VN
    stg_vn = (
    df_vn_raw
    .withColumn("country", F.lit("VN"))
    .withColumn("datadate", F.lit(datadate).cast("date"))
    .withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
    .withColumn("company_name", F.col("company_name"))
    .withColumn("sector", F.col("industry"))
    .withColumn("industry", F.col("industry"))
    .withColumn("website", F.col("website"))
    .withColumn("employees", F.col("no_employees").cast("long"))
    # Làm sạch số
    .withColumn("ref_price_clean",      as_double_clean("ref_price"))
    .withColumn("prior_close_clean",    as_double_clean("prior_close_price"))
    .withColumn("ceiling",              as_double_clean("ceiling"))
    .withColumn("floor",                as_double_clean("floor"))
    .withColumn("foreign_percent",      as_double_clean("foreign_percent"))
    .withColumn("delta_in_week",        as_double_clean("delta_in_week"))
    .withColumn("delta_in_month",       as_double_clean("delta_in_month"))
    .withColumn("delta_in_year",        as_double_clean("delta_in_year"))
    .withColumn("avg_match_vol_2w",     F.col("average_match_volume2_week").cast("long"))
    .withColumn("outstanding_share",    F.col("outstanding_share")*F.lit(1_000_000).cast("long"))
    .withColumn("issue_share",          F.col("issue_share").cast("long"))
    # Exchange: lấy từ overview, nếu null thì lấy exchange_price (đang là mã sàn)
    .withColumn("exchange",
        F.coalesce(F.col("exchange_overview"), F.col("exchange_price")).cast("string")
    )
    # current_price fallback: không có giá khớp trong dataset này
    .withColumn("current_price",
        F.coalesce(F.col("ref_price_clean"), F.col("prior_close_clean"))
    )
    .withColumn("price_source",
        F.when(F.col("ref_price_clean").isNotNull(),   F.lit("ref_price"))
        .when(F.col("prior_close_clean").isNotNull(), F.lit("previous_close"))
        .otherwise(F.lit(None).cast("string"))
    )
    # previous_close
    .withColumn("previous_close", F.col("prior_close_clean"))
    # market_cap (tỷ VND)
    .withColumn(
        "market_cap",
        F.when(
            F.col("outstanding_share").isNotNull() & F.col("current_price").isNotNull(),
            (F.col("outstanding_share") * F.col("current_price"))
        )
    )
    # Các trường VN khác giữ nguyên/cast
    .withColumn("currency", F.lit("VND"))
    .withColumn("trading_status", F.col("trading_status"))
    .withColumn("trading_status_code", F.col("trading_status_code"))
    .withColumn("trading_status_group", F.col("trading_status_group"))
    .select(
        "symbol","country","datadate","company_name","sector","industry","website",
        "employees","market_cap","currency","exchange","current_price","previous_close",
        "ceiling","floor","ref_price_clean","trading_status","trading_status_code","trading_status_group",
        "foreign_percent","outstanding_share","issue_share","delta_in_week","delta_in_month","delta_in_year",
        "avg_match_vol_2w","price_source"
    )
    .withColumnRenamed("ref_price_clean","ref_price")
    .where(F.col("symbol").isNotNull() & (F.col("symbol") != ""))
    )

    # Chuẩn hóa Stg
    print("Chuẩn hóa lại Stg")
    stg_vn = format_stg(stg_vn)

    # Ghi dữ liệu
    write_iceberg_dynamic_partition(
        spark=spark,
        df=stg_vn,
        table_name='nessie.stg.stg_stock'
    )

    spark.stop()
    print("[Done] ✅", flush=True)

if __name__ == "__main__":
    main()
