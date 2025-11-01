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
import pycountry
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

    print("Reading from staging")
    stg_df = spark.sql("select * from nessie.stg.stg_stock")

    print("Đang tạo bảng tra cứu tên tiền tệ (df_names_lookup)...")
    currency_name_data = []
    # Lấy tất cả tiền tệ từ thư viện pycountry
    for currency in pycountry.currencies:
        currency_name_data.append((currency.alpha_3, currency.name))

    # Thêm 'VND' (vì pycountry không có)
    currency_name_data.append(("VND", "Vietnamese Dong"))

    df_names_lookup = spark.createDataFrame(
        currency_name_data, 
        ["currency_code", "currency_name"]
    )

    # --- 2. TẠO BẢNG TRA CỨU TỶ GIÁ (Từ cấu trúc JSON) ---
    print("Đang tạo bảng tra cứu tỷ giá (df_rates_lookup)...")

    # Khai báo tỷ giá bằng cấu trúc Python dict (giống JSON)
    rates_json_style = {
        "VND": 1,
        "USD": 25000,
        "JPY": 170
        # Bạn có thể thêm các đồng tiền khác vào đây
    }

    # Chuyển đổi dict sang danh sách (list) các tuple để Spark đọc
    rate_data_list = list(rates_json_style.items())

    # Tạo DataFrame tra cứu tỷ giá
    df_rates_lookup = spark.createDataFrame(
        rate_data_list, 
        ["currency_code", "exchange_rate_to_vnd"]
    )

    print("Done")

    # 3.1. Lấy danh sách currency_code duy nhất từ nguồn
    dim_cur_base = (stg_df
        .select(F.upper(F.col("currency")).alias("currency_code")) # UPPER-case để chuẩn hoá
        .where(F.col("currency_code").isNotNull())
        .distinct()
    )

    # 3.2. Join với Bảng Tra Cứu Tên
    # Dùng F.broadcast() vì bảng lookup rất nhỏ, giúp tăng tốc độ join
    dim_cur_with_name = dim_cur_base.join(
        F.broadcast(df_names_lookup),
        "currency_code",
        "left"  # Giữ lại code dù không tìm thấy tên
    )

    # 3.3. Join với Bảng Tra Cứu Tỷ Giá
    dim_cur_with_rate = dim_cur_with_name.join(
        F.broadcast(df_rates_lookup),
        "currency_code",
        "left"  # Giữ lại code dù không tìm thấy tỷ giá
    )

    # 3.4. Thêm Surrogate Key (SK) và sắp xếp cột
    dim_currency = (dim_cur_with_rate
        .withColumn("currency_sk", F.crc32(F.col("currency_code")).cast("bigint"))
        # Sắp xếp lại các cột theo đúng thứ tự bạn muốn
        .select(
            "currency_sk",
            "currency_code",
            "currency_name",
            "exchange_rate_to_vnd"
        )
    )

    # Ghi dữ liệu
    write_iceberg_dynamic_partition(
        spark=spark,
        df=dim_currency,
        table_name='nessie.curated.dim_currency',
        partition_cols=['currency_code']
    )

    spark.stop()
    print("[Done] ✅", flush=True)

if __name__ == "__main__":
    main()
