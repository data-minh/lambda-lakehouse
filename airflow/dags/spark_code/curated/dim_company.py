# file: write_iceberg_sample.py
import os
import sys
import platform
from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
from pyspark.sql import Window as W
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

    print("Reading from staging")
    stg_df = spark.sql("select * from nessie.stg.stg_stock")

    comp_cols = ["company_name","sector","industry","website","employees","exchange"]
    comp = (stg_df
    .select("symbol","country","datadate", *comp_cols)
    .where(F.col("symbol").isNotNull() & F.col("datadate").isNotNull())
    .withColumn("attr_fingerprint", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in comp_cols]), 256))
    .dropDuplicates(["symbol","country","datadate","attr_fingerprint"])
    )

    w = W.partitionBy("symbol","country").orderBy("datadate")
    scd2 = (comp
    .withColumn("prev_fp", F.lag("attr_fingerprint").over(w))
    .withColumn("chg", F.when(F.col("prev_fp").isNull() | (F.col("prev_fp")!=F.col("attr_fingerprint")), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("grp", F.sum("chg").over(w))  # nhóm phiên bản
    )

    # Tính effective_from cho từng phiên bản (grp), rồi dùng LEAD để lấy next_effective_from
    rng_base = (scd2
    .groupBy("symbol","country","grp")
    .agg(F.min("datadate").alias("effective_from"))
    )

    w_ver = W.partitionBy("symbol","country").orderBy("effective_from")

    rng = (rng_base
    .withColumn("next_effective_from", F.lead("effective_from").over(w_ver))
    # Rule bạn yêu cầu:
    # - Nếu có phiên bản kế tiếp tại ngày D_next, bản ghi hiện tại đóng vào đúng ngày D_next
    # - Nếu không có phiên bản kế tiếp -> đang hiệu lực, set 9999-12-31
    .withColumn(
        "effective_to",
        F.coalesce(F.col("next_effective_from"), F.to_date(F.lit("9999-12-31")))
    )
    .withColumn("is_current", F.col("next_effective_from").isNull())
    .drop("next_effective_from")
    )

    dim_company = (scd2
    .join(rng, ["symbol","country","grp"], "inner")
    .drop("prev_fp","chg")
    .withColumn("company_sk",
        F.crc32(F.concat_ws(":", "symbol","country", F.col("effective_from").cast("string"))).cast("bigint"))
    .withColumnRenamed("exchange","exchange_code")
    .select("company_sk","symbol","country","exchange_code","company_name",
            "sector","industry","website","employees","effective_from","effective_to","is_current")
    )

    # Ghi dữ liệu
    write_iceberg_dynamic_partition(
        spark=spark,
        df=dim_company,
        table_name='nessie.curated.dim_company',
        partition_cols=['country']
    )

    spark.stop()
    print("[Done] ✅", flush=True)

if __name__ == "__main__":
    main()
