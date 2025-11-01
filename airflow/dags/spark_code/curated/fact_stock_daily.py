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

    print("Reading from staging")
    stg_df = spark.sql("select * from nessie.stg.stg_stock")

    print("Reading from staging")
    # ---- Load DIMs
    date_df = spark.table("nessie.curated.dim_date").select("date_sk","date")
    exch_df = spark.table("nessie.curated.dim_exchange") \
                .select("exchange_sk", F.upper(F.col("exchange_code")).alias("ex_code"), F.col("country").alias("ex_country"))
    cur_df  = spark.table("nessie.curated.dim_currency").select("currency_sk","currency_code")
    ts_df   = spark.table("nessie.curated.dim_trading_status") \
                .select("trading_status_sk", "status_code", "status_group")
    comp_all = spark.table("nessie.curated.dim_company") \
                    .select("company_sk","symbol","country","effective_from","effective_to")

    # ---- Base từ STG: gom cả cột price + snapshot
    base = (stg_df
    .select("symbol","country","datadate","exchange","currency",
            "current_price","previous_close","ref_price","ceiling","floor",
            "delta_in_week","delta_in_month","delta_in_year","avg_match_vol_2w",
            "trading_status_code","trading_status_group",
            "market_cap","employees","outstanding_share","issue_share","foreign_percent")
    .where(F.col("symbol").isNotNull() & F.col("datadate").isNotNull())
    .withColumn("date_sk", F.date_format("datadate","yyyyMMdd").cast("int"))
    .withColumn("pct_change",
        F.when(F.col("previous_close").isNotNull() & (F.col("previous_close")!=0),
                (F.col("current_price")-F.col("previous_close"))/F.col("previous_close")))
    .withColumn("is_limit_up",   F.col("ceiling").isNotNull() & (F.col("current_price")>=F.col("ceiling")))
    .withColumn("is_limit_down", F.col("floor").isNotNull()   & (F.col("current_price")<=F.col("floor")))
    )

    # ---- Map DIM keys
    # date
    fact0 = base.join(date_df, "date_sk", "left")

    # exchange
    fact1 = fact0.join(
        exch_df,
        (F.upper(fact0["exchange"])==exch_df["ex_code"]) & (fact0["country"]==exch_df["ex_country"]),
        "left"
    )

    # currency
    fact2 = fact1.join(cur_df, fact1["currency"]==cur_df["currency_code"], "left")

    # trading status (VN)
    fact3 = fact2.join(
        ts_df,
        (fact2["trading_status_code"]==ts_df["status_code"]) & (fact2["trading_status_group"]==ts_df["status_group"]),
        "left"
    )

    # company PIT join: datadate ∈ [effective_from, effective_to]
    fact4 = fact3.alias("f").join(
        comp_all.alias("d"),
        (F.col("f.symbol")==F.col("d.symbol")) &
        (F.col("f.country")==F.col("d.country")) &
        (F.col("f.datadate")>=F.col("d.effective_from")) &
        (F.col("f.datadate")<=F.col("d.effective_to")),
        "left"
    )

    fact_equity_daily = fact4.select(
        F.col("f.date_sk"),
        F.col("d.company_sk"),
        F.col("exchange_sk"),
        F.col("currency_sk"),
        F.col("trading_status_sk"),
        F.col("f.current_price"), F.col("f.previous_close"), F.col("f.ref_price"),
        F.col("f.ceiling"), F.col("f.floor"), F.col("f.pct_change"),
        F.col("f.delta_in_week"), F.col("f.delta_in_month"), F.col("f.delta_in_year"),
        F.col("f.avg_match_vol_2w"), F.col("f.is_limit_up"), F.col("f.is_limit_down"),
        # snapshot attributes
        F.col("f.market_cap"), F.col("f.employees"), F.col("f.outstanding_share"),
        F.col("f.issue_share"), F.col("f.foreign_percent")
    )

    # Ghi dữ liệu
    write_iceberg_dynamic_partition(
        spark=spark,
        df=fact_equity_daily,
        table_name='nessie.curated.fact_equity_daily',
        partition_cols=['date_sk']
    )

    spark.stop()
    print("[Done] ✅", flush=True)

if __name__ == "__main__":
    main()
