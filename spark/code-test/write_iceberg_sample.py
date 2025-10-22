# file: write_iceberg_sample.py
import os
import sys
import platform
from pyspark.sql import SparkSession, functions as F, types as T
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
        .config("spark.jars.packages", ",".join([
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.103.2",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
            "org.apache.iceberg:iceberg-aws-bundle:1.8.1",
            ## Minio
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ]))
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

    # 1) Đảm bảo namespace tồn tại
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.demo")

    print("Start reading data from Minio")
    df = spark.read.csv("s3a://lakehouse/raw/vnstock_1d.csv", header=True, inferSchema=True)

    print("Completed reading data from Minio")

    df.writeTo("nessie.demo.vnstock_1d_submit").createOrReplace()
    # df.write.parquet("s3a://lakehouse/raw/vnstock_1d_parquet")
    # print("Write data to Minio")

    spark.stop()
    print("[Done] ✅", flush=True)

if __name__ == "__main__":
    main()

# spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/code/write_iceberg_sample.py