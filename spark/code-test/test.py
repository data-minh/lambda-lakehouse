import sys, os, platform, shutil
from pyspark.sql import SparkSession, functions as F

print("[1] Python info:", sys.version.split("\n")[0], flush=True)
print("[1] Exec:", sys.executable, "| platform:", platform.platform(), flush=True)

spark = (SparkSession.builder
         .appName("pyspark-smoke-check")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())

print("[2] Spark version:", spark.version, "| master:", spark.sparkContext.master, flush=True)
print("[2] PYSPARK_PYTHON:", os.environ.get("PYSPARK_PYTHON"),
      "| PYSPARK_DRIVER_PYTHON:", os.environ.get("PYSPARK_DRIVER_PYTHON"), flush=True)

# 3) RDD test
print("[3] RDD test ...", flush=True)
rdd = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("a", 3)])
print("[3] RDD reduceByKey:", rdd.reduceByKey(lambda x,y: x+y).collect(), flush=True)

# Chuẩn bị DataFrame nếu chưa có
print("[4] Create DataFrame ...", flush=True)
df = spark.range(0, 1000).withColumn("even", (F.col("id") % 2 == 0))

# 4) Ghi/đọc Parquet
out_path = "/tmp/pyspark_smoke_parquet"
print(f"[4] Clean output path: {out_path}", flush=True)
shutil.rmtree(out_path, ignore_errors=True)

print("[4] Write parquet ...", flush=True)
df.write.mode("overwrite").parquet(out_path)

print("[4] Read parquet ...", flush=True)
df2 = spark.read.parquet(out_path)
print("[4] Read-back count:", df2.count(), flush=True)

print("=== All good! ===", flush=True)
spark.stop()
