from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
from pyspark.sql.functions import col

def get_yesterday_string(format="%Y-%m-%d"):
  """
  Lấy ngày T-1 (ngày hôm qua) và trả về dưới dạng chuỗi
  theo định dạng được cung cấp.

  Args:
    format: Định dạng chuỗi ngày tháng (mặc định là 'YYYY-MM-DD').
  """
  # Lấy ngày giờ hiện tại
  today = datetime.today()
  
  # Trừ đi 1 ngày để có ngày hôm qua
  yesterday = today - timedelta(days=1)
  
  # Định dạng ngày hôm qua thành chuỗi và trả về
  return yesterday.strftime(format)

def read_data_from_minio(spark: SparkSession, path: str) -> DataFrame:
    df_raw = (
    spark.read
    .option("header", True)
    .option("multiLine", True)         
    .option("escape", '"')            
    .option("quote", '"')
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")    
    .option("inferSchema", "true")  
    # .schema(custom_schema)          
    .csv(path)
    )

    return df_raw

def norm_sym(c): return F.upper(F.trim(F.col(c)))
def as_double(c): return F.col(c).cast(T.DoubleType())
def as_long(c):   return F.col(c).cast(T.LongType())
def as_long_clean(c):
    return F.regexp_replace(F.col(c).cast("string"), r"[^0-9\.\-]", "").cast(T.LongType())
def as_double_clean(c):
    return F.regexp_replace(F.col(c).cast("string"), r"[^0-9\.\-]", "").cast(T.DoubleType())


def write_iceberg_dynamic_partition(spark: SparkSession, 
                                    df: DataFrame, 
                                    table_name: str, 
                                    partition_cols: list = ["country", "datadate"]):
    """
    Ghi một DataFrame vào bảng Iceberg với cơ chế ghi đè partition động.

    - Nếu bảng chưa tồn tại, tạo bảng mới và partition theo partition_cols.
    - Nếu bảng đã tồn tại, chỉ ghi đè (overwrite) các partition
      có trong DataFrame nguồn (df).

    Args:
        spark (SparkSession): Spark session đã kích hoạt Iceberg.
        df (DataFrame): DataFrame đầu vào (phải chứa 2 cột partition).
        table_name (str): Tên của bảng Iceberg (ví dụ: 'catalog.db.my_table').
        partition_cols (list): Danh sách các cột partition. 
                               Thứ tự rất quan trọng.
                               [ngoài, trong] -> ["country", "datadate"].
    """
    
    # 1. Kiểm tra xem các cột partition có tồn tại trong DataFrame không
    for p_col in partition_cols:
        if p_col not in df.columns:
            raise ValueError(f"Cột partition '{p_col}' không tìm thấy trong DataFrame.")

    print(f"Bắt đầu quá trình ghi vào bảng: {table_name}")

    try:
        # 2. Kiểm tra xem bảng đã tồn tại hay chưa
        if not spark.catalog.tableExists(table_name):
            
            # === TRƯỜNG HỢP 1: Bảng CHƯA TỒN TẠI ===
            # Cần tạo bảng lần đầu. 
            # Chúng ta phải chỉ định .partitionBy() khi tạo bảng.
            
            print(f"Bảng {table_name} chưa tồn tại. Đang tạo bảng mới...")
            
            df.write \
              .format("iceberg") \
              .mode("overwrite") \
              .partitionBy(*partition_cols) \
              .saveAsTable(table_name)
              
            print(f"Đã tạo bảng mới {table_name} và ghi dữ liệu thành công.")

        else:
            
            # === TRƯỜNG HỢP 2: Bảng ĐÃ TỒN TẠI ===
            # Sử dụng chế độ "dynamic partition overwrite".
            
            print(f"Bảng {table_name} đã tồn tại. Áp dụng ghi đè partition động...")

            # Cấu hình Spark để bật chế độ ghi đè partition động
            # (Ghi đè dựa trên dữ liệu, không phải toàn bộ bảng)
            spark.conf.set("spark.sql.iceberg.partition-overwrite-mode", "dynamic")
            
            # Khi ghi, không cần .partitionBy() nữa vì bảng đã tồn tại
            # và đã có siêu dữ liệu (metadata) về partition.
            
            df.writeTo(table_name).overwritePartitions()
              
            
            print(f"Đã ghi đè partition động vào bảng {table_name} thành công.")

    except Exception as e:
        print(f"Đã xảy ra lỗi khi ghi vào bảng {table_name}: {e}")


def format_stg(df: DataFrame) -> DataFrame:
    TARGET_COLS = [
    "country","datadate","symbol","company_name","sector","industry","website",
    "employees","market_cap","currency","exchange","current_price","previous_close",
    "ceiling","floor","ref_price","trading_status","trading_status_code","trading_status_group",
    "foreign_percent","outstanding_share","issue_share","delta_in_week","delta_in_month","delta_in_year",
    "avg_match_vol_2w"
    ]
    TARGET_TYPES = {
    "symbol":"string","country":"string","datadate":"date","company_name":"string","company_name_jp":"string",
    "sector":"string","industry":"string","website":"string","employees":"long","market_cap":"double",
    "currency":"string","exchange":"string","current_price":"double","previous_close":"double",
    "ceiling":"double","floor":"double","ref_price":"double","trading_status":"string",
    "trading_status_code":"string","trading_status_group":"string","foreign_percent":"double",
    "outstanding_share":"long","issue_share":"long","delta_in_week":"double","delta_in_month":"double",
    "delta_in_year":"double","avg_match_vol_2w":"long"
    }

    def _align(df):
        for c in TARGET_COLS:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast(TARGET_TYPES[c]))
            else:
                df = df.withColumn(c, F.col(c).cast(TARGET_TYPES[c]))
        return df.select(*TARGET_COLS)
    
    final_df = _align(df)
    final_df = final_df.select(*TARGET_COLS)
    
    return final_df