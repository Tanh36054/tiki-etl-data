import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

BUCKET_NAME = "my-ecommerce-dev-bucket"

KEY_PATH = "/home/tanh/.gcp_keys/ecommerce-sa.json"

RAW_PATH = f"gs://{BUCKET_NAME}/raw/order_raw/*/*.jsonl"   

BRONZE_PATH = f"gs://{BUCKET_NAME}/bronze/order_events/"

def create_spark_session():
    """
    Khoi tao Spark session voi cau hinh GCS connector
    """
    spark = SparkSession.builder \
        .appName("ETL_Raw_To_Bronze") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()
    return spark 

def process_raw_to_bronze():
    spark = create_spark_session()
    
    print("Đang đọc dữ liệu từ: {RAW_PATH}")
    
    # 1. Đọc dữ liệu JSON/JSONL
    try:
        df = spark.read.json(RAW_PATH)
    except Exception as e:
        print(f"Lỗi khi đọc file (Có thể chưa có data): {e}")
        spark.stop()
        return
    
    if df.rdd.isEmpty():
        print("Không có dữ liêu mới để xử lý.")
        spark.stop()
        return
    
    print("Schema gốc của dữ liệu")
    df.printSchema()
    
    # 2. Transform (Thêm Metadata)
    df_transformed = df.withColumn("ingestion_date", current_timestamp()) \
                       .withColumn("source_file", input_file_name()) \
                       .withColumn("data_source", lit("kafka_order_events"))
                    
    # 3. Ghi dữ liệu ra Bronze(Parquet)
    print(f"Đang ghi dữ liệu ra: {BRONZE_PATH}")
    
    df_transformed.write \
        .mode("append") \
        .format("parquet") \
        .save(BRONZE_PATH)
        
    print("Hoàn thành ETL Raw -> Bronze.")
    spark.stop()
    
if __name__ == "__main__":
    if not os.path.exists(KEY_PATH):
        print(f"Không tìm thấy key tại: {KEY_PATH}")
        sys.exit(1)
        
    process_raw_to_bronze()
    
    