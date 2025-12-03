import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

# --- CẤU HÌNH CHUNG ---
BUCKET_NAME = "my-ecommerce-dev-bucket"
KEY_PATH = "/home/tanh/.gcp_keys/ecommerce-sa.json"

RAW_ORDERS = f"gs://{BUCKET_NAME}/raw/order_raw/*/*.jsonl"
BRONZE_ORDERS = f"gs://{BUCKET_NAME}/bronze/order_events/"

RAW_USERS = f"gs://{BUCKET_NAME}/raw/users/*/*.json"
BRONZE_USERS = f"gs://{BUCKET_NAME}/bronze/users/"

RAW_PRODUCTS = f"gs://{BUCKET_NAME}/raw/products/*/*/*.json"
BRONZE_PRODUCTS = f"gs://{BUCKET_NAME}/bronze/products/"


def create_spark_session():
    spark = SparkSession.builder \
        .appName("ETL_Raw_To_Bronze_Multi_Source") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()
    return spark

def process_single_table(spark, table_name, input_path, output_path, is_multiline=False):

    print(f"Đang xử lý bảng: {table_name}")
    print(f"Input: {input_path}")
    print(f"Multiline Mode: {is_multiline}")

    try:
        reader = spark.read.option("recursiveFileLookup", "true")
        
        if is_multiline:
            reader = reader.option("multiline", "true")
            
        df = reader.json(input_path)
        
        if df.rdd.isEmpty():
            print(f"   ⚠️ Cảnh báo: Không có dữ liệu tại {input_path}")
            return

        df_transformed = df.withColumn("ingest_time", F.current_timestamp()) \
                           .withColumn("source_file", F.input_file_name()) \
                           .withColumn("data_source", F.lit(table_name))

        df_transformed.write \
            .mode("append") \
            .format("parquet") \
            .save(output_path)
            
        print(f"Đã ghi xong: {output_path}")
        
    except Exception as e:
        print(f"Lỗi khi xử lý {table_name}: {e}")

def main():
    if not os.path.exists(KEY_PATH):
        print(f"Không tìm thấy key tại: {KEY_PATH}")
        sys.exit(1)

    spark = create_spark_session()
    process_single_table(spark, "order_events", RAW_ORDERS, BRONZE_ORDERS, is_multiline=False)
    process_single_table(spark, "users", RAW_USERS, BRONZE_USERS, is_multiline=True)
    process_single_table(spark, "products", RAW_PRODUCTS, BRONZE_PRODUCTS, is_multiline=True)
    print("Hoàn thành toàn bộ job")
    spark.stop()

if __name__ == "__main__":
    main()