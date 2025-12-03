import os
from pyspark.sql import SparkSession

def get_spark(app_name="ecommerce-batch"):
    """
    Tạo spark session với cấu hình GCS connector
    """
    
    KEY_PATH = "/home/tanh/.gcp_keys/ecommerce-sa.json"
    
    if not os.path.exists(KEY_PATH):
        raise FileNotFoundError(f"Không tìm thấy key tại: {KEY_PATH}")
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.sql.parquet.compression.codec", "snappy")
        
    return builder.getOrCreate()
            