import os
from pyspark.sql import SparkSession

def get_spark(app_name):
    """
    Tạo spark session với cấu hình GCS connector
    """

    KEY_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    JAR_PATH = "/opt/airflow/plugins/gcs-connector-hadoop3-shaded.jar"
    
    if not os.path.exists(KEY_PATH):
        raise FileNotFoundError(f"Không tìm thấy key tại: {KEY_PATH}")
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.jars", JAR_PATH) \
        .config("spark.driver.extraClassPath", JAR_PATH) \
        .config("spark.executor.extraClassPath", JAR_PATH) \
        \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
        \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        \
        .config("spark.hadoop.fs.gs.output.stream.upload.buffer.size", "6291456")
        
    return builder.getOrCreate()
            