import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DataType

BUCKET = "my-ecommerce-dev-bucket"
KEY_PATH = "/home/tanh/.gcp_keys/ecommerce-sa.json"

SILVER_PRODUCTS = f"gs://{BUCKET}/silver/products/"
SILVER_USERS = f"gs://{BUCKET}/silver/users/"
SILVER_ORDERS = f"gs://{BUCKET}/silver/orders/"

GOLD_DIM_PRODUCTS = f"gs://{BUCKET}/gold/dim_products/"
GOLD_DIM_USERS = f"gs://{BUCKET}/gold/dim_users/"
GOLD_DIM_DATE = f"gs://{BUCKET}/gold/dim_date/"
GOLD_FACT_ORDERS = f"gs://{BUCKET}/gold/fact_orders/"

def create_spark_session():
    spark = SparkSession.builder \
        .appName("ETL_Silver_To_Gold_StarSchema") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()
    return spark

def create_dim_date(spark):
    print("Creating dim_date...")
    try:
        df = spark.sql("""
            Select explode(sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)) as full_date
                       """)
        
        dim_date = df.select(
            F.col("full_date").alias("date_key"),
            F.dayofmonth("full_date").alias("day"),
            F.month("full_date").alias("month"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "E").alias("day_name"),
            F.date_format("full_date", "MMM").alias("month_name")
        )
    
        dim_date.write.mode("overwrite").parquet(GOLD_DIM_DATE)
        print(f"Xong dim_date: {GOLD_DIM_DATE}")
    except Exception as e:
        print(f"Lỗi dim_date: {e}")
        
def create_dim_products(spark):
    print(f"Creating Dim_Products...")
    try:
        df = spark.read.parquet(SILVER_PRODUCTS)
        
        dim_products = df.select(
            "product_id", "title", "price", "image_url", "category", "rating_average"
        )
        
        dim_products.write.mode("overwrite").parquet(GOLD_DIM_PRODUCTS)
        print(f"Xong dim_products : {GOLD_DIM_PRODUCTS}")
    except Exception as e:
        print(f"Lỗi Dim_products: {e}")

def create_dim_users(spark):
    print("Creating dim_users...")
    try:
        df = spark.read.parquet(SILVER_USERS)
        
        dim_users = df.select(
            "user_id", "name", "username", "email", "phone", "gender", "address", "job", "created_at"
        )
        
        dim_users.write.mode("overwrite").parquet(GOLD_DIM_USERS)
        print(f"Xong dim_users: {GOLD_DIM_USERS}")
        
    except Exception as e:
        print(f"Lỗi dim_users: {e}")
     
def create_fact_orders(spark):
    print("Creating Fact_orders...")
    try:
        df = spark.read.parquet(SILVER_ORDERS)
        
        fact_orders = df.select(
            "order_id",
            "user_id",
            "product_id",
            F.col("order_date").alias("date_key"),
            "order_time",
            "quantity",
            "price",
            (F.col("price") * F.col("quantity")).alias("total_amount")   
        )
        
        fact_orders.write.mode("overwrite").parquet(GOLD_FACT_ORDERS)
        print(f"Xong Fact_Orders: {GOLD_FACT_ORDERS}")
    except Exception as e:
        print(f"Lỗi Fact_Orders: {e}")
        
def main():
    if not os.path.exists(KEY_PATH):
        print(f"Không tìm thấy key tại: {KEY_PATH}")
        sys.exit(1)
        
    spark = create_spark_session()
    
    create_dim_date(spark)
    create_dim_products(spark)
    create_dim_users(spark)
    
    create_fact_orders(spark)
    
    print("Hoàn thành ETL silver -> gold")
    spark.stop()
    
if __name__ == "__main__":
    main()