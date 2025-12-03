# src/batch/transform_bronze_to_silver.py

from pyspark.sql import functions as F
from src.batch.utils.spark_session import get_spark
from pyspark.sql.types import DoubleType, IntegerType, StringType

BUCKET = "my-ecommerce-dev-bucket"

BRONZE_PRODUCTS = f"gs://{BUCKET}/bronze/products/"
BRONZE_USERS = f"gs://{BUCKET}/bronze/users/"
BRONZE_ORDERS = f"gs://{BUCKET}/bronze/order_events/"

SILVER_PRODUCTS = f"gs://{BUCKET}/silver/products/"
SILVER_USERS = f"gs://{BUCKET}/silver/users/"
SILVER_ORDERS = f"gs://{BUCKET}/silver/orders/"

def process_products(spark):
    print("processing products...")
    try:
        df = spark.read.parquet(BRONZE_PRODUCTS)
        
        df_silver = df.select(
            F.col("vendor_product_id").alias("product_id"),
            F.col("title"),
            F.col("price").cast(DoubleType()),
            F.col("original_price").cast(DoubleType()),
            F.col("rating_average").cast(DoubleType()),
            F.col("review_count").cast(DoubleType()),
            F.col("vendor"),
            F.col("image_url"),
            F.lit("Unknown").alias("category")
        )
        
        df_silver = df_silver.filter(F.col("product_id").isNotNull())
        df_silver = df_silver.dropDuplicates(["product_id"])
        df_silver.write.mode("overwrite").parquet(SILVER_PRODUCTS)
        print(f"Đã ghi xong products: {SILVER_PRODUCTS}")
        
    except Exception as e:
        print(f"Lỗi xử lý products: {e}")
    
def process_users(spark):
    print("processing users....")
    try:
        df = spark.read.parquet(BRONZE_USERS)
        
        df_silver = df.select(
            F.col("user_id").cast(StringType()),
            F.col("username"),
            F.col("name"),
            F.lower(F.col("email")).alias("email"),
            F.col("phone"),
            F.col("address"),
            F.col("gender"),
            F.col("job"),
            F.to_timestamp(F.col("created_at")).alias("created_at")
        )
        
        df_silver = df_silver.dropDuplicates(["user_id"])
        df_silver.write.mode("overwrite").parquet(SILVER_USERS)
        print(f"Đã ghi xong users: {SILVER_USERS}")
        
    except Exception as e:
        print(f"Lỗi xử lý Users: {e}")
        
def process_orders(spark):
    print("processing orders...")
    try:
        df = spark.read.parquet(BRONZE_ORDERS)
        
        df_silver = df.select(
            F.col("order_id").cast(StringType()),
            F.col("user_id").cast(StringType()),
            F.col("product_id").cast(StringType()),
            F.col("price").cast(StringType()),
            F.col("quantity").cast(IntegerType()),
            F.to_timestamp(F.col("created_at")).alias("order_time")
        )
    
        df_silver = df_silver.filter(F.col("order_id").isNotNull())
        df_silver = df_silver.dropDuplicates(["order_id"])
        df_silver = df_silver.withColumn("order_date", F.to_date(F.col("order_time")))
        df_silver.write.mode("overwrite").partitionBy("order_date").parquet(SILVER_ORDERS)
        print(f"Đã ghi xong: {SILVER_ORDERS}")
        
    except Exception as e:
        print(f"Lỗi xử lý Orders: {e}")
        
if __name__ == "__main__":
    spark = get_spark("Bronze_To_Silver_Tranfomation")
    
    process_products(spark)
    process_users(spark)
    process_orders(spark)
    
    print("Hoàn thành ETL Bronze -> Silver")
    spark.stop()