# src/batch/transform_bronze_to_silver.py

import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.batch.utils.spark_session import get_spark
from pyspark.sql.types import DoubleType, StringType, LongType, IntegerType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

env_path = "/opt/airflow/.env"
load_dotenv(env_path)
class Config:
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    
    @classmethod
    def validate(cls):
        if not cls.BUCKET_NAME:
            raise EnvironmentError("Biến môi trường BUCKET_NAME chưa được thiết lập trong file .env")

    @classmethod
    def get_path(cls, layer: str, dataset:str) -> str:
        return f"gs://{cls.BUCKET_NAME}/{layer}/{dataset}/"


  
def read_data(spark: SparkSession, path: str) -> DataFrame:
    logger.info(f"Đang đọc dữ liệu từ: {path}")
    return spark.read.parquet(path)

def write_data(df: DataFrame, path: str, partition_cols: list = None):
    logger.info(f"Đang ghi dữ liệu xuống: {path}")
    if partition_cols:
        df = df.repartition(*partition_cols)
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    logger.info(f"Ghi thành công: {path}") 
    
def transform_products(df: DataFrame) -> DataFrame:
    df_transformed = df.select(
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
    return (df_transformed
            .filter(F.col("product_id").isNotNull())
            .dropDuplicates(["product_id"]))
    
def transform_users(df: DataFrame) -> DataFrame:
    df_transformed = df.select(
        F.col("user_id").cast(StringType()),
        F.col("username"),
        F.col("name"),
        F.lower(F.col("email")).alias("email"),
        F.col("phone"),
        F.col("address"),
        F.col("gender"),
        F.col("job"),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
        F.to_timestamp(F.col("updated_at")).alias("updated_at")
    )
    return df_transformed.dropDuplicates(["user_id"])    
    
def transform_orders(df: DataFrame) -> DataFrame:    
    df_transformed = df.select(
        F.col("order_id").cast(StringType()),
        F.col("user_id").cast(StringType()),
        F.col("product_id").cast(StringType()),
        F.col("unit_price").cast(LongType()),
        F.col("total_amount").cast(LongType()),
        F.col("quantity").cast(IntegerType()),
        F.col("status").cast(StringType()),
        F.to_timestamp(F.col("order_date")).alias("order_time")
    )
    df_transformed = df_transformed.filter(
        (F.col("order_id").isNotNull()) &
        (F.year(F.col("order_time")) >= 2024)
    )
    return (df_transformed
            .dropDuplicates(["order_id"])
            .withColumn("order_month_part", F.date_format(F.col("order_time"), "yyyy-MM")))

def run_bronze_to_silver(spark: SparkSession):
    logger.info("=== Bắt đầu tầng BRONZE -> SILVER ===")
    try: 
        source = Config.get_path("bronze", "products")
        dest = Config.get_path("silver", "products")
        
        df = read_data(spark, source)
        df_clean = transform_products(df)
        write_data(df_clean, dest)
    except Exception as e:
        logger.error(f"Lỗi trong quá trình ETL products: {e}")
        raise e
    
    try: 
        source = Config.get_path("bronze", "users")
        dest = Config.get_path("silver", "users")
        
        df = read_data(spark, source)
        df_clean = transform_users(df)
        write_data(df_clean, dest)
    except Exception as e:
        logger.error(f"Lỗi trong quá trình ETL users: {e}")
        raise e
    
    try:
        source = Config.get_path("bronze", "order_events")
        dest = Config.get_path("silver", "orders")
        
        df = read_data(spark, source)
        df_clean = transform_orders(df)
        write_data(df_clean, dest, partition_cols=["order_month_part"])
    except Exception as e:
        logger.error(f"Lỗi trong quá trình ETL orders: {e}")
        raise e
    logger.info("Hoàn thành Bronze -> Silver")
# if __name__ == "__main__":
#     try:
#         Config.validate()
#     except EnvironmentError as e:
#         logger.error(e)
#         exit(1)
        
#     spark = get_spark("Bronze_To_Silver_Tranfomation")
    
#     try:
#         run_etl(spark)
#         logger.info("Hoàn thành ETL Bronze -> Siler")
#     finally:
#         spark.stop()