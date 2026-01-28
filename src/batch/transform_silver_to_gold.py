import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DataType
from src.batch.utils.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

env_path = "/opt/airflow/.env"
load_dotenv(env_path)

class Config:
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    
    @classmethod
    def validate(cls):
        if not cls.BUCKET_NAME:
            raise EnvironmentError("Biến môi trường BUCKET_NAME chưa được thiết lập.")
    
    @classmethod
    def get_path(cls, layer: str, dataset: str) -> str:
        return f"gs://{cls.BUCKET_NAME}/{layer}/{dataset}/"
    
def read_data(spark: SparkSession, path: str) -> DataFrame:
    logger.info(f"Đang đọc dữ liệu từ {path}")
    return spark.read.parquet(path)

def write_data(df: DataFrame, path: str, partition_cols: list = None):
    logger.info(f"Đang ghi dữ liệu xuống {path}")
    if partition_cols:
        df = df.repartition(*partition_cols)
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    logger.info(f"Ghi thành công {path}")

def generate_dim_date(spark : SparkSession) -> DataFrame:
    """Tạo bảng Dim_Date từ ngày 2024-01-01 đến 2025-12-31."""
    df = spark.sql("""
        Select explode(sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)) as full_date
    """)
    return df.select(
        F.col("full_date").alias("date_key"),
        F.dayofmonth("full_date").alias("day"),
        F.month("full_date").alias("month"),
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "E").alias("day_name"),
        F.date_format("full_date", "MMM").alias("month_name")
        )
    
def transform_dim_products(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("product_id").alias("product_id"),
        F.col("title"),
        F.col("price"),
        F.col("original_price"),
        F.col("image_url"),
        F.col("rating_average"),
        F.col("review_count")
    )
def transform_dim_users(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("user_id"),
        F.col("name"),
        F.col("username"),
        F.col("email"),
        F.col("phone"),
        F.col("gender"),
        F.col("address"),
        F.col("job"),
        F.col("created_at")
    )
     
def transform_fact_orders(df: DataFrame) -> DataFrame:
    df_transformed = df.select(
        F.col("order_id"),
        F.col("user_id"),
        F.col("product_id"),
        F.col("order_month_part").alias("date_key"),
        F.col("order_time").alias("order_time"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("total_amount"),
        F.col("status")
    )
    df_transformed = df_transformed.withColumn("partition_month", F.date_format(F.col("date_key"), "yyyy-MM"))
    return df_transformed

def run_silver_to_gold(spark: SparkSession):
    logger.info("=== Bắt đầu tầng SILVER -> GOLD ===")
    # --- Task 1: Dim Date (Generate -> Write) ---
    try:
        dest = Config.get_path("gold", "dim_date")
        df_date = generate_dim_date(spark)
        write_data(df_date, dest)
    except Exception as e:
        logger.error(f"Lỗi trong quá trình tạo Dim_Date: {e}")
        raise e
    
    # --- Task 2: Dim Products (Silver Products -> Gold Dim Products) ---
    try:
        source = Config.get_path("silver", "products")
        dest = Config.get_path("gold", "dim_products")
        df = read_data(spark, source)
        df_dim = transform_dim_products(df)
        write_data(df_dim, dest)
    except Exception as e:
        logger.error(f"Lỗi trong quá trình ETL Dim_Products: {e}")
        raise e
    
    # --- Task 3: Dim Users (Silver Users -> Gold Dim Users) ---
    try:
        source = Config.get_path("silver", "users")
        dest = Config.get_path("gold", "dim_users")
        df = read_data(spark, source)
        df_dim = transform_dim_users(df)
        write_data(df_dim, dest)
    except Exception as e:
        logger.error(f"Lỗi trong quá trình ETL Dim_Users: {e}")
        raise e
    
    # --- Task 4: Fact Orders (Silver Orders -> Gold Fact Orders) ---
    try:
        source = Config.get_path("silver", "orders")
        dest = Config.get_path("gold", "fact_orders")
        df = read_data(spark, source)
        df_fact = transform_fact_orders(df)
        write_data(df_fact, dest, partition_cols=["partition_month"])
    except Exception as e:
        logger.error(f"Lỗi trong quá trình ETL Fact_Orders: {e}")
        raise e
    logger.info("Hoàn thành Silver -> Gold")
# if __name__ == "__main__":
#     try:
#         Config.validate()
#     except EnvironmentError as e:
#         logger.error(e)
#         exit(1)

#     spark = get_spark("Silver_To_Gold_Transformation")
    
#     try:
#         run_etl(spark)
#         logger.info("Hoàn thành ETL Silver -> Gold")
#     finally:
#         spark.stop()