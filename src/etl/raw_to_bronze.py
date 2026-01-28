import sys
import os
import logging
from typing import List, Dict, Any
from dotenv import load_dotenv

from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from src.batch.utils.spark_session import get_spark
from pyspark.sql.utils import AnalysisException

env_path = "/opt/airflow/.env"
load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

class Config:
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    
    @classmethod
    def validate(cls):
        if not cls.BUCKET_NAME:
            raise EnvironmentError("Biến môi trường chưa được thiết lập trong file .env")
        
    @classmethod
    def get_path(cls, path_type: str, sub_path: str) -> str:
        return f"gs://{cls.BUCKET_NAME}/{path_type}/{sub_path}"

TABLE_CONFIGS = [
    {
        "name": "order_events",
        "input_path": Config.get_path("raw", "order_raw/*/*.jsonl"),
        "output_path": Config.get_path("bronze", "order_events/"),
        "is_multiline": False
    },
    {
        "name": "users",
        "input_path": Config.get_path("raw", "users/*/*.json"),
        "output_path": Config.get_path("bronze", "users/"),
        "is_multiline": False
    },
    {
        "name": "products",
        "input_path": Config.get_path("raw", "products/*/*.json"),
        "output_path": Config.get_path("bronze", "products/"),
        "is_multiline": False
    }  
]

def process_single_table(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Hàm xử lý logic đọc -> biến đổi -> ghi cho một bảng
    """
    table_name = config["name"]
    input_path = config["input_path"]
    output_path = config["output_path"]
    is_multiline = config["is_multiline"]

    logger.info(f"Bắt đầu xử lý bảng: {table_name}")
    logger.info(f"Input: {input_path} | Multiline: {is_multiline}")

    try:
        reader = spark.read.option("recursiveFileLookup", "true")
        
        if is_multiline:
            reader = reader.option("multiline", "true")
            
        df = reader.json(input_path)
        
        if df.rdd.isEmpty():
            logger.warning(f"   ⚠️ Cảnh báo: Không có dữ liệu tại {input_path}")
            return

        df_transformed = df.withColumn("ingest_time", F.current_timestamp()) \
                           .withColumn("source_file", F.input_file_name()) \
                           .withColumn("data_source", F.lit(table_name))

        df_transformed.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)
            
        logger.info(f"Đã ghi xong: {output_path}")
        
    except AnalysisException as ae:
        logger.error(f"Lỗi Spark Analytics khi xử lý {table_name} (thường do sai đường dẫn hoặc format) {ae}")
    except Exception as e:
        logger.error(f"Lỗi không xác định khi xử lý {table_name}: {e}")

def run_raw_to_bronze(spark):
    logger.info("=== Bắt đầu tầng RAW -> BRONZE ===")
    
    try:
        for config in TABLE_CONFIGS:
            process_single_table(spark, config)   
        logger.info("Hoàn thành Raw -> Bronze.")
    except Exception as e:
        logger.error(f"lỗi tầng Raw-Bronze: {e}")
        raise e
