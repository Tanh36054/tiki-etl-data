import sys
import logging
from src.batch.utils.spark_session import get_spark

from src.etl.raw_to_bronze import run_raw_to_bronze, Config as RawConfig
from src.batch.transform_bronze_to_silver import run_bronze_to_silver, Config as SilverConfig
from src.batch.transform_silver_to_gold import run_silver_to_gold, Config as GoldConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        RawConfig.validate()
        SilverConfig.validate()
        GoldConfig.validate()
    except Exception as e:
        logger.critical(f"Lỗi biến môi trường: {e}")
        sys.exit(1)
        
    spark = get_spark("Ecommerce_Full_ETL_Pipeline")
    
    try:
        run_raw_to_bronze(spark)
        run_bronze_to_silver(spark)
        run_silver_to_gold(spark)
        logger.info("Hoàn thành toàn bộ pipeline (Raw -> Gold)")
    except Exception as e:
        logger.critical(f"Pipeline dừng đột ngột: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Đã tắt Spark Session")
        
if __name__ == "__main__":
    main()

        
            