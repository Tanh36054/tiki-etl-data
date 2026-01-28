import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",   
)
logger = logging.getLogger(__name__)
env_path = "/opt/airflow/.env"
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

class Config:
    KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    PROJECT_ID = os.getenv("PROJECT_ID")
    DATASET_ID = os.getenv("DATASET_ID")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    REGION = os.getenv("GCP_REGION", "asia-southeast1")
    
    @classmethod
    def validate(cls):
        required_vars = [
            ("GOOGLE_APPLICATION_CREDENTIALS", cls.KEY_PATH),
            ("PROJECT_ID", cls.PROJECT_ID),
            ("DATASET_ID", cls.DATASET_ID),
            ("BUCKET_NAME", cls.BUCKET_NAME)
        ]
        missing = [name for name, value in required_vars if not value]
        if missing:
            raise EnvironmentError(f"Thiếu biến môi trường sau trong file .env: {', '.join(missing)}")

try:
    Config.validate()
except EnvironmentError as e:
    logger.error(e)
    exit(1)

TABLES = {
    "dim_users": f"gs://{Config.BUCKET_NAME}/gold/dim_users/*.parquet",
    "dim_products": f"gs://{Config.BUCKET_NAME}/gold/dim_products/*.parquet",
    "dim_date": f"gs://{Config.BUCKET_NAME}/gold/dim_date/*.parquet",
    "fact_orders": f"gs://{Config.BUCKET_NAME}/gold/fact_orders/*",
}

def create_dataset_if_not_exists(client: bigquery.Client) -> None:
    dataset_id = f"{Config.PROJECT_ID}.{Config.DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = Config.REGION
    try:
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Đã kiểm tra/tạo dataset: {dataset_id}")
    except GoogleAPICallError as e:
        logger.error(f"Lỗi khi tạo dataset {dataset_id}: {e}")
        raise
    
def get_job_config(table_name: str) -> bigquery.LoadJobConfig:
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    
    if table_name == "fact_orders":
        config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="order_time"
        )
    return config

def load_table(client: bigquery.Client, table_name: str, gcs_uri: str) -> None:
    table_id = f"{Config.PROJECT_ID}.{Config.DATASET_ID}.{table_name}"
    logger.info(f"\nĐang xử lý bảng: {table_name} từ {gcs_uri}")
    
    job_config = get_job_config(table_name)
    
    try:
        load_job = client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )
        load_job.result()
        
        destination_table = client.get_table(table_id)
        logger.info(f"Load thành công bảng {table_name}. Tổng số dòng {destination_table.num_rows}")
    
    except GoogleAPICallError as e:
        logger.error(f"Lỗi khi load bảng {table_name}: {e}")
        raise e
    except Exception as e:
        logger.error(f"Lỗi không xác định khi load bảng {table_name}: {e}")
        raise e
def main():
    try:
        client = bigquery.Client()
    
        create_dataset_if_not_exists(client)
    
        for table_name, uri in TABLES.items():
            load_table(client, table_name, uri)
        logger.info("Hoàn thành toàn bộ quy trình ETL Load.")
        
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng trong quá trình ETL Load: {e}")
        exit(1)
if __name__ == "__main__":
    main()