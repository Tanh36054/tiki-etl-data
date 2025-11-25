from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os
import json
import datetime
from datetime import timezone

sys.path.insert(0, "/opt/airflow")

try:
    from src.ingest.crawl_products import fetch_tiki_search, parse_tiki_api_data
    from google.cloud import storage
except Exception as e:
    print(f"Lỗi import: {e}. Hãy kiểm tra lại cấu trúc thư mục.")
    
#cau hinh chung
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

def run_crawler(**kwargs):
    """
    Task 1: crawl dữ liệu -> lưu xuống file local -> return đường dẫn file
    """
    keyword = "tai nghe bluetooth"
    
    # 1.Gọi API & Parse
    print(f"Bắt đầu crawl dữ liệu...")
    json_data = fetch_tiki_search(keyword, page=1)
    items = parse_tiki_api_data(json_data)
    
    if not items:
        raise ValueError("Không có sản phẩm nào được crawl.")
    
    # 2.Lưu file xuống thư mục tạm (/tmp) của worker
    # Dùng timestamp để file không bị trùng
    now = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    filename = f"tiki_raw_{now}.json"
    file_path = f"/tmp/{filename}"
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
        
    print(f"Đã lưu file tạm tại: {file_path}")
    
    # 3. Đẩy đường dẫn file sang Task tiếp theo bằng XCom
    # Key mặc định là "return_value"
    return file_path

def upload_to_gcs(**kwargs):
    """
    Task 2: lấy file từ Task 1 -> upload lên GCS
    """
    
    # 1.Lấy đường dẫn file từ Task 1 thông qua XCom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids = 'crawl_products')
    
    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"Không tìm thấy file tại: {file_path}")
    
    print(f"Nhận được file cần upload: {file_path}")
    
    #2.Cấu hình GCS(lấy từ biến môi trường của Docker)
    bucket_name = os.environ.get('GCS_BUCKET')
    if not bucket_name:
        raise ValueError("Biến môi trường GCS_BUCKET chưa được thiết lập.")
    
    #tạo đường dãn đích trên gcs
    filename = os.path.basename(file_path)
    now_folder = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    destination_blob_name = f"raw/products/tiki/{now_folder}/{filename}"
    
    #3.Upload file lên GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    
    print(f"Upload thành công lên GCS: gs://{bucket_name}/{destination_blob_name}")
    
with DAG(
    dag_id = 'crawl_products_dag',
    default_args = default_args,
    description = 'ETL Tiki: Crawl -> GCS',
    schedule_interval = '@daily',
    catchup = False,
    tags=['ecommerce', 'tiki']
) as dag:
    
    t1 = PythonOperator(
        task_id = 'crawl_products',
        python_callable = run_crawler,
        provide_context = True
    )
    
    t2 = PythonOperator(
        task_id = 'upload_to_gcs',
        python_callable = upload_to_gcs,
        provide_context = True
    )
    
    t1 >> t2