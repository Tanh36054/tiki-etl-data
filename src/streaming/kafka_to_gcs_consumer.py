from kafka import KafkaConsumer
from google.cloud import storage
import json
import datetime
import os
import time

KEY_PATH = "/home/tanh/.gcp_keys/ecommerce-sa.json"

if not os.path.exists(KEY_PATH):
    print(f"Không tìm thấy file key tại {KEY_PATH}")
    exit(1)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

BUCKET = 'my-ecommerce-dev-bucket'

def upload_batch_to_gcs(bucket_name, topic, batch_data):
    """
    Upload mot danh sach cac tin nhan len gcs thanh 1 file moi
    """
    if not batch_data:
        return
    
    #tao ten file duy nhat theo thoi gian thuc de khong bi ghi de
    date_str = datetime.date.today().strftime('%Y-%m-%d')
    time_str = datetime.datetime.now().strftime('%H-%M-%S-%f')
    path = f"raw/{topic}/{date_str}/{time_str}.jsonl"
    
    content = "\n".join([json.dumps(msg) for msg in batch_data])
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    
    blob.upload_from_string(content, content_type='text/plain')
    print(f"Đã upload batch {len(batch_data)} dòng lên: {path}")

def run_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        
        group_id = 'gcs-loader-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Đang lắng nghe topic: {topic}...")
    
    batch_buffer = []
    BATCH_SIZE = 20  
    
    try:
        for msg in consumer:
            data = msg.value
            batch_buffer.append(data)
            
            print(f"Nhận tin nhắn: {len(batch_buffer)}/{BATCH_SIZE}")
            
            if len(batch_buffer) >= BATCH_SIZE:
                upload_batch_to_gcs(BUCKET, topic, batch_buffer)
                batch_buffer = [] #reset bo nho dem
    except KeyboardInterrupt:
        print("Đang dừng... Upload nốt dữ liệu còn lại")
        upload_batch_to_gcs(BUCKET, topic, batch_buffer)

        
if __name__ == "__main__":
    run_consumer("order_raw")