# src/ingest/generate_users.py

import json
import os
import datetime
import random
from google.cloud import storage
from faker import Faker

GCS_BUCKET = os.environ.get('GCS_BUCKET')
RAW_PREFIX = "raw/users"
NUM_USERS = 1000  # Số lượng người dùng giả lập cần tạo

fake = Faker('vi_VN')

def generate_fake_users(limit):
    """
    Gia lap du lieu user master data
    """
    print(f" Đang giả lập {limit} người dùng...")
    users = []
    
    for i in range(1, limit + 1):
        profile = fake.profile()
        
        user = {
            "user_id": str(i),
            "name": profile['username'],
            "username": profile['username'],
            "email": profile['mail'],
            "phone": fake.phone_number(),
            "gender": profile['sex'],
            "address": profile['address'],
            "job": profile['job'],
            "created_at": fake.date_time_between(start_date='-2y', end_date='now').isoformat()
        }
        users.append(user)
        
    print(f"Đã tạo xong {len(users)} người dùng giả lập.")
    return users

def upload_to_gcs(data):
    """
    Luu file json xuong local roi upload len gcs raw zone
    """
    #1.Tao ten file theo timestamp
    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    filename = f"users_master_{now_str}.json"
    local_path = f"/tmp/{filename}"
    
    #2.Ghi file tam o local
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Đã lưu file tạm tại: {local_path}")
    
    #3.Upload len GCS
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        
        dest_path = datetime.datetime.now().strftime("%Y-%m-%d")
        dest = f"{RAW_PREFIX}/{dest_path}/{filename}"
        
        blob = bucket.blob(dest)
        blob.upload_from_filename(local_path)
        
        print(f"Upload thành công: gs://{GCS_BUCKET}/{dest}")
        
        os.remove(local_path)
        
    except Exception as e:
        print(f"Lỗi upload GCS: {e}")
        
def main():
    try:
        users_data = generate_fake_users(NUM_USERS)
        
        if users_data:
            upload_to_gcs(users_data)
        else:
            print("Không tạo được dữ liệu")
    
    except Exception as e:
        print(f"Lỗi nghiêm trọng: {e}")
    
if __name__ == "__main__":
    main()
        
        
    