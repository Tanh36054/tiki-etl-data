# src/ingest/crawl_products.py
import requests
from bs4 import BeautifulSoup
import json, time, os, datetime
from google.cloud import storage
from datetime import timezone


#config
VENDOR = "tiki" 
GCS_BUCKET = os.environ.get('GCS_BUCKET')
RAW_PREFIX = "raw/products"

def fetch_tiki_search(keyword, page = 1):
    # lightweight search page crawl (example)
    url = "https://tiki.vn/api/v2/products"
    params = {
        "q": keyword,
        "limit": 40, #số lượng sản phẩm mỗi trang
        "page": page,
        "include": "advertisement",
        "aggregations": 2
    }
    
    # giả lập header giống trình duyệt thật để tránh bị chặn
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://tiki.vn/"
    }
    
    print(f"Fetching API: {url} with keyword= '{keyword}'...")
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    
    return r.json()

def parse_tiki_api_data(json_data):
    items = []
    
    products = json_data.get("data", [])
    
    if not products:
        print("Cảnh báo: không tìm thấy sản phẩm nào trong phản hồi API")
        return []
    
    for p in products:
        item = {
            "vendor": "tiki",
            "vendor_product_id": str(p.get("id")),
            "title": p.get("name"),
            "price": p.get("price"),
            "original_price": p.get("original_price"), # Lấy thêm giá gốc
            "rating_average": p.get("rating_average"), # Lấy thêm đánh giá
            "review_count": p.get("review_count"),
            "image_url": p.get("thumbnail_url")
        }
        items.append(item)
        
    print(f"Đã lấy được {len(items)} sản phẩm.")
    return items
def upload_to_gcs(data, vendor):
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    filename = f"{vendor}_{now}.json"
    local_path = f"/tmp/{filename}"
    
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2) # indent=2 de file de doc hon
        
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    dest = f"{RAW_PREFIX}/{vendor}/{now}/{filename}"
    blod = bucket.blob(dest)
    blod.upload_from_filename(local_path)
    print("Upload thành công: gs://{}/{}".format(GCS_BUCKET, dest))
    
# def main():
#     keyword = "mỹ phẩm nữ"
#     try:
#         #1. goi API
#         json_data = fetch_tiki_search(keyword, page=1)
        
#         #2. parse Json
#         items = parse_tiki_api_data(json_data)
        
#         #3. upload neu co du lieu
#         if items:
#             upload_to_gcs(items, "tiki")
#         else:
#             print("Không có dữ liệu để upload")
#     except Exception as e:
#         print("Lỗi nghiêm trọng: {e}")
    
# if __name__ == "__main__":
#     main()