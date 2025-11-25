#  src/ingest/price_tracker.py
import requests, csv, datetime, os
from google.cloud import storage

GCS_BUCKET = os.environ.get('GCS_BUCKET')
RAW_PREFIX = "raw/price_history"

PRODUCTS = [
    {"vendor": "tiki", "vendor_product_id": "197216291", "name": "Tai nghe Bluetooth"}, 
    {"vendor": "tiki", "vendor_product_id": "118386686", "name": "Chuột Logitech"},
]

def get_real_price_from_api(product_id):
    # dung api chi tiet san pham cua tiki
    url = f"https://tiki.vn/api/v2/products/{product_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://tiki.vn/"
    }
    
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        
        current_price = data.get("price")
        print(f"Sản phẩm {product_id}: {current_price} VND")
        return current_price
    
    except Exception as e:
        print(f"Lỗi lấy giá ID {product_id}: {e}")
        return None

def append_local_csv(rows, filename):
    header = ["timestamp", "vendor", "vendor_product_id", "price"]
    write_header = not os.path.exists(filename)
    
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerows(rows)
        
def upload_file(local_path, vendor):
    if not os.path.exists(local_path):
        print("File không tồn tại để upload.")
        return
    
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    
    #tao duong dan luu tru theo thoi gian
    dest = f"{RAW_PREFIX}/{vendor}/{now}/{os.path.basename(local_path)}"
    blod = bucket.blob(dest)
    
    blod.upload_from_filename(local_path)
    print(f"Upload thành công: gs://{GCS_BUCKET}/{dest}")
    
def main():
    rows = []
    
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    print("--- Bắt đầu theo dõi giá ---")
    
    for p in PRODUCTS:
        price = get_real_price_from_api(p["vendor_product_id"])
        
        if price is not None:
            rows.append([ts, p["vendor"], p["vendor_product_id"], price])
            
    if rows:
        local = "/tmp/price_history_run.csv"
        #xoa file cu neu muon moi lan chay la file moi tinh
        if os.path.exists(local):
            os.remove(local)
            
        append_local_csv(rows, local)
        upload_file(local, "tiki")
    else:
        print("Không lấy được giá nào cả.")
if __name__ == "__main__":
    main()