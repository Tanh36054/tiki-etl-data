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
    url = "https://tiki.vn/api/v2/products"
    params = {
        "q": keyword,
        "limit": 40,
        "page": page,
        "include": "advertisement",
        "aggregations": 2
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://tiki.vn/"
    }
    
    print(f"Fetching API: {url} with keyword= '{keyword}'...")
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    
    return r.json()

def clean_price_value(raw_value):
    try:
        if isinstance(raw_value, (int, float)):
            return int(raw_value)
        if isinstance(raw_value, str):
            clean_str = raw_value.replace('.', '').replace(',', '').replace('₫', '').replace('d', '').strip()
            if clean_str.isdigit():
                return int(clean_str)
        return 0
    except Exception:
        return 0

def parse_tiki_api_data(json_data):
    items = []
    
    products = json_data.get("data", [])
    
    if not products:
        print("Cảnh báo: không tìm thấy sản phẩm nào trong phản hồi API")
        return []
    
    for p in products:
        price = clean_price_value(p.get("price"))
        original_price = clean_price_value(p.get("original_price"))
        
        if price > 0:
            item = {
                "vendor": "tiki",
                "vendor_product_id": str(p.get("id")),
                "title": p.get("name"),
                "price": price,
                "original_price": original_price,
                "rating_average": p.get("rating_average", 0), 
                "review_count": p.get("review_count", 0),
                "image_url": p.get("thumbnail_url")
            }
            items.append(item)
            
    print(f"Đã lấy được {len(items)} sản phẩm.")
    return items
