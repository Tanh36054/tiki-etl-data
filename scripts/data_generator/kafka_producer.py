# scripts/data_generator/kafka_producer.py
import os
import json
import time
import logging
from kafka import KafkaProducer
from faker import Faker
import random

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__) 
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
TOPIC_NAME = 'order_raw'
PRODUCT_FILE_PATH = 'data/tiki_raw_products.json'
fake = Faker('vi_VN')

DUMMY_PRODUCTS = [
  {"vendor": "tiki", "vendor_product_id": "52424296", "title": "Áo thun nam nữ 80% Cotton White07", "price": 169000, "original_price": 169000, "rating_average": 0, "review_count": 0, "image_url": "https://salt.tikicdn.com/cache/280x280/ts/product/bb/b2/e6/04d2bb78605c844964c46c4d8ea545ae.png"},
  {"vendor": "tiki", "vendor_product_id": "52423759", "title": "Áo thun nam nữ 80% Cotton Pink13", "price": 135200, "original_price": 169000, "rating_average": 0, "review_count": 0, "image_url": "https://salt.tikicdn.com/cache/280x280/ts/product/7d/65/9d/7fc3ccb00fb86d3740a302880e3b0028.png"}
]

def parse_price(price_raw):
    if isinstance(price_raw, (int, float)):
        return int(price_raw)
    
    if isinstance(price_raw, str):
        clean_str = price_raw.replace('.', '').replace(' ₫', '').strip()
        if clean_str.isdigit():
            return int(clean_str)
    return 0

def load_products():
    valid_products = []
    
    if os.path.exists(PRODUCT_FILE_PATH):
        logger.info(f"Đang đọc sản dữ liệu sản phẩm từ: {PRODUCT_FILE_PATH}")
        try:
            with open(PRODUCT_FILE_PATH, 'r', encoding='utf-8') as f:
                
                for line in f:
                    line = line.strip()
                    if not line: continue # Bỏ qua dòng trống
                    
                    try:
                        p = json.loads(line) # Parse từng dòng
                        
                        price = parse_price(p.get('price'))
                        pid = str(p.get("vendor_product_id") or p.get("id"))
                        title = p.get("title")
                        
                        if price > 0 and pid and title:
                            valid_products.append({
                                "id": pid,
                                "price": price,
                                "title": title
                            })
                    except json.JSONDecodeError:
                        continue # Bỏ qua dòng lỗi
        except Exception as e:
            logger.error(f"Lỗi khi đọc file: {e}. Sẽ sử dụng sản phẩm mẫu")
    
    if not valid_products:
        logger.warning("Không tìm thấy file hoặc file rỗng, dùng dữ liệu DUMMY")
        for p in DUMMY_PRODUCTS:
            valid_products.append({
                "id": str(p.get("vendor_product_id")),
                "price": int(p.get("price")),
                "title": p.get("title")
            })
    logger.info(f"Đã load thành công {len(valid_products)} sản phẩm để bán")
    return valid_products

def generate_order(products_list):
    statues = ['completed', 'shipped', 'processing', 'canceled']
    weights = [70, 15, 10, 5]
    
    user_id = str(random.randint(1, 1000))
    product = random.choice(products_list)
    
    quantity = random.randint(1, 5)
    unit_price = product['price']
    total_amount = unit_price * quantity
    
    order_time_ojb = fake.date_time_between(start_date='-1y', end_date='now')
    
    return{
        "order_id": fake.uuid4(),
        "user_id": user_id,
        "product_id": product['id'],
        "product_name": product['title'],
        "order_date": order_time_ojb.isoformat(),
        "status": random.choices(statues, weights = weights, k=1)[0],
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
    }
    
def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Không thể kết nối kafka: {e}")
        return
    
    products_cache = load_products()
    logger.info("Đang gửi đơn ảo đến Kafka...")
    try:
        while True:
            order = generate_order(products_cache)
            producer.send(TOPIC_NAME, order)

            logger.info(f"[Đơn {order['order_id'][:6]}] User {order['user_id']} - {order['total_amount']:,}đ")
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        logger.info("Dừng gửi đơn hàng.")
        producer.close()
if __name__ == "__main__":
    main()