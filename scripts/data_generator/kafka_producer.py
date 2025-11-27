# scripts/data_generator/kafka_producer.py
import json
import time
from kafka import KafkaProducer
from faker import Faker
import random

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_order():
    return{
        "order_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 200),
        "price": round(random.uniform(10, 500), 2),
        "quantity": random.randint(1, 5),
        "created_at": fake.iso8601()
    }
    
if __name__ == "__main__":
    print("Đang gửi đơn ảo đến Kafka...")
    while True:
        order = generate_order()
        producer.send('order_raw', order)
        print("Gửi đơn hàng:", order)
        time.sleep(2)  # Gửi 2 giây một đơn hàng