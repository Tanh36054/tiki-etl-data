import os
import json
import logging
import datetime
import signal
import sys
from typing import List, Dict
from dotenv import load_dotenv
from kafka import KafkaConsumer
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
load_dotenv()

class Config:
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = "order_raw"
    CONSUMER_GROUP = "gcs-loader-group"
    BATCH_SIZE = 2000
    
    @classmethod
    def validate(cls):
        if not cls.BUCKET_NAME:
            raise EnvironmentError("Chưa set biến môi trường: BUCKET_NAME")
        if not cls.GOOGLE_APPLICATION_CREDENTIALS:
            raise EnvironmentError("Chưa set biến môi trường: GOOGLE_APPLICATION_CREDENTIALS")
        if not os.path.exists(cls.GOOGLE_APPLICATION_CREDENTIALS):
            raise FileNotFoundError(f"không tìm thấy file key tại: {cls.GOOGLE_APPLICATION_CREDENTIALS}")

class KafkaGCSLoader: 
    def __init__(self):
        self.bucket_name = Config.BUCKET_NAME
        self.topic = Config.KAFKA_TOPIC
        self.buffer: List[Dict] = []
        self.running = True
        
        try:
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(self.bucket_name)
        except Exception as e:
            logger.critical(f"không thể kết nối GCS: {e}")
            raise e
        
        signal.signal(signal.SIGTERM, self.stop_handler)
        signal.signal(signal.SIGINT, self.stop_handler)
        
    def stop_handler(self, sigum, frame):
        logger.info("Đã nhận được tín hiệu dừng. Đang chuẩn bị tắt...")
        self.running=False
        
    def upload_batch(self):
        """
        Upload mot danh sach cac tin nhan len gcs thanh 1 file moi
        """
        if not self.buffer: 
            return
        
        try:
            now = datetime.datetime.now()
            date_path = now.strftime('%Y-%m-%d')
            file_name = f"{now.strftime('%H-%M-%S-%f')}.jsonl"
            blob_path = f"raw/{self.topic}/{date_path}/{file_name}"

            # Chuyển đổi list -> chuỗi NDJSON (Newline Delimited JSON)
            content = "\n".join([json.dumps(msg, ensure_ascii=False) for msg in self.buffer])

            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(content, content_type='application/json')
            
            logger.info(f"UPLOAD THÀNH CÔNG: {len(self.buffer)} records -> gs://{self.bucket_name}/{blob_path}")
            
            # Reset buffer sau khi upload thành công
            self.buffer = [] 
            
        except Exception as e:
            logger.error(f"LỖI UPLOAD GCS: {e}")

    def run(self):
        """Vòng lặp chính consumer."""
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=Config.CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        logger.info(f"Đang lắng nghe topic '{self.topic}' trên {Config.KAFKA_BOOTSTRAP_SERVERS}...")

        try:
            while self.running:
                msg_pack = consumer.poll(timeout_ms=1000)
                
                if not msg_pack:
                    continue
                
                for tp, messages in msg_pack.items():
                    for msg in messages:
                        data = msg.value
                        self.buffer.append(data)

                        # Log nhẹ mỗi 100 tin nhắn để đỡ spam terminal
                        if len(self.buffer) % 100 == 0:
                            logger.info(f"Buffer hiện tại: {len(self.buffer)}/{Config.BATCH_SIZE}")

                        # Kiểm tra điều kiện để upload
                        if len(self.buffer) >= Config.BATCH_SIZE:
                            self.upload_batch()

        except Exception as e:
            logger.error(f"Lỗi Consumer: {e}")
        finally:
            if self.buffer:
                logger.info("Đang upload nốt dữ liệu còn trong buffer...")
                self.upload_batch()
            consumer.close()
            logger.info("Đã đóng Kafka Consumer.")

        
if __name__ == "__main__":
    try:
        Config.validate()
        loader = KafkaGCSLoader()
        loader.run()
    except Exception as e:
        logger.critical(f"Chương trình dừng đột ngột {e}")
        exit(1)