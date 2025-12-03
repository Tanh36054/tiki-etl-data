Data Platform End-to-End: Batch + Streaming + Data Lake + Data Warehouse (GCS + PySpark + Kafka + Airflow + BigQuery)
Vị trí hướng tới: Cloud/Data Engineer Intern
Tech stack: GCP, Airflow, Kafka, Spark, Python, BigQuery, Docker

🚀 1. Giới thiệu dự án
Dự án này mô phỏng một Data Platform chuẩn doanh nghiệp, xử lý cả batch lẫn streaming dữ liệu theo mô hình Medallion Architecture (Raw → Bronze → Silver → Gold).

Nền tảng sử dụng:

| Thành phần            | Công nghệ                         |
|-----------------------|-----------------------------------|
| Data Ingestion        | Python, Airflow, Kafka            |
| Data Lake             | Google Cloud Storage (GCS)        |
| Batch Processing      | PySpark                           |
| Streaming Processing  | Kafka Consumer → GCS              |
| Data Warehouse        | BigQuery                          |
| Orchestration         | Airflow                           |
| Deployment            | Docker Compose                    |
|-----------------------|-----------------------------------|
Hệ thống được thiết kế để:
- Thu thập dữ liệusản phẩm/giá từ nhiều nguồn(batch & streaming)
- Xây dựng Data Lake với các tầng:
  - Raw  
  - Bronze  
  - Silver  
  - Gold
- Làm sạch và chuẩn hóa dữ liệu bằng PySpark
- Load dữ liệu vào BigQuery theo mô hình Star Schema:
  - Fact Tables  
  - Dimension Tables
- Tối ưu hóa truy vấn bằng:
  - Partitioning  
  - Clustering
- Viết báo cáo phân tích thử nghiệm bằng SQL

🧱 2. Kiến trúc tổng quan
                ┌───────────────────────────────┐
                │  External Sources / API / Web │
                └───────────────┬───────────────┘
                                │
                        (Batch Crawling)
                                ▼
                    ┌───────────────────┐
                    │ Airflow DAGs      │
                    └─────────┬─────────┘
                              │
                          Raw Zone
                              │
                     Bronze Transformation
                              │
                ┌─────────────▼──────────────┐
                │       GCS Data Lake         │
                │  raw / bronze / silver / gold
                └─────────────┬──────────────┘
                              │
                       PySpark (Batch)
                   Cleaning / Standardizing
                              │
                         Silver Zone
                              │
                       Gold Aggregations
                              │
                          BigQuery DWH
                      Fact + Dimensions

🗂 3. Cấu trúc thư mục(dự kiến khi hoàn thành)
TIKI-ETL-DATA/
│
├── configs/                      # Config chung (API keys, constants…)
│
├── dags/                         # Airflow DAGs (tuần 6 thêm nhiều)
│   └── crawl_products_dag.py
│
├── deployments/
│   ├── docker/
│   │   ├── dags/                 # Mount DAGs vào Airflow Docker
│   │   ├── data/                 # File tạm trong local container
│   │   ├── logs/                 # Log Airflow
│   │   ├── scripts/              # Script entrypoint cho Airflow
│   │   ├── src/                  # Source chạy trong Docker
│   │   ├── Dockerfile
│   │   └── docker-compose.yml    # Airflow multi-container
│   │
│   └── kafka/
│       ├── data/                 # Kafka & Zookeeper storage
│       └── docker-compose.yml    # Kafka cluster
│
├── docs/
│   └── README.md                 # Document chính (viết portfolio)
│
├── infra/
│   └── terraform/                # Tuần 8: IaC cho GCP
│
├── logs/                         # Log chạy local (kafka/spark/scripts)
│
├── notebooks/                    # EDA, test data, thử Spark
│
├── plugins/                      # Airflow plugins (DB hook, operators…)
│
├── scripts/
│   ├── data_generator/
│   │   └── kafka_producer.py     # Fake event → Kafka
│   ├── utils/
│   └── pg_seed.py                # Fake PostgreSQL seed
│
├── src/
│   ├── ingest/
│   │   ├── crawl_products.py
│   │   ├── generate_users.py
│   │   ├── init_schema.sql
│   │   ├── postgres_loader.py
│   │   └── price_tracker.py
│   │
│   ├── streaming/
│   │   └── kafka_to_gcs_consumer.py
│   │
│   ├── etl/
│   │   └── raw_to_bronze.py
│   │
│   └── batch/
│       ├── utils/
│       ├── check_gold_data.py
│       ├── transform_bronze_to_silver.py
│       └── transform_silver_to_gold.py
│
├── tests/                        # Unit tests (nếu thêm)
│
├── .gitignore
├── README.md
└── requirements.txt

📦 4. Cách chạy project
4.1. 🔧 Cài đặt môi trường
Yêu cầu:
- Docker + Docker Compose
- Python 3.12+
- GCP service account key(JSON)
4.2. 🔥 Khởi động hệ thống
cd deployments/docker
docker-compose up -d

cd deployments/kafka
docker-compose up -d

Dịch vụ được bật:
- Airflow Webserver (localhost:8080)
- Kafka + Zookeeper
4.3. 🌐 Thiết lập biến môi trường
python -m venv .venv

📊 5. Kết quả
- Data Lake đầy đủ Raw/Bronze/Silver/Gold
- Streaming & Batch ingestion hoạt động ổn định
- Các bảng fact/dim được xây dựng theo chuẩn Kimball:
  - dim_products  
  - dim_users
  - dim_orders
  - fact_date
- BigQuery hoạt động tối ưu với partition + clustering
- Có dashboard mẫu (nếu bạn tạo thêm Looker Studio)

🧾 7. Hướng phát triển tiếp
- Thêm Data Quality (Great Expectations)
- Thêm CI/CD cho pipelne (GitHub Actions)
- Dùng Terraform để xây dựng hạ tầng thật trên GCP 

