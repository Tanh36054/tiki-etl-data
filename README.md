The system is designed to:
 # TIKI-ETL-DATA — End-to-End Data Platform (Portfolio-ready)

 Short, professional README describing architecture, components, how to run and results for a Data Engineer portfolio submission.

 ## Project Overview

 This project simulates an enterprise-style data platform that handles both batch and streaming ingestion and follows the Medallion Architecture (Raw → Bronze → Silver → Gold). The platform ingests product and order data, processes it with PySpark, stores data in Google Cloud Storage (GCS) as a Data Lake, and publishes curated tables into BigQuery as a Data Warehouse.

 Key goals:
 - Demonstrate end-to-end ETL (crawl/stream → Kafka → GCS → Spark → BigQuery)
 - Implement Medallion layers with clear separation of concerns
 - Show data orchestration using Apache Airflow and local deployment with Docker Compose

 ## Tech Stack

 - Orchestration: Apache Airflow
 - Messaging: Apache Kafka (for streaming simulation)
 - Batch processing: PySpark
 - Cloud storage: Google Cloud Storage (GCS)
 - Data warehouse: Google BigQuery
 - Language & tooling: Python, Docker, Docker Compose

 ## Architecture (high level)

 See the diagram: `docs/data_flow.svg` (also available as PNG at `docs/data_flow.png` if generated).

 Flow summary:
 - Sources (web API crawler and synthetic Kafka producer) → Kafka topic(s) and direct uploads to GCS raw
 - Kafka consumer batches messages → writes newline-delimited JSON files to `gs://<BUCKET>/raw/...`
 - PySpark jobs read raw files from GCS → write Bronze parquet → transform Bronze → Silver → Gold
 - Final Gold parquet files are loaded into BigQuery tables (dimensions + facts)

 ## Repository layout (important files)

 - `dags/` — Airflow DAG definitions
   - `crawl_products_dag.py` — crawl Tiki API and upload raw JSON to GCS
   - `ecommerce_etl_pipeline.py` — orchestrates Spark/ETL steps and BigQuery load
 - `src/ingest/` — crawlers and ingestion utilities (`crawl_products.py`)
 - `scripts/data_generator/` — `kafka_producer.py` produces synthetic `order_raw` events
 - `src/streaming/` — `kafka_to_gcs_consumer.py` consumes Kafka and writes GCS raw JSONL
 - `src/etl/raw_to_bronze.py` — Raw → Bronze parquet conversion (PySpark)
 - `src/batch/transform_bronze_to_silver.py` — Bronze → Silver (typing, dedupe, partitioning)
 - `src/batch/transform_silver_to_gold.py` — Silver → Gold (dim/fact creation)
 - `src/warehouse/load_to_bq.py` — Loads gold parquet into BigQuery
 - `deployments/docker/docker-compose.yml` — Local Airflow + Postgres deployment configuration
 - `docs/data_flow.svg` — Data flow diagram (generated)

 ## Detailed data flow (quick reference)

 - Producer: `scripts/data_generator/kafka_producer.py` → Kafka topic `order_raw` (JSON messages)
 - Kafka consumer: `src/streaming/kafka_to_gcs_consumer.py` → writes `gs://<BUCKET>/raw/order_raw/YYYY/<ts>.jsonl`
 - Crawler: `dags/crawl_products_dag.py` uses `src/ingest/crawl_products.py` → `/tmp/tiki_raw_*.json` → uploaded to `gs://<BUCKET>/raw/products/...`
 - Raw → Bronze: `src/etl/raw_to_bronze.py` reads raw JSON/JSONL → writes parquet to `gs://<BUCKET>/bronze/*/`
 - Bronze → Silver: `src/batch/transform_bronze_to_silver.py` → writes parquet to `gs://<BUCKET>/silver/*/` (orders partitioned by date)
 - Silver → Gold: `src/batch/transform_silver_to_gold.py` → writes `gs://<BUCKET>/gold/dim_*/` and `gs://<BUCKET>/gold/fact_*/`
 - Gold → BigQuery: `src/warehouse/load_to_bq.py` loads GCS parquet into dataset `tiki_dwh` (with partitioning on `fact_orders`)

 ## How to run (local / dev)

 Prerequisites:
 - Docker & Docker Compose
 - Python 3.8+ (project venv used for CLI utilities)
 - Google Cloud service account JSON (mounted to container or available locally)

 Quick start (dev):

 ```bash
 # from repo root
 python -m venv .venv
 source .venv/bin/activate
 pip install -r requirements.txt

 # Start Airflow services (from deployments/docker)
 cd deployments/docker
 docker-compose up -d

 # Start Kafka (if using streaming) from deployments/kafka
 cd ../kafka
 docker-compose up -d
 ```

 Airflow UI: http://localhost:8080

 Run DAGs:
 - `crawl_products_dag` — triggers crawler and upload to GCS
 - `ecommerce_etl_pipeline` — runs Raw→Bronze→Silver→Gold and BigQuery load

 Notes for running locally:
 - Ensure GCS credentials are mounted in `deployments/docker/docker-compose.yml` or set `GOOGLE_APPLICATION_CREDENTIALS` locally
 - Set `GCS_BUCKET` environment variable to your test bucket or the default in the compose file

 ## Results / What to include in a report

 - Data Lake layout (list of GCS prefixes): `raw/`, `bronze/`, `silver/`, `gold/`
 - BigQuery dataset: `tiki_dwh` with tables: `dim_users`, `dim_products`, `dim_date`, `fact_orders`
 - Key transformations: deduplication, type casting, partitioning on `order_date_part` and `date_key` for facts
 - Performance/optimization notes: parquet format, partitioning, potential clustering on high-cardinality keys

 ## Security & production considerations

 - Do not mount raw service account JSON into source repo in production — use secrets manager
 - Replace `LocalExecutor` with `CeleryExecutor` and move metadata DB to a managed service for scale
 - Add data quality checks (Great Expectations) between Bronze→Silver and Silver→Gold

 ## Contact

 Project author: Tanh — use this repo as portfolio artifact for Data Engineer applications.

 ---