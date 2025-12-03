from src.batch.utils.spark_session import get_spark
import pyspark.sql.functions as F

BUCKET = "my-ecommerce-dev-bucket"

def check_gold():
    spark = get_spark("Check_Gold")
    
    print("---Kiểm tra Fact_orders---")
    fact = spark.read.parquet(f"gs://{BUCKET}/gold/fact_orders/")
    print(f"Tổng số đơn hàng: {fact.count()}")
    fact.show(5)
    
    print("---Doanh thu tho ngày---")
    revenue = fact.groupBy("date_key").agg(F.sum("total_amount").alias("revenue")).orderBy("date_key")
    revenue.show(5)
    
    spark.stop()
    
if __name__ == "__main__":
    check_gold()
