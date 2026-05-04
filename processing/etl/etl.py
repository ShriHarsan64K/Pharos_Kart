from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round, desc, month, year

spark = SparkSession.builder \
    .appName("PharosKart - ETL to Warehouse") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("\n========== PHAROSKART ETL ==========\n")

HDFS = "hdfs://localhost:9000"

transactions = spark.read.parquet(f"{HDFS}/pharoskart/processed/transactions")
reviews      = spark.read.parquet(f"{HDFS}/pharoskart/processed/reviews")
weblogs      = spark.read.parquet(f"{HDFS}/pharoskart/processed/weblogs")

# --- Fact: Sales by category ---
print("Building fact_sales_by_category...")
fact_sales = transactions.groupBy("category").agg(
    count("transaction_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_revenue"),
    round(avg("total_amount"), 2).alias("avg_order_value"),
    round(avg("discount_pct"), 4).alias("avg_discount")
).orderBy(desc("total_revenue"))
fact_sales.write.mode("overwrite").option("header", True).csv(f"{HDFS}/pharoskart/output/fact_sales_by_category")
print(f"  {fact_sales.count()} category rows written")

# --- Fact: Product performance ---
print("Building fact_product_performance...")
product_txn = transactions.groupBy("product_id").agg(
    count("transaction_id").alias("total_sold"),
    round(sum("total_amount"), 2).alias("total_revenue")
)
product_rev = reviews.groupBy("product_id").agg(
    round(avg("rating"), 2).alias("avg_rating"),
    count("review_id").alias("review_count")
)
fact_products = product_txn.join(product_rev, "product_id", "left")
fact_products.write.mode("overwrite").option("header", True).csv(f"{HDFS}/pharoskart/output/fact_product_performance")
print(f"  {fact_products.count()} product rows written")

# --- Dim: Payment method breakdown ---
print("Building dim_payment_methods...")
dim_payment = transactions.groupBy("payment_method").agg(
    count("transaction_id").alias("total_transactions"),
    round(sum("total_amount"), 2).alias("total_value")
).orderBy(desc("total_transactions"))
dim_payment.write.mode("overwrite").option("header", True).csv(f"{HDFS}/pharoskart/output/dim_payment_methods")
print(f"  {dim_payment.count()} payment method rows written")

# --- Dim: Traffic by device ---
print("Building dim_traffic_by_device...")
dim_device = weblogs.groupBy("device").agg(
    count("session_id").alias("total_sessions"),
    avg("response_time_ms").alias("avg_response_time")
)
dim_device.write.mode("overwrite").option("header", True).csv(f"{HDFS}/pharoskart/output/dim_traffic_by_device")
print(f"  {dim_device.count()} device rows written")

print("\nAll outputs written to /pharoskart/output/")
print("\n========== ETL COMPLETE ==========")
spark.stop()
