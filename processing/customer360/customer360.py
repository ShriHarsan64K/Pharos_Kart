from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, countDistinct, round

spark = SparkSession.builder \
    .appName("PharosKart - Customer 360") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("\n========== PHAROSKART CUSTOMER 360 ==========\n")

HDFS = "hdfs://localhost:9000"

# Load cleaned data
weblogs     = spark.read.parquet(f"{HDFS}/pharoskart/processed/weblogs")
transactions= spark.read.parquet(f"{HDFS}/pharoskart/processed/transactions")
reviews     = spark.read.parquet(f"{HDFS}/pharoskart/processed/reviews")
social      = spark.read.parquet(f"{HDFS}/pharoskart/processed/social")

# --- Aggregate per customer ---

# Web behaviour
web_agg = weblogs.groupBy("customer_id").agg(
    count("session_id").alias("total_sessions"),
    countDistinct("page").alias("unique_pages_visited"),
    avg("response_time_ms").alias("avg_response_time"),
    countDistinct("device").alias("device_count")
)

# Transaction behaviour
txn_agg = transactions.groupBy("customer_id").agg(
    count("transaction_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_spent"),
    round(avg("total_amount"), 2).alias("avg_order_value"),
    max("total_amount").alias("max_order_value"),
    countDistinct("category").alias("categories_purchased")
)

# Review behaviour
rev_agg = reviews.groupBy("customer_id").agg(
    count("review_id").alias("total_reviews"),
    round(avg("rating"), 2).alias("avg_rating_given"),
    sum("helpful_votes").alias("total_helpful_votes")
)

# Social behaviour
soc_agg = social.groupBy("customer_id").agg(
    count("post_id").alias("total_posts"),
    countDistinct("platform").alias("platforms_used"),
    sum("likes").alias("total_likes_received"),
    sum("shares").alias("total_shares")
)

# --- Join all into Customer 360 ---
print("Building Customer 360 view...")
customer360 = web_agg \
    .join(txn_agg, "customer_id", "left") \
    .join(rev_agg, "customer_id", "left") \
    .join(soc_agg, "customer_id", "left")

total = customer360.count()
print(f"  Unified customer profiles built: {total:,}")

# Save
customer360.write.mode("overwrite").parquet(f"{HDFS}/pharoskart/processed/customer360")
customer360.write.mode("overwrite").option("header", True).csv(f"{HDFS}/pharoskart/output/customer360_csv")

print("\nSample Customer 360 profiles:")
customer360.show(5, truncate=False)

print("\n========== CUSTOMER 360 COMPLETE ==========")
spark.stop()
