from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, to_timestamp, lit

spark = SparkSession.builder \
    .appName("PharosKart - Data Cleaning") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("\n========== PHAROSKART DATA CLEANING ==========\n")

HDFS = "hdfs://localhost:9000"

# --- Weblogs ---
print("Cleaning weblogs...")
weblogs = spark.read.json(f"{HDFS}/pharoskart/raw/weblogs/weblogs.json")
weblogs_clean = weblogs \
    .dropna(subset=["customer_id", "timestamp", "page"]) \
    .filter(col("status_code").isNotNull()) \
    .filter(col("response_time_ms") > 0) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("customer_id", trim(col("customer_id")))
print(f"  Raw: {weblogs.count():,} | Clean: {weblogs_clean.count():,}")

# --- Transactions ---
print("Cleaning transactions...")
transactions = spark.read.csv(f"{HDFS}/pharoskart/raw/transactions/transactions.csv", header=True, inferSchema=True)
transactions_clean = transactions \
    .dropna(subset=["transaction_id", "customer_id", "total_amount"]) \
    .filter(col("total_amount") > 0) \
    .filter(col("quantity") > 0) \
    .withColumn("status", when(col("status").isNull(), lit("unknown")).otherwise(col("status"))) \
    .withColumn("customer_id", trim(col("customer_id")))
print(f"  Raw: {transactions.count():,} | Clean: {transactions_clean.count():,}")

# --- Reviews ---
print("Cleaning reviews...")
reviews = spark.read.json(f"{HDFS}/pharoskart/raw/reviews/reviews.json")
reviews_clean = reviews \
    .dropna(subset=["review_id", "customer_id", "rating"]) \
    .filter(col("rating").between(1, 5)) \
    .filter(col("review_text").isNotNull()) \
    .withColumn("customer_id", trim(col("customer_id")))
print(f"  Raw: {reviews.count():,} | Clean: {reviews_clean.count():,}")

# --- Social ---
print("Cleaning social media...")
social = spark.read.json(f"{HDFS}/pharoskart/raw/social/social.json")
social_clean = social \
    .dropna(subset=["post_id", "customer_id", "platform"]) \
    .filter(col("content").isNotNull()) \
    .withColumn("customer_id", trim(col("customer_id")))
print(f"  Raw: {social.count():,} | Clean: {social_clean.count():,}")

# --- Save cleaned data to HDFS ---
print("\nSaving cleaned data to HDFS...")
weblogs_clean.write.mode("overwrite").parquet(f"{HDFS}/pharoskart/processed/weblogs")
transactions_clean.write.mode("overwrite").parquet(f"{HDFS}/pharoskart/processed/transactions")
reviews_clean.write.mode("overwrite").parquet(f"{HDFS}/pharoskart/processed/reviews")
social_clean.write.mode("overwrite").parquet(f"{HDFS}/pharoskart/processed/social")

print("\n========== CLEANING COMPLETE ==========")
print("Cleaned data saved to /pharoskart/processed/")
spark.stop()
