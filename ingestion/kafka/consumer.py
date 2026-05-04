from kafka import KafkaConsumer
import psycopg2, json
from datetime import datetime

DB = psycopg2.connect(
    host="localhost", port=5432,
    dbname="pharoskart", user="pharos_user", password="pharos123"
)
DB.autocommit = False
cursor = DB.cursor()

consumers = {
    "pharoskart-transactions": KafkaConsumer(
        "pharoskart-transactions",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="pharos-transactions-group"
    ),
    "pharoskart-weblogs": KafkaConsumer(
        "pharoskart-weblogs",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="pharos-weblogs-group"
    ),
    "pharoskart-reviews": KafkaConsumer(
        "pharoskart-reviews",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="pharos-reviews-group"
    ),
    "pharoskart-social": KafkaConsumer(
        "pharoskart-social",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="pharos-social-group"
    ),
}

def insert_transaction(d):
    cursor.execute("""
        INSERT INTO live_transactions
            (transaction_id, customer_id, product_id, category, quantity,
             unit_price, discount_pct, total_amount, payment_method, status, timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (transaction_id) DO NOTHING
    """, (
        d["transaction_id"], d["customer_id"], d["product_id"],
        d["category"], d["quantity"], d["unit_price"], d["discount_pct"],
        d["total_amount"], d["payment_method"], d["status"],
        datetime.fromisoformat(d["timestamp"])
    ))

def insert_weblog(d):
    cursor.execute("""
        INSERT INTO live_weblogs
            (session_id, customer_id, ip_address, method, page,
             status_code, response_time_ms, device, country, timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (session_id) DO NOTHING
    """, (
        d["session_id"], d["customer_id"], d["ip_address"],
        d["method"], d["page"], d["status_code"],
        d["response_time_ms"], d["device"], d["country"],
        datetime.fromisoformat(d["timestamp"])
    ))

def insert_review(d):
    cursor.execute("""
        INSERT INTO live_reviews
            (review_id, customer_id, product_id, rating,
             review_text, verified_purchase, timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (review_id) DO NOTHING
    """, (
        d["review_id"], d["customer_id"], d["product_id"],
        d["rating"], d["review_text"], d["verified_purchase"],
        datetime.fromisoformat(d["timestamp"])
    ))

def insert_social(d):
    cursor.execute("""
        INSERT INTO live_social
            (post_id, customer_id, platform, content,
             sentiment, likes, timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (post_id) DO NOTHING
    """, (
        d["post_id"], d["customer_id"], d["platform"],
        d["content"], d["sentiment"], d["likes"],
        datetime.fromisoformat(d["timestamp"])
    ))

handlers = {
    "pharoskart-transactions": insert_transaction,
    "pharoskart-weblogs":      insert_weblog,
    "pharoskart-reviews":      insert_review,
    "pharoskart-social":       insert_social,
}

print("Starting PHAROSKART Kafka → PostgreSQL consumer...")
print("Listening on 4 topics. Press Ctrl+C to stop.\n")

counts = {"pharoskart-transactions": 0, "pharoskart-weblogs": 0,
          "pharoskart-reviews": 0, "pharoskart-social": 0}
batch = 0

try:
    while True:
        for topic, consumer in consumers.items():
            messages = consumer.poll(timeout_ms=500, max_records=20)
            for _, records in messages.items():
                for record in records:
                    try:
                        handlers[topic](record.value)
                        counts[topic] += 1
                    except Exception as e:
                        print(f"  Error on {topic}: {e}")
                        DB.rollback()

        DB.commit()
        batch += 1

        if batch % 10 == 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Inserted → "
                  f"transactions: {counts['pharoskart-transactions']:,} | "
                  f"weblogs: {counts['pharoskart-weblogs']:,} | "
                  f"reviews: {counts['pharoskart-reviews']:,} | "
                  f"social: {counts['pharoskart-social']:,}")

except KeyboardInterrupt:
    DB.commit()
    print(f"\nStopped.")
    print(f"Total inserted → transactions: {counts['pharoskart-transactions']:,} | "
          f"weblogs: {counts['pharoskart-weblogs']:,} | "
          f"reviews: {counts['pharoskart-reviews']:,} | "
          f"social: {counts['pharoskart-social']:,}")

finally:
    cursor.close()
    DB.close()
