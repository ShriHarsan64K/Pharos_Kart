from kafka import KafkaProducer
from faker import Faker
import json, random, time
from datetime import datetime

fake = Faker()
random.seed()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PAGES = ["/home", "/product", "/cart", "/checkout", "/search", "/account"]
CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Toys", "Food"]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "wallet", "cod"]
PLATFORMS = ["twitter", "instagram", "facebook", "reddit"]
SENTIMENTS = ["positive", "neutral", "negative"]
HASHTAGS = ["#pharoskart", "#onlineshopping", "#deals", "#sale", "#review"]

customer_ids = [f"CUST_{str(i).zfill(6)}" for i in range(1, 10001)]

def make_weblog():
    return {
        "timestamp": datetime.now().isoformat(),
        "customer_id": random.choice(customer_ids),
        "session_id": fake.uuid4(),
        "ip_address": fake.ipv4(),
        "method": random.choice(["GET", "POST"]),
        "page": random.choice(PAGES),
        "status_code": random.choice([200, 200, 200, 404, 500]),
        "response_time_ms": random.randint(50, 3000),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "country": fake.country_code()
    }

def make_transaction():
    qty = random.randint(1, 5)
    price = round(random.uniform(5.0, 2000.0), 2)
    discount = round(random.uniform(0, 0.4), 2)
    return {
        "transaction_id": f"TXN_{fake.uuid4()[:8].upper()}",
        "customer_id": random.choice(customer_ids),
        "product_id": f"PROD_{str(random.randint(1, 5000)).zfill(5)}",
        "category": random.choice(CATEGORIES),
        "quantity": qty,
        "unit_price": price,
        "discount_pct": discount,
        "total_amount": round(qty * price * (1 - discount), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(["completed", "pending", "cancelled"]),
        "timestamp": datetime.now().isoformat()
    }

def make_review():
    rating = random.choices([1,2,3,4,5], weights=[5,10,15,30,40])[0]
    return {
        "review_id": f"REV_{fake.uuid4()[:8].upper()}",
        "customer_id": random.choice(customer_ids),
        "product_id": f"PROD_{str(random.randint(1, 5000)).zfill(5)}",
        "rating": rating,
        "review_text": fake.sentence(),
        "verified_purchase": random.choice([True, False]),
        "timestamp": datetime.now().isoformat()
    }

def make_social():
    return {
        "post_id": f"POST_{fake.uuid4()[:8].upper()}",
        "customer_id": random.choice(customer_ids),
        "platform": random.choice(PLATFORMS),
        "content": fake.text(max_nb_chars=280),
        "sentiment": random.choice(SENTIMENTS),
        "hashtags": random.sample(HASHTAGS, random.randint(1, 3)),
        "likes": random.randint(0, 5000),
        "timestamp": datetime.now().isoformat()
    }

print("Starting PHAROSKART Kafka producers...")
print("Streaming to 4 topics. Press Ctrl+C to stop.\n")

count = 0
try:
    while True:
        producer.send("pharoskart-weblogs",      make_weblog())
        producer.send("pharoskart-transactions",  make_transaction())
        producer.send("pharoskart-reviews",       make_review())
        producer.send("pharoskart-social",        make_social())

        count += 1
        if count % 10 == 0:
            producer.flush()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {count * 4:,} messages sent across 4 topics")

        time.sleep(0.5)

except KeyboardInterrupt:
    producer.flush()
    print(f"\nStopped. Total messages sent: {count * 4:,}")
