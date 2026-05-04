from faker import Faker
import pandas as pd
import json, random, os
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

PAGES = ["/home", "/product", "/cart", "/checkout", "/search", "/account", "/wishlist", "/category"]
METHODS = ["GET", "POST"]
STATUS = [200, 200, 200, 301, 404, 500]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]

def generate_log(customer_id, timestamp):
    return {
        "timestamp": timestamp.isoformat(),
        "customer_id": customer_id,
        "session_id": fake.uuid4(),
        "ip_address": fake.ipv4(),
        "method": random.choice(METHODS),
        "page": random.choice(PAGES),
        "status_code": random.choice(STATUS),
        "response_time_ms": random.randint(50, 3000),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "country": fake.country_code(),
        "bytes_transferred": random.randint(500, 50000)
    }

print("Generating web logs...")
logs = []
base_time = datetime.now() - timedelta(days=30)
customer_ids = [f"CUST_{str(i).zfill(6)}" for i in range(1, 10001)]

for _ in range(500000):
    ts = base_time + timedelta(seconds=random.randint(0, 30*24*3600))
    logs.append(generate_log(random.choice(customer_ids), ts))

output_path = os.path.dirname(os.path.abspath(__file__))
with open(f"{output_path}/weblogs.json", "w") as f:
    for log in logs:
        f.write(json.dumps(log) + "\n")

print(f"Done. Generated {len(logs):,} web log records.")
print(f"Saved to {output_path}/weblogs.json")
