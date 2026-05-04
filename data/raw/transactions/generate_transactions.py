from faker import Faker
import pandas as pd
import random, os
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Toys", "Food"]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "wallet", "cod"]
STATUS = ["completed", "completed", "completed", "pending", "cancelled", "refunded"]

def generate_transaction():
    order_date = datetime.now() - timedelta(days=random.randint(0, 30))
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(5.0, 2000.0), 2)
    discount = round(random.uniform(0, 0.4), 2)
    return {
        "transaction_id": f"TXN_{fake.uuid4()[:8].upper()}",
        "customer_id": f"CUST_{str(random.randint(1, 10000)).zfill(6)}",
        "product_id": f"PROD_{str(random.randint(1, 5000)).zfill(5)}",
        "product_name": fake.catch_phrase(),
        "category": random.choice(CATEGORIES),
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_pct": discount,
        "total_amount": round(quantity * unit_price * (1 - discount), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(STATUS),
        "order_date": order_date.isoformat(),
        "delivery_date": (order_date + timedelta(days=random.randint(1, 10))).isoformat(),
        "shipping_country": fake.country_code(),
        "shipping_city": fake.city()
    }

print("Generating transactions...")
records = [generate_transaction() for _ in range(100000)]
df = pd.DataFrame(records)

output_path = os.path.dirname(os.path.abspath(__file__))
df.to_csv(f"{output_path}/transactions.csv", index=False)

print(f"Done. Generated {len(df):,} transaction records.")
print(f"Saved to {output_path}/transactions.csv")
