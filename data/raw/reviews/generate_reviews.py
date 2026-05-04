from faker import Faker
import json, random, os
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

POSITIVE = ["Great product!", "Excellent quality", "Fast delivery", "Highly recommend",
            "Worth every penny", "Exceeded expectations", "Will buy again"]
NEGATIVE = ["Poor quality", "Late delivery", "Not as described", "Disappointed",
            "Waste of money", "Bad packaging", "Stopped working quickly"]
NEUTRAL  = ["Average product", "Okay for the price", "Nothing special",
            "Meets basic needs", "Could be better"]

def sentiment_text(rating):
    if rating >= 4:
        return random.choice(POSITIVE) + ". " + fake.sentence()
    elif rating <= 2:
        return random.choice(NEGATIVE) + ". " + fake.sentence()
    else:
        return random.choice(NEUTRAL) + ". " + fake.sentence()

def generate_review():
    rating = random.choices([1,2,3,4,5], weights=[5,10,15,30,40])[0]
    return {
        "review_id": f"REV_{fake.uuid4()[:8].upper()}",
        "customer_id": f"CUST_{str(random.randint(1, 10000)).zfill(6)}",
        "product_id": f"PROD_{str(random.randint(1, 5000)).zfill(5)}",
        "rating": rating,
        "review_text": sentiment_text(rating),
        "helpful_votes": random.randint(0, 200),
        "verified_purchase": random.choice([True, True, False]),
        "review_date": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
        "language": random.choices(["en","fr","de","es","ar","hi"], weights=[60,10,8,10,6,6])[0]
    }

print("Generating reviews...")
reviews = [generate_review() for _ in range(50000)]

output_path = os.path.dirname(os.path.abspath(__file__))
with open(f"{output_path}/reviews.json", "w") as f:
    for r in reviews:
        f.write(json.dumps(r) + "\n")

print(f"Done. Generated {len(reviews):,} review records.")
print(f"Saved to {output_path}/reviews.json")
