from faker import Faker
import json, random, os
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

PLATFORMS = ["twitter", "instagram", "facebook", "reddit"]
SENTIMENTS = ["positive", "neutral", "negative"]
HASHTAGS = ["#pharoskart", "#onlineshopping", "#deals", "#sale", "#review",
            "#shopping", "#ecommerce", "#discount", "#fashion", "#tech"]

def generate_post():
    platform = random.choice(PLATFORMS)
    sentiment = random.choices(SENTIMENTS, weights=[50, 30, 20])[0]
    return {
        "post_id": f"POST_{fake.uuid4()[:8].upper()}",
        "customer_id": f"CUST_{str(random.randint(1, 10000)).zfill(6)}",
        "platform": platform,
        "content": fake.text(max_nb_chars=280),
        "sentiment": sentiment,
        "hashtags": random.sample(HASHTAGS, random.randint(1, 4)),
        "likes": random.randint(0, 5000),
        "shares": random.randint(0, 500),
        "comments": random.randint(0, 200),
        "posted_at": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
        "country": fake.country_code(),
        "is_mention": random.choice([True, False, False])
    }

print("Generating social media posts...")
posts = [generate_post() for _ in range(20000)]

output_path = os.path.dirname(os.path.abspath(__file__))
with open(f"{output_path}/social.json", "w") as f:
    for p in posts:
        f.write(json.dumps(p) + "\n")

print(f"Done. Generated {len(posts):,} social media records.")
print(f"Saved to {output_path}/social.json")
