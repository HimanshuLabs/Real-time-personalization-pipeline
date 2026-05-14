import json
import random
import time
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer (
    bootstrap_servers= "localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

EVENT_TYPES = ["click", "view", "purchase", "page_view" , "product_view" , "add_to_cart" , "remove_from_cart" , "checkout" , "search" , "login" , "logout"]

PRODUCT_CATEGORIES = ["electronics", "clothing", "home", "beauty", "sports", "toys"]
USER_SEGMENTS = ["new_user", "returning_user", "loyal_customer", "high_spender", "bargain_hunter"]
PRODUCTS = ["iphone", "macbook", "headphones", "keyboard", "monitor", "shoes"]

def generate_event():
    return {
        "event_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(EVENT_TYPES),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(500, 200000), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "python-generator",
        "schema_version": "1.0"
    }

while True:
    event = generate_event()
    producer.send("user_events", value=event)
    producer.flush()

    print(event)
    time.sleep(1)