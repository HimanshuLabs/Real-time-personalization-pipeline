import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from functools import lru_cache

from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# Separate instance for profiles; reseeding it never touches `fake`,
# so event_id/session_id UUIDs stay unique.
profile_fake = Faker()

NUM_USERS = int(os.environ.get("NUM_USERS", "5000"))

FLUSH_TIMEOUT_SECONDS = 10

delivery_stats = {"delivered": 0, "failed": 0}


EVENT_TYPES = [
    "home_page_view",
    "search",
    "search_result_click",
    "product_view",
    "image_zoom",
    "wishlist_add",
    "wishlist_remove",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "payment_initiated",
    "payment_success",
    "payment_failed",
    "order_placed",
    "order_cancelled",
    "return_requested",
    "review_submitted",
    "rating_given",
    "recommendation_impression",
    "recommendation_click",
    "ad_impression",
    "ad_click",
    "coupon_applied",
    "delivery_tracking_view",
    "login",
    "logout"
]

PRODUCT_CATEGORIES = [
    "electronics",
    "fashion",
    "beauty",
    "home",
    "sports",
    "books",
    "grocery",
    "toys"
]

PRODUCTS = {
    "electronics": [
        "iphone_15",
        "macbook_air",
        "sony_headphones",
        "gaming_mouse",
        "monitor_4k"
    ],

    "fashion": [
        "nike_shoes",
        "hoodie",
        "cargo_pants",
        "oversized_tshirt",
        "jacket"
    ],

    "beauty": [
        "facewash",
        "moisturizer",
        "serum",
        "perfume"
    ],

    "home": [
        "air_fryer",
        "coffee_machine",
        "vacuum_cleaner"
    ],

    "sports": [
        "dumbbells",
        "yoga_mat",
        "cricket_bat"
    ],

    "books": [
        "atomic_habits",
        "sapiens",
        "python_crash_course"
    ],

    "grocery": [
        "basmati_rice",
        "olive_oil",
        "green_tea"
    ],

    "toys": [
        "lego_set",
        "rubiks_cube",
        "rc_car"
    ]
}

# Stable id per product: sorted names -> 10000 + index, built once.
PRODUCT_IDS = {
    name: 10000 + index
    for index, name in enumerate(
        sorted(name for names in PRODUCTS.values() for name in names)
    )
}

USER_SEGMENTS = [
    "new_user",
    "returning_user",
    "loyal_customer",
    "high_spender",
    "discount_hunter"
]

PAYMENT_METHODS = [
    "upi",
    "credit_card",
    "debit_card",
    "wallet",
    "cash_on_delivery",
    "net_banking"
]

DEVICES = [
    "mobile",
    "desktop",
    "tablet"
]

OPERATING_SYSTEMS = [
    "android",
    "ios",
    "windows",
    "macos",
    "linux"
]

BROWSERS = [
    "chrome",
    "firefox",
    "edge",
    "safari"
]

NETWORK_TYPES = [
    "wifi",
    "4g",
    "5g",
    "ethernet"
]

TRAFFIC_SOURCES = [
    "organic",
    "google_ads",
    "instagram_ads",
    "facebook_ads",
    "affiliate",
    "email_campaign",
    "push_notification"
]

SEARCH_QUERIES = [
    "iphone",
    "gaming laptop",
    "wireless headphones",
    "running shoes",
    "protein powder",
    "hoodie",
    "smart watch",
    "face serum"
]

RECOMMENDATION_ALGORITHMS = [
    "collaborative_filtering",
    "content_based",
    "deep_learning",
    "trending",
    "hybrid_v2"
]

USER_JOURNEY_STAGES = [
    "discovery",
    "comparison",
    "consideration",
    "checkout",
    "post_purchase"
]


def weighted_event():

    events = [
        ("product_view", 20),
        ("search", 15),
        ("recommendation_impression", 15),
        ("recommendation_click", 10),
        ("add_to_cart", 8),
        ("checkout_started", 5),
        ("payment_success", 3),
        ("wishlist_add", 5),
        ("home_page_view", 10),
        ("ad_click", 3),
        ("review_submitted", 2),
        ("logout", 1),
        ("login", 3)
    ]

    population = [x[0] for x in events]

    weights = [x[1] for x in events]

    return random.choices(
        population,
        weights=weights,
        k=1
    )[0]


@lru_cache(maxsize=None)
def get_user_profile(user_id):

    rng = random.Random(user_id)

    profile_fake.seed_instance(user_id)

    return {
        "user_name": profile_fake.name(),
        "email": profile_fake.email(),
        "gender": rng.choice(["male", "female", "other"]),
        "age": rng.randint(18, 65),
        "membership_tier": rng.choice(["free", "plus", "premium"]),
        "loyalty_points": rng.randint(0, 50000),
        "preferred_language": rng.choice(["english", "hindi", "telugu", "tamil", "kannada"]),
        "home_city": profile_fake.city(),
        "home_state": profile_fake.state(),
        "user_segment": rng.choice(USER_SEGMENTS),
        "is_prime_user": rng.choice([True, False]),
    }


def generate_event():

    user_id = random.randint(1, NUM_USERS)

    profile = get_user_profile(user_id)

    category = random.choice(PRODUCT_CATEGORIES)

    product_name = random.choice(
        PRODUCTS[category]
    )

    event_type = weighted_event()

    session_duration_sec = random.randint(
        30,
        7200
    )

    time_on_page_sec = round(
        random.uniform(1, 900),
        2
    )

    scroll_depth_percent = random.randint(
        0,
        100
    )

    hover_duration_ms = random.randint(
        0,
        20000
    )

    repeat_product_view_count = random.randint(
        1,
        20
    )

    recommendation_rank = random.randint(
        1,
        50
    )

    engagement_score = round(
        (
            time_on_page_sec * 0.4
            + scroll_depth_percent * 0.3
            + repeat_product_view_count * 2
        ) / 10,
        2
    )

    purchase_probability = round(
        min(1.0, engagement_score / 10),
        4
    )

    discount_percent = round(
        random.uniform(0, 70),
        2
    )

    original_price = round(
        random.uniform(100, 250000),
        2
    )

    discounted_price = round(
        original_price - (
            original_price * discount_percent / 100
        ),
        2
    )

    quantity = random.randint(1, 5)

    cart_value = round(
        discounted_price * quantity,
        2
    )

    recommendation_clicked = random.choice(
        [True, False]
    )

    return {

        "event_id": fake.uuid4(),

        "session_id": fake.uuid4(),

        "user_id": user_id,

        "user_name": profile["user_name"],
        "email": profile["email"],
        "gender": profile["gender"],
        "age": profile["age"],
        "membership_tier": profile["membership_tier"],
        "loyalty_points": profile["loyalty_points"],
        "preferred_language": profile["preferred_language"],
        "home_city": profile["home_city"],
        "home_state": profile["home_state"],


        "event_time": datetime.now(
            timezone.utc
        ).isoformat(),

        "event_type": event_type,

        "user_journey_stage":
            random.choice(USER_JOURNEY_STAGES),

        "user_segment":
            profile["user_segment"],

        "is_prime_user":
            profile["is_prime_user"],

        "product_id":
            PRODUCT_IDS[product_name],

        "product_name":
            product_name,

        "category":
            category,

        "quantity":
            quantity,

        "original_price":
            original_price,

        "discount_percent":
            discount_percent,

        "discounted_price":
            discounted_price,

        "cart_value":
            cart_value,

        "inventory_remaining":
            random.randint(0, 500),

        "search_query":
            random.choice(SEARCH_QUERIES),

        "time_on_page_sec":
            time_on_page_sec,

        "scroll_depth_percent":
            scroll_depth_percent,

        "hover_duration_ms":
            hover_duration_ms,

        "session_duration_sec":
            session_duration_sec,

        "items_viewed_in_session":
            random.randint(1, 100),

        "repeat_product_view_count":
            repeat_product_view_count,

        "time_since_last_event_ms":
            random.randint(100, 600000),

        "recommendation_rank":
            recommendation_rank,

        "recommendation_clicked":
            recommendation_clicked,

        "recommendation_algorithm":
            random.choice(
                RECOMMENDATION_ALGORITHMS
            ),

        "click_position":
            random.randint(1, 20),

        "engagement_score":
            engagement_score,

        "purchase_probability":
            purchase_probability,

        "cart_abandonment_probability":
            round(random.uniform(0, 1), 4),

        "ab_test_group":
            random.choice(["A", "B", "C"]),

        "payment_method":
            random.choice(PAYMENT_METHODS),

        "device_type":
            random.choice(DEVICES),

        "operating_system":
            random.choice(OPERATING_SYSTEMS),

        "browser":
            random.choice(BROWSERS),

        "network_type":
            random.choice(NETWORK_TYPES),

        "app_version":
            f"{random.randint(1,5)}.{random.randint(0,9)}",

        "traffic_source":
            random.choice(TRAFFIC_SOURCES),

        "campaign_id":
            random.randint(1000, 9999),

        "api_latency_ms":
            random.randint(20, 2000),

        "page_load_time_ms":
            random.randint(100, 8000),

        "fraud_score":
            round(random.uniform(0, 1), 4),

        "country":
            fake.country(),

        "city":
            fake.city(),

        "ip_address":
            fake.ipv4(),

        "schema_version": "4.0",

        "source":
            "real-time-personalization-generator"
    }


def on_delivery(err, msg):

    if err is not None:
        delivery_stats["failed"] += 1
        print(
            f"[producer] delivery failed: topic={msg.topic()} error={err}",
            file=sys.stderr,
            flush=True
        )
    else:
        delivery_stats["delivered"] += 1


def produce_event(producer, event):

    value = json.dumps(event).encode("utf-8")

    # Keyed by user_id so each user's events stay in order on one partition.
    key = str(event["user_id"])

    try:
        producer.produce(
            "user_events",
            key=key,
            value=value,
            on_delivery=on_delivery
        )
    except BufferError:
        # Local queue full: serve callbacks for up to 1s, then retry once.
        producer.poll(1)
        producer.produce(
            "user_events",
            key=key,
            value=value,
            on_delivery=on_delivery
        )

    # Serve delivery callbacks without blocking.
    producer.poll(0)


def main():

    producer = Producer({
        "bootstrap.servers": "localhost:9092"
    })

    try:
        while True:

            event = generate_event()

            produce_event(producer, event)

            print(
                json.dumps(
                    event,
                    indent=2
                )
            )

            time.sleep(
                random.uniform(0.05, 1.5)
            )

    except KeyboardInterrupt:
        pass

    finally:
        undelivered = producer.flush(FLUSH_TIMEOUT_SECONDS)
        print(
            f"[producer] summary: delivered={delivery_stats['delivered']} "
            f"failed={delivery_stats['failed']} undelivered={undelivered}",
            file=sys.stderr,
            flush=True
        )


if __name__ == "__main__":
    main()
