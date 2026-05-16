import json
import random
import time
from datetime import datetime, timezone

from faker import Faker
from confluent_kafka import Producer

fake = Faker()

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


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
    ]
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


def generate_event():

    category = random.choice(PRODUCT_CATEGORIES)

    product_name = random.choice(
        PRODUCTS.get(category, ["unknown"])
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

        "user_id": random.randint(
            1,
            1000000
        ),
        "user_name": fake.name(),
"email": fake.email(),
"gender": random.choice(["male", "female", "other"]),
"age": random.randint(18, 65),
"membership_tier": random.choice(["free", "plus", "premium"]),
"loyalty_points": random.randint(0, 50000),
"preferred_language": random.choice(["english", "hindi", "telugu", "tamil", "kannada"]),
"home_city": fake.city(),
"home_state": fake.state(),


        "event_time": datetime.now(
            timezone.utc
        ).isoformat(),

        "event_type": event_type,

        "user_journey_stage":
            random.choice(USER_JOURNEY_STAGES),

        "user_segment":
            random.choice(USER_SEGMENTS),

        "is_prime_user":
            random.choice([True, False]),

        "product_id":
            random.randint(10000, 99999),

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


while True:

    event = generate_event()

    producer.produce(
        "user_events",
        value=json.dumps(event).encode("utf-8")
    )

    producer.flush()

    print(
        json.dumps(
            event,
            indent=2
        )
    )

    time.sleep(
        random.uniform(0.05, 1.5)
    )