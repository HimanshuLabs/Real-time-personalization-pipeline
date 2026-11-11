import json
import math
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

# Event types that concern a product. Must match COMMERCE_EVENT_TYPES in
# spark-streaming/read_stream.py.
COMMERCE_EVENT_TYPES = [
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
    "search_result_click"
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

# Realistic list-price range (low, high) per category.
CATEGORY_PRICE_BANDS = {
    "grocery": (50, 2000),
    "books": (200, 1500),
    "beauty": (200, 5000),
    "toys": (300, 8000),
    "fashion": (500, 15000),
    "sports": (500, 20000),
    "home": (2000, 40000),
    "electronics": (1000, 150000),
}

# Fixed list price per product (implicit currency INR), each inside its
# category band. The product dimension uses the same prices.
PRODUCT_REFERENCE_PRICES = {
    "iphone_15": 79900.00,
    "macbook_air": 99900.00,
    "sony_headphones": 29990.00,
    "gaming_mouse": 2499.00,
    "monitor_4k": 24999.00,

    "nike_shoes": 7995.00,
    "hoodie": 1999.00,
    "cargo_pants": 2499.00,
    "oversized_tshirt": 799.00,
    "jacket": 4999.00,

    "facewash": 349.00,
    "moisturizer": 599.00,
    "serum": 899.00,
    "perfume": 2999.00,

    "air_fryer": 6999.00,
    "coffee_machine": 12999.00,
    "vacuum_cleaner": 15999.00,

    "dumbbells": 2499.00,
    "yoga_mat": 999.00,
    "cricket_bat": 3499.00,

    "atomic_habits": 499.00,
    "sapiens": 599.00,
    "python_crash_course": 1499.00,

    "basmati_rice": 899.00,
    "olive_oil": 1199.00,
    "green_tea": 449.00,

    "lego_set": 4999.00,
    "rubiks_cube": 499.00,
    "rc_car": 2999.00,
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

# A search uses a query that fits the session's category.
CATEGORY_SEARCH_QUERIES = {
    "electronics": ["iphone", "laptop", "wireless headphones", "gaming mouse", "4k monitor"],
    "fashion": ["running shoes", "hoodie", "cargo pants", "oversized t-shirt", "winter jacket"],
    "beauty": ["face wash", "moisturizer", "face serum", "perfume"],
    "home": ["air fryer", "coffee machine", "vacuum cleaner"],
    "sports": ["dumbbells", "yoga mat", "cricket bat"],
    "books": ["atomic habits", "sapiens", "python book"],
    "grocery": ["basmati rice", "olive oil", "green tea"],
    "toys": ["lego", "rubiks cube", "remote control car"],
}

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


MAX_ACTIVE_SESSIONS = 20

# Funnel step probabilities; each step is one event.
LOGIN_ENTRY_RATE = 0.30                # otherwise home_page_view
AD_IMPRESSION_RATE = 0.20              # after home_page_view
AD_CLICK_RATE = 0.25                   # of ad impressions
SEARCH_RATE = 0.50
RECOMMENDATION_BEFORE_VIEW_RATE = 0.25
IMAGE_ZOOM_RATE = 0.20                 # after a product_view
WISHLIST_RATE = 0.05                   # after a product_view
WISHLIST_EXIT_RATE = 0.50              # session leaves the funnel after wishlist_add
ADD_TO_CART_RATE = 0.18                # of sessions that viewed a product
REMOVE_FROM_CART_RATE = 0.10           # of carts; no checkout afterwards
CHECKOUT_RATE = 0.60                   # of carts kept
COUPON_RATE = 0.20                     # of checkouts
PAYMENT_SUCCESS_RATE = 0.85            # otherwise payment_failed
FEEDBACK_RATE = 0.05                   # review or rating after order_placed
LOGOUT_RATE = 0.40

# Product and price fields: all None on non-commerce events.
PRODUCT_FIELDS = [
    "product_id",
    "product_name",
    "category",
    "quantity",
    "original_price",
    "discount_percent",
    "discounted_price",
    "cart_value",
]

# Cart events: the cart quantity applies and cart_value is set.
CART_STAGE_EVENT_TYPES = {
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "payment_initiated",
    "payment_success",
    "payment_failed",
    "order_placed",
}


# user_journey_stage follows the event type; logout keeps the session's latest stage.
JOURNEY_STAGE_BY_EVENT_TYPE = {
    "home_page_view": "discovery",
    "login": "discovery",
    "search": "discovery",
    "ad_impression": "discovery",
    "ad_click": "discovery",
    "product_view": "consideration",
    "image_zoom": "consideration",
    "wishlist_add": "consideration",
    "recommendation_impression": "consideration",
    "recommendation_click": "consideration",
    "add_to_cart": "checkout",
    "remove_from_cart": "checkout",
    "checkout_started": "checkout",
    "coupon_applied": "checkout",
    "payment_initiated": "checkout",
    "payment_failed": "checkout",
    "payment_success": "checkout",
    "order_placed": "post_purchase",
    "review_submitted": "post_purchase",
    "rating_given": "post_purchase",
}

# Operating systems per device type, and the browsers each one runs.
DEVICE_OS_BROWSERS = {
    "mobile": {
        "android": ["chrome", "firefox", "edge"],
        "ios": ["safari", "chrome", "firefox", "edge"],
    },
    "tablet": {
        "android": ["chrome", "firefox", "edge"],
        "ios": ["safari", "chrome", "firefox", "edge"],
    },
    "desktop": {
        "windows": ["chrome", "firefox", "edge"],
        "macos": ["safari", "chrome", "firefox", "edge"],
        "linux": ["chrome", "firefox"],
    },
}

DEVICE_NETWORKS = {
    "mobile": ["wifi", "4g", "5g"],
    "tablet": ["wifi", "4g", "5g"],
    "desktop": ["wifi", "ethernet"],
}

# payment_method is set only on these events.
PAYMENT_EVENT_TYPES = {
    "payment_initiated",
    "payment_success",
    "payment_failed",
    "order_placed",
}


def plan_session_steps():
    steps = []
    if random.random() < LOGOUT_RATE:
        ending = ["logout"]
    else:
        ending = []

    if random.random() < LOGIN_ENTRY_RATE:
        steps.append("login")
    else:
        steps.append("home_page_view")
        if random.random() < AD_IMPRESSION_RATE:
            steps.append("ad_impression")
            if random.random() < AD_CLICK_RATE:
                steps.append("ad_click")

    if random.random() < SEARCH_RATE:
        steps.append("search")

    for _ in range(random.randint(1, 3)):
        if random.random() < RECOMMENDATION_BEFORE_VIEW_RATE:
            steps += ["recommendation_impression", "recommendation_click"]
        steps.append("product_view")

        if random.random() < IMAGE_ZOOM_RATE:
            steps.append("image_zoom")

        if random.random() < WISHLIST_RATE:
            steps.append("wishlist_add")
            if random.random() < WISHLIST_EXIT_RATE:
                return steps + ending

    if random.random() >= ADD_TO_CART_RATE:
        return steps + ending
    steps.append("add_to_cart")

    if random.random() < REMOVE_FROM_CART_RATE:
        return steps + ["remove_from_cart"] + ending

    if random.random() >= CHECKOUT_RATE:
        return steps + ending
    steps.append("checkout_started")

    if random.random() < COUPON_RATE:
        steps.append("coupon_applied")
    steps.append("payment_initiated")

    if random.random() >= PAYMENT_SUCCESS_RATE:
        return steps + ["payment_failed"] + ending
    steps += ["payment_success", "order_placed"]

    if random.random() < FEEDBACK_RATE:
        steps.append(random.choice(["review_submitted", "rating_given"]))

    return steps + ending


# Funnel-stage score for purchase_probability: higher further down the funnel.
FUNNEL_STAGE_SCORES = {
    "login": -3.5,
    "home_page_view": -3.5,
    "ad_impression": -3.5,
    "ad_click": -3.2,
    "logout": -3.0,
    "search": -3.0,
    "recommendation_impression": -2.8,
    "remove_from_cart": -2.5,
    "recommendation_click": -2.2,
    "product_view": -2.0,
    "image_zoom": -1.8,
    "wishlist_add": -1.5,
    "add_to_cart": -0.5,
    "payment_failed": -0.5,
    "checkout_started": 0.5,
    "coupon_applied": 0.8,
    "payment_initiated": 1.0,
    "review_submitted": 1.5,
    "rating_given": 1.5,
    "payment_success": 2.0,
    "order_placed": 2.0,
}
DEFAULT_STAGE_SCORE = -3.0
STAGE_SCORE_NOISE = 0.4

HOME_COUNTRY = "United States"   # the profiles are US (Faker en_US)
ABROAD_RATE = 0.05               # events from a random other country
HOME_CITY_RATE = 0.85            # events from the profile's home_city

INITIAL_STOCK_RANGE = (50, 500)
RESTOCK_BELOW = 5

MAX_INT32 = 2_147_483_647   # Bronze stores time_since_last_event_ms as a 32-bit int


def draw_purchase_probability(event_type):
    score = FUNNEL_STAGE_SCORES.get(event_type, DEFAULT_STAGE_SCORE)
    score += random.gauss(0, STAGE_SCORE_NOISE)
    probability = 1 / (1 + math.exp(-score))
    # Rounded to 4 decimals but kept strictly between 0 and 1.
    return round(min(max(probability, 0.0001), 0.9999), 4)


def draw_location(profile):
    if random.random() < ABROAD_RATE:
        country = fake.country()
        while country.startswith("United States"):
            country = fake.country()
        return country, fake.city()

    # Share of the at-home events that use home_city, so HOME_CITY_RATE holds overall.
    if random.random() < HOME_CITY_RATE / (1 - ABROAD_RATE):
        return HOME_COUNTRY, profile["home_city"]
    return HOME_COUNTRY, fake.city()


def draw_discount_percent():
    # Mostly 0-15%, sometimes 15-40%, never above 50%.
    if random.random() < 0.85:
        return round(random.uniform(0, 15), 2)
    return round(random.uniform(15, 40), 2)


class Session:
    """One user, one product and one price for every event in the session."""

    def __init__(self, user_id):
        self.user_id = user_id
        self.session_id = fake.uuid4()
        self.category = random.choice(
            [category for category in PRODUCT_CATEGORIES if ACTIVE_PRODUCTS[category]]
        )
        self.product_name = random.choice(ACTIVE_PRODUCTS[self.category])

        # Within +/-10% of the product's list price; fixed for the session.
        self.original_price = round(
            PRODUCT_REFERENCE_PRICES[self.product_name] * random.uniform(0.9, 1.1),
            2
        )
        self.discount_percent = draw_discount_percent()
        self.discounted_price = round(
            self.original_price - self.original_price * self.discount_percent / 100,
            2
        )

        self.cart_quantity = None
        self.steps = plan_session_steps()
        self.position = 0

        self.started_at = None       # time of the session's first event
        self.product_views = 0       # product_view events so far, this one included
        self.recommendation = None   # (rank, algorithm) shown by the last impression
        self.journey_stage = None    # stage of the latest event with a stage

        # Device context and payment method, fixed for the session.
        self.device_type = random.choice(DEVICES)
        self.operating_system = random.choice(list(DEVICE_OS_BROWSERS[self.device_type]))
        self.browser = random.choice(DEVICE_OS_BROWSERS[self.device_type][self.operating_system])
        self.network_type = random.choice(DEVICE_NETWORKS[self.device_type])
        self.app_version = f"{random.randint(1, 5)}.{random.randint(0, 9)}"
        self.payment_method = random.choice(PAYMENT_METHODS)

    @property
    def finished(self):
        return self.position >= len(self.steps)

    def next_event_type(self, now):
        event_type = self.steps[self.position]
        self.position += 1

        if self.started_at is None:
            self.started_at = now

        # logout (no entry in the map) keeps the latest stage.
        self.journey_stage = JOURNEY_STAGE_BY_EVENT_TYPE.get(event_type, self.journey_stage)

        if event_type == "add_to_cart":
            self.cart_quantity = random.randint(1, 3)
        elif event_type == "product_view":
            self.product_views += 1
        elif event_type == "recommendation_impression":
            self.recommendation = (
                random.randint(1, 50),
                random.choice(RECOMMENDATION_ALGORITHMS),
            )

        return event_type

    def recommendation_fields(self, event_type):
        # The click reuses the rank and algorithm of the impression before it.
        if event_type not in ("recommendation_impression", "recommendation_click"):
            return {"rank": None, "clicked": None, "algorithm": None, "click_position": None}

        rank, algorithm = self.recommendation
        clicked = event_type == "recommendation_click"
        return {
            "rank": rank,
            "clicked": clicked,
            "algorithm": algorithm,
            "click_position": random.randint(1, 20) if clicked else None,
        }

    def search_query(self, event_type):
        if event_type != "search":
            return None
        return random.choice(CATEGORY_SEARCH_QUERIES[self.category])

    def product_fields(self, event_type):
        if event_type not in COMMERCE_EVENT_TYPES:
            return dict.fromkeys(PRODUCT_FIELDS)

        in_cart = event_type in CART_STAGE_EVENT_TYPES
        quantity = self.cart_quantity if in_cart else 1

        return {
            "product_id": PRODUCT_IDS[self.product_name],
            "product_name": self.product_name,
            "category": self.category,
            "quantity": quantity,
            "original_price": self.original_price,
            "discount_percent": self.discount_percent,
            "discounted_price": self.discounted_price,
            "cart_value": round(self.discounted_price * quantity, 2) if in_cart else None,
        }


class SessionPool:
    """Up to max_active concurrent sessions; a finished one is replaced."""

    def __init__(self, max_active=MAX_ACTIVE_SESSIONS):
        # One session per user at a time, so never more sessions than users.
        self.max_active = min(max_active, NUM_USERS)
        self.active = []
        self.sessions_completed = 0
        self.last_event_at = {}   # user_id -> time of that user's previous event
        self.stock = {}           # product_name -> units in stock

    def start_session(self):
        # Active accounts only, one session per user at a time.
        busy_users = {session.user_id for session in self.active}
        user_id = random.randint(1, NUM_USERS)
        while user_id in busy_users or account_status(user_id) != "active":
            user_id = random.randint(1, NUM_USERS)
        self.active.append(Session(user_id))

    def next_event(self):
        while len(self.active) < self.max_active:
            self.start_session()

        session = random.choice(self.active)
        now = datetime.now(timezone.utc)
        event_type = session.next_event_type(now)

        event = build_event(
            session,
            event_type,
            now,
            time_since_last_event_ms=self.time_since_last_event_ms(session.user_id, now),
            inventory_remaining=self.inventory_remaining(session, event_type),
        )

        if session.finished:
            self.active.remove(session)
            self.sessions_completed += 1

        return event

    def time_since_last_event_ms(self, user_id, now):
        # Across sessions; None on the user's first event since the producer
        # started, and None for a gap too large for Bronze's 32-bit column.
        previous = self.last_event_at.get(user_id)
        self.last_event_at[user_id] = now
        if previous is None:
            return None
        gap_ms = round((now - previous).total_seconds() * 1000)
        return gap_ms if gap_ms <= MAX_INT32 else None

    def inventory_remaining(self, session, event_type):
        # Stock after this event: drops on payment_success and restocks below
        # RESTOCK_BELOW (the restock shows from the product's next event).
        if event_type not in COMMERCE_EVENT_TYPES:
            return None

        product = session.product_name
        if product not in self.stock:
            self.stock[product] = random.randint(*INITIAL_STOCK_RANGE)

        if event_type == "payment_success":
            self.stock[product] -= session.cart_quantity

        remaining = self.stock[product]
        if remaining < RESTOCK_BELOW:
            self.stock[product] = random.randint(*INITIAL_STOCK_RANGE)
        return remaining


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


SIGNUP_MAX_DAYS_AGO = 1199
INACTIVE_ACCOUNT_ODDS = 25    # 1 account in 25 is inactive


@lru_cache(maxsize=None)
def user_reference_attributes(user_id):
    # The users dim's seeded stream, drawn in this order: device preference,
    # signup offset, account status. Reordering the draws changes every value.
    rng = random.Random(f"users-dim:{user_id}")
    return {
        "device_preference": rng.choice(DEVICES),
        "signup_days_ago": rng.randint(0, SIGNUP_MAX_DAYS_AGO),
        "account_status": "inactive" if rng.randint(1, INACTIVE_ACCOUNT_ODDS) == 1 else "active",
    }


def account_status(user_id):
    return user_reference_attributes(user_id)["account_status"]


def is_product_active(product_id):
    return product_id % 20 != 0


# Products the producer may pick: active ones only, by category.
ACTIVE_PRODUCTS = {
    category: [name for name in names if is_product_active(PRODUCT_IDS[name])]
    for category, names in PRODUCTS.items()
}


def build_event(session, event_type, now, time_since_last_event_ms, inventory_remaining):

    user_id = session.user_id

    profile = get_user_profile(user_id)

    product = session.product_fields(event_type)

    recommendation = session.recommendation_fields(event_type)

    country, city = draw_location(profile)

    session_duration_sec = int(
        (now - session.started_at).total_seconds()
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

    # One product per session, so every product_view so far is of this product.
    repeat_product_view_count = session.product_views

    engagement_score = round(
        (
            time_on_page_sec * 0.4
            + scroll_depth_percent * 0.3
            + repeat_product_view_count * 2
        ) / 10,
        2
    )

    purchase_probability = draw_purchase_probability(event_type)

    return {

        "event_id": fake.uuid4(),

        "session_id": session.session_id,

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


        "event_time": now.isoformat(),

        "event_type": event_type,

        "user_journey_stage":
            session.journey_stage,

        "user_segment":
            profile["user_segment"],

        "is_prime_user":
            profile["is_prime_user"],

        "product_id":
            product["product_id"],

        "product_name":
            product["product_name"],

        "category":
            product["category"],

        "quantity":
            product["quantity"],

        "original_price":
            product["original_price"],

        "discount_percent":
            product["discount_percent"],

        "discounted_price":
            product["discounted_price"],

        "cart_value":
            product["cart_value"],

        "inventory_remaining":
            inventory_remaining,

        "search_query":
            session.search_query(event_type),

        "time_on_page_sec":
            time_on_page_sec,

        "scroll_depth_percent":
            scroll_depth_percent,

        "hover_duration_ms":
            hover_duration_ms,

        "session_duration_sec":
            session_duration_sec,

        "items_viewed_in_session":
            session.product_views,

        "repeat_product_view_count":
            repeat_product_view_count,

        "time_since_last_event_ms":
            time_since_last_event_ms,

        "recommendation_rank":
            recommendation["rank"],

        "recommendation_clicked":
            recommendation["clicked"],

        "recommendation_algorithm":
            recommendation["algorithm"],

        "click_position":
            recommendation["click_position"],

        "engagement_score":
            engagement_score,

        "purchase_probability":
            purchase_probability,

        "cart_abandonment_probability":
            round(random.uniform(0, 1), 4),

        "ab_test_group":
            random.choice(["A", "B", "C"]),

        "payment_method":
            session.payment_method if event_type in PAYMENT_EVENT_TYPES else None,

        "device_type":
            session.device_type,

        "operating_system":
            session.operating_system,

        "browser":
            session.browser,

        "network_type":
            session.network_type,

        "app_version":
            session.app_version,

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
            country,

        "city":
            city,

        "ip_address":
            fake.ipv4(),

        "schema_version": "4.0",

        "source":
            "real-time-personalization-generator"
    }


session_pool = SessionPool()


def generate_event():
    # Next event from a random active session.
    return session_pool.next_event()


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
