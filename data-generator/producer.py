import csv
import json
import math
import os
import random
import signal
import sys
import time
import uuid
from collections.abc import Callable
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

from confluent_kafka import Producer

# reference-data/ resolves from PROJECT1_ROOT (default: the repository root),
# like the batch jobs, so the producer works from any directory.
PROJECT_ROOT = os.environ.get("PROJECT1_ROOT", str(Path(__file__).resolve().parents[1]))

# Read-only export of the batch lakehouse's warehouse (Batch-Lakehouse-Marketing-Analytics):
# the current SCD2 rows of warehouse.dim_customer and warehouse.dim_product.
# SHOP_MASTER_DIR picks another export (reference-data/export_shop_master.py
# writes one): a relative path resolves from PROJECT1_ROOT, an absolute one is
# used as is. Unset or empty means the committed reference-data/shop_master.
DEFAULT_SHOP_MASTER_DIR = "reference-data/shop_master"
SHOP_MASTER_DIR = os.environ.get("SHOP_MASTER_DIR") or DEFAULT_SHOP_MASTER_DIR

# Optional cap: only the first NUM_USERS customers of customers.csv shop.
# Unset or empty means every customer.
NUM_USERS = int(os.environ["NUM_USERS"]) if os.environ.get("NUM_USERS") else None

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

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
    "logout",
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
    "search_result_click",
]

# A search uses a query that fits the session's category. One list per
# batch-lakehouse category; a category in products.csv without a list is a load error.
CATEGORY_SEARCH_QUERIES = {
    "Electronics": [
        "wireless earbuds",
        "bluetooth speaker",
        "gaming headset",
        "mechanical keyboard",
        "portable ssd 1tb",
        "power bank 20000mah",
        "usb c hub",
        "webcam for laptop",
        "smart watch",
        "wireless mouse",
        "laptop stand",
    ],
    "Fashion": [
        "running shoes",
        "white sneakers",
        "cotton kurta",
        "denim jacket",
        "formal shirt",
        "leather wallet",
        "analog watch",
        "sunglasses",
        "backpack",
        "canvas tote bag",
        "rain jacket",
        "silk scarf",
    ],
    "Home": [
        "air purifier",
        "cotton bedsheet",
        "dinner set",
        "memory foam pillow",
        "nonstick cookware set",
        "office chair",
        "study table",
        "table lamp",
        "wall clock",
        "curtains",
        "steel water bottle",
        "storage organizer",
    ],
    "Beauty": [
        "face wash",
        "face serum",
        "sunscreen spf 50",
        "lip balm",
        "kajal",
        "hair oil",
        "shampoo",
        "night cream",
        "sheet mask",
        "beard oil",
        "perfume",
        "body lotion",
    ],
    "Fitness": [
        "yoga mat",
        "kettlebell",
        "resistance bands",
        "skipping rope",
        "foam roller",
        "fitness tracker",
        "protein shaker",
        "gym bag",
        "push up bars",
        "exercise ball",
        "cycling gloves",
    ],
    "Books": [
        "python book",
        "personal finance book",
        "vegetarian cookbook",
        "english dictionary",
        "atlas of india",
        "world history book",
        "short stories",
        "sudoku book",
        "daily planner",
        "yoga book",
        "maths workbook",
        "children's encyclopedia",
    ],
}

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "gender",
    "age",
    "membership_tier",
    "loyalty_points",
    "preferred_language",
    "customer_segment",
    "is_prime_user",
    "home_city",
    "home_state",
    "country",
    "signup_date",
]

PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "product_status",
    "original_price",
]

PRODUCT_STATUSES = {"active", "discontinued"}


class ShopDataError(Exception):
    """A shop_master CSV is missing, lacks a required column or has a bad value."""


def parse_flag(value):
    # The batch lakehouse exports booleans as PostgreSQL text: "t" / "f".
    if value not in ("t", "f"):
        raise ValueError(f"expected 't' or 'f', got {value!r}")
    return value == "t"


def parse_customer(row):
    return {
        "customer_id": int(row["customer_id"]),
        # The event and reference-data names for the batch lakehouse's columns.
        "profile": {
            "user_name": row["customer_name"],
            "email": row["email"],
            "gender": row["gender"],
            "age": int(row["age"]),
            "membership_tier": row["membership_tier"],
            "loyalty_points": int(row["loyalty_points"]),
            "preferred_language": row["preferred_language"],
            "home_city": row["home_city"],
            "home_state": row["home_state"],
            "user_segment": row["customer_segment"],
            "is_prime_user": parse_flag(row["is_prime_user"]),
            "country": row["country"],
        },
        "signup_date": date.fromisoformat(row["signup_date"]),
    }


def parse_product(row):
    if row["product_status"] not in PRODUCT_STATUSES:
        raise ValueError(f"unknown product_status {row['product_status']!r}")
    if row["category"] not in CATEGORY_SEARCH_QUERIES:
        raise ValueError(f"category {row['category']!r} has no CATEGORY_SEARCH_QUERIES entry")
    return {
        "product_id": int(row["product_id"]),
        "product_name": row["product_name"],
        "category": row["category"],
        "brand": row["brand"],
        "is_active": row["product_status"] == "active",
        "original_price": float(row["original_price"]),
    }


def read_shop_csv(root, file_name, required_columns, parse_row, key):
    path = os.path.join(root, SHOP_MASTER_DIR, file_name)
    if not os.path.isfile(path):
        if SHOP_MASTER_DIR == DEFAULT_SHOP_MASTER_DIR:
            hint = "set PROJECT1_ROOT to the repository root"
        else:
            hint = "check SHOP_MASTER_DIR"
        raise ShopDataError(f"{path} not found ({hint})")

    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        missing = [column for column in required_columns if column not in (reader.fieldnames or [])]
        if missing:
            raise ShopDataError(f"{path} lacks required column(s): {', '.join(missing)}")

        rows = []
        seen = set()
        for row in reader:
            try:
                parsed = parse_row(row)
            except (TypeError, ValueError) as exc:
                raise ShopDataError(f"{path} line {reader.line_num}: {exc}") from None
            if parsed[key] in seen:
                raise ShopDataError(f"{path} line {reader.line_num}: duplicate {key} {parsed[key]}")
            seen.add(parsed[key])
            rows.append(parsed)

    if not rows:
        raise ShopDataError(f"{path} has no rows")
    return rows


def load_shop_master(root):
    """(customers, products) from the batch lakehouse's shop_master export, in file order."""
    customers = read_shop_csv(
        root, "customers.csv", CUSTOMER_COLUMNS, parse_customer, "customer_id"
    )
    products = read_shop_csv(root, "products.csv", PRODUCT_COLUMNS, parse_product, "product_id")
    if len({product["product_name"] for product in products}) != len(products):
        raise ShopDataError("products.csv: product_name must be unique")
    return customers, products


# Loaded once, at import. A missing file or column stops the producer (and
# create_reference_data.py, which imports this module) with exit code 2.
try:
    SHOP_CUSTOMERS, SHOP_PRODUCTS = load_shop_master(PROJECT_ROOT)
except ShopDataError as exc:
    print(f"[producer] error: {exc}", file=sys.stderr, flush=True)
    sys.exit(2)

# Users are the batch lakehouse's customers. IDs are not contiguous, so sessions
# pick from this list (file order), never from a numeric range.
USER_IDS = [customer["customer_id"] for customer in SHOP_CUSTOMERS]
CUSTOMERS_BY_ID = {customer["customer_id"]: customer for customer in SHOP_CUSTOMERS}

# Catalogue, keyed by product_name (unique in products.csv). PRODUCTS lists
# discontinued products too; only ACTIVE_PRODUCTS (below) are ever produced.
PRODUCTS = {}
for _product in SHOP_PRODUCTS:
    PRODUCTS.setdefault(_product["category"], []).append(_product["product_name"])

PRODUCT_CATEGORIES = list(PRODUCTS)

PRODUCT_IDS = {product["product_name"]: product["product_id"] for product in SHOP_PRODUCTS}

# List price per product (implicit currency INR).
PRODUCT_REFERENCE_PRICES = {
    product["product_name"]: product["original_price"] for product in SHOP_PRODUCTS
}

PRODUCT_BRAND_BY_NAME = {product["product_name"]: product["brand"] for product in SHOP_PRODUCTS}

PRODUCT_ACTIVE_BY_ID = {product["product_id"]: product["is_active"] for product in SHOP_PRODUCTS}

# Lowest and highest list price per category.
CATEGORY_PRICE_BANDS = {
    category: (
        min(PRODUCT_REFERENCE_PRICES[name] for name in names),
        max(PRODUCT_REFERENCE_PRICES[name] for name in names),
    )
    for category, names in PRODUCTS.items()
}

# Cities the shop's customers live in; an other-city event uses one of these.
SHOP_CITIES = sorted({customer["profile"]["home_city"] for customer in SHOP_CUSTOMERS})

PAYMENT_METHODS = ["upi", "credit_card", "debit_card", "wallet", "cash_on_delivery", "net_banking"]

DEVICES = ["mobile", "desktop", "tablet"]

OPERATING_SYSTEMS = ["android", "ios", "windows", "macos", "linux"]

BROWSERS = ["chrome", "firefox", "edge", "safari"]

NETWORK_TYPES = ["wifi", "4g", "5g", "ethernet"]

TRAFFIC_SOURCES = [
    "organic",
    "google_ads",
    "instagram_ads",
    "facebook_ads",
    "affiliate",
    "email_campaign",
    "push_notification",
]

RECOMMENDATION_ALGORITHMS = [
    "collaborative_filtering",
    "content_based",
    "deep_learning",
    "trending",
    "hybrid_v2",
]

USER_JOURNEY_STAGES = ["discovery", "comparison", "consideration", "checkout", "post_purchase"]


MAX_ACTIVE_SESSIONS = 20

# Funnel step probabilities; each step is one event.
LOGIN_ENTRY_RATE = 0.30  # otherwise home_page_view
AD_IMPRESSION_RATE = 0.20  # after home_page_view
AD_CLICK_RATE = 0.25  # of ad impressions
SEARCH_RATE = 0.50
RECOMMENDATION_BEFORE_VIEW_RATE = 0.25
IMAGE_ZOOM_RATE = 0.20  # after a product_view
WISHLIST_RATE = 0.05  # after a product_view
WISHLIST_EXIT_RATE = 0.50  # session leaves the funnel after wishlist_add
ADD_TO_CART_RATE = 0.18  # of sessions that viewed a product
REMOVE_FROM_CART_RATE = 0.10  # of carts; no checkout afterwards
CHECKOUT_RATE = 0.60  # of carts kept
COUPON_RATE = 0.20  # of checkouts
PAYMENT_SUCCESS_RATE = 0.85  # otherwise payment_failed
FEEDBACK_RATE = 0.05  # review or rating after order_placed
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

# Cart events: the cart quantity applies and cart_value is set. Must match
# CART_EVENT_TYPES in spark-streaming/read_stream.py.
CART_EVENT_TYPES = [
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "payment_initiated",
    "payment_success",
    "payment_failed",
    "order_placed",
]


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

HOME_COUNTRY = "India"  # the batch lakehouse's shop and every customer in customers.csv
ABROAD_RATE = 0.05  # events from a random other country
HOME_CITY_RATE = 0.85  # events from the profile's home_city

# (country, city) for events from abroad; never HOME_COUNTRY.
ABROAD_LOCATIONS = [
    ("United Arab Emirates", "Dubai"),
    ("United States", "New York"),
    ("United Kingdom", "London"),
    ("Singapore", "Singapore"),
    ("Saudi Arabia", "Riyadh"),
    ("Qatar", "Doha"),
    ("Canada", "Toronto"),
    ("Australia", "Sydney"),
    ("Germany", "Berlin"),
    ("Nepal", "Kathmandu"),
    ("Japan", "Tokyo"),
    ("France", "Paris"),
]

INITIAL_STOCK_RANGE = (50, 500)
RESTOCK_BELOW = 5

MAX_INT32 = 2_147_483_647  # Bronze stores time_since_last_event_ms as a 32-bit int


def draw_purchase_probability(event_type):
    score = FUNNEL_STAGE_SCORES.get(event_type, DEFAULT_STAGE_SCORE)
    score += random.gauss(0, STAGE_SCORE_NOISE)
    probability = 1 / (1 + math.exp(-score))
    # Rounded to 4 decimals but kept strictly between 0 and 1.
    return round(min(max(probability, 0.0001), 0.9999), 4)


def draw_location(profile):
    home_city = profile["home_city"]

    if random.random() < ABROAD_RATE:
        # Another country, and never a city named like the home city.
        return random.choice(
            [
                (country, city)
                for country, city in ABROAD_LOCATIONS
                if country != HOME_COUNTRY and city != home_city
            ]
        )

    # Share of the at-home events that use home_city, so HOME_CITY_RATE holds overall.
    if random.random() < HOME_CITY_RATE / (1 - ABROAD_RATE):
        return HOME_COUNTRY, home_city
    # Another city where the shop has customers.
    return HOME_COUNTRY, random.choice([city for city in SHOP_CITIES if city != home_city])


def draw_discount_percent():
    # Mostly 0-15%, sometimes 15-40%, never above 50%.
    if random.random() < 0.85:
        return round(random.uniform(0, 15), 2)
    return round(random.uniform(15, 40), 2)


class Session:
    """One user, one product and one price for every event in the session."""

    def __init__(self, user_id):
        self.user_id = user_id
        self.session_id = str(uuid.uuid4())
        self.category = random.choice(
            [category for category in PRODUCT_CATEGORIES if ACTIVE_PRODUCTS[category]]
        )
        self.product_name = random.choice(ACTIVE_PRODUCTS[self.category])

        # The shop's list price from products.csv; the session's discount varies.
        self.original_price = PRODUCT_REFERENCE_PRICES[self.product_name]
        self.discount_percent = draw_discount_percent()
        self.discounted_price = round(
            self.original_price - self.original_price * self.discount_percent / 100, 2
        )

        self.cart_quantity = None
        self.steps = plan_session_steps()
        self.position = 0

        self.started_at = None  # time of the session's first event
        self.product_views = 0  # product_view events so far, this one included
        self.recommendation = None  # (rank, algorithm) shown by the last impression
        self.journey_stage = None  # stage of the latest event with a stage

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

        in_cart = event_type in CART_EVENT_TYPES
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


class UserPlan(NamedTuple):
    """A run with a session target (PRODUCER_ONE_PASS=1); see SessionPool."""

    seed: int
    shard_index: int
    shard_count: int
    total_sessions: int  # for all shards together


class SessionPool:
    """Up to max_active concurrent sessions; a finished one is replaced.

    Without a plan (the default) the pool runs until stopped, and each new
    session goes to a random free active user. With a plan, the active users
    are shuffled by plan.seed and shard i of n takes every n-th of them; the
    shard first gives each of its users exactly one session, in that order,
    then starts extra sessions with the default rule among its own users until
    its share of plan.total_sessions has started, and stops once every session
    has finished (next_event returns None). A plan also seeds the event draws,
    so the same plan gives the same users, sessions and draws.
    """

    def __init__(self, max_active=MAX_ACTIVE_SESSIONS, num_users=None, plan=None):
        # num_users caps the users to the first N customers; None means all of them.
        self.num_users = NUM_USERS if num_users is None else num_users
        if self.num_users is not None and self.num_users < 1:
            raise ValueError(f"NUM_USERS must be at least 1, got {self.num_users}")

        # Customer IDs from customers.csv (not contiguous), active accounts only.
        self.active_user_ids = [
            user_id for user_id in USER_IDS[: self.num_users] if account_status(user_id) == "active"
        ]
        if not self.active_user_ids:
            scope = "customers.csv" if self.num_users is None else f"NUM_USERS={self.num_users}"
            raise ValueError(f"{scope} has no active accounts; use a larger NUM_USERS")

        self.plan = plan
        self.sessions_target = None  # None: run until stopped
        # The users this pool picks from; positions in this list identify them.
        self.shard_user_ids = self.active_user_ids
        if plan is not None:
            self.shard_user_ids, self.sessions_target = shard_of(self.active_user_ids, plan)
            # Every shard its own stream, the same for the same plan.
            random.seed(f"producer-events:{plan.seed}:{plan.shard_index}/{plan.shard_count}")

        # One session per active user at a time, so never more sessions than
        # active users; otherwise start_session could never find a free user.
        self.max_active = min(max_active, len(self.shard_user_ids))
        self.active = []
        self.sessions_started = 0
        self.sessions_completed = 0
        self.last_event_at = {}  # user_id -> time of that user's previous event
        self.stock = {}  # product_name -> units in stock

    @property
    def target_reached(self):
        return self.sessions_target is not None and self.sessions_started >= self.sessions_target

    def start_session(self):
        # Active accounts only, one session per user at a time.
        users = self.shard_user_ids
        if self.plan is not None and self.sessions_started < len(users):
            position = self.sessions_started  # the pass: each user once, in shard order
        else:
            position = self.pick_free_position(users)
        session = Session(users[position])
        session.user_position = position
        self.active.append(session)
        self.sessions_started += 1

    def pick_free_position(self, users):
        # A random free user, drawn exactly like random.choice over the free
        # users in list order (one randrange over their count), without building
        # that list: step over the busy positions, at most max_active of them.
        busy = sorted(session.user_position for session in self.active)
        position = random.randrange(len(users) - len(busy))
        for taken in busy:
            if taken > position:
                break
            position += 1
        return position

    def next_event(self):
        """The next event, or None once a plan's sessions have all finished."""
        while len(self.active) < self.max_active and not self.target_reached:
            self.start_session()
        if not self.active:
            return None

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


def shard_of(active_user_ids, plan):
    """(this shard's users in pass order, this shard's session target) for a plan."""
    # The same shuffle in every process of the run, so shards never share a user.
    order = list(active_user_ids)
    random.Random(f"user-cycle:{plan.seed}").shuffle(order)
    users = order[plan.shard_index :: plan.shard_count]
    shard = f"{plan.shard_index}/{plan.shard_count}"
    if not users:
        raise ValueError(f"shard {shard} has no active users; use fewer shards")

    # The total split evenly; the first shards take the remainder.
    target = plan.total_sessions // plan.shard_count + (
        plan.shard_index < plan.total_sessions % plan.shard_count
    )
    if target < len(users):
        raise ValueError(
            f"PRODUCER_TOTAL_SESSIONS={plan.total_sessions} gives shard {shard} "
            f"{target} sessions, fewer than its {len(users)} active users"
        )
    return users, target


def get_user_profile(user_id):
    # The customer's customers.csv row under the event's field names, plus
    # country. A copy, so a caller can't change the loaded data. KeyError for
    # an ID that is not in customers.csv.
    return dict(CUSTOMERS_BY_ID[user_id]["profile"])


SIGNUP_MAX_DAYS_AGO = 1199
INACTIVE_ACCOUNT_ODDS = 25  # 1 account in 25 is inactive


@lru_cache(maxsize=None)
def user_reference_attributes(user_id):
    # The users dim's seeded stream, drawn in this order: device preference,
    # a former signup offset, account status. The offset is still drawn,
    # though signup_date now comes from customers.csv, so every customer keeps
    # its device preference and account status. Reordering the draws changes them.
    rng = random.Random(f"users-dim:{user_id}")
    device_preference = rng.choice(DEVICES)
    rng.randint(0, SIGNUP_MAX_DAYS_AGO)
    inactive = rng.randint(1, INACTIVE_ACCOUNT_ODDS) == 1
    return {
        "device_preference": device_preference,
        "signup_date": CUSTOMERS_BY_ID[user_id]["signup_date"],
        "account_status": "inactive" if inactive else "active",
    }


def account_status(user_id):
    return user_reference_attributes(user_id)["account_status"]


def is_product_active(product_id):
    # products.csv product_status == "active".
    return PRODUCT_ACTIVE_BY_ID[product_id]


def draw_ip_address():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


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

    session_duration_sec = int((now - session.started_at).total_seconds())

    time_on_page_sec = round(random.uniform(1, 900), 2)

    scroll_depth_percent = random.randint(0, 100)

    hover_duration_ms = random.randint(0, 20000)

    # One product per session, so every product_view so far is of this product.
    repeat_product_view_count = session.product_views

    engagement_score = round(
        (time_on_page_sec * 0.4 + scroll_depth_percent * 0.3 + repeat_product_view_count * 2) / 10,
        2,
    )

    purchase_probability = draw_purchase_probability(event_type)

    return {
        "event_id": str(uuid.uuid4()),
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
        "user_journey_stage": session.journey_stage,
        "user_segment": profile["user_segment"],
        "is_prime_user": profile["is_prime_user"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": product["quantity"],
        "original_price": product["original_price"],
        "discount_percent": product["discount_percent"],
        "discounted_price": product["discounted_price"],
        "cart_value": product["cart_value"],
        "inventory_remaining": inventory_remaining,
        "search_query": session.search_query(event_type),
        "time_on_page_sec": time_on_page_sec,
        "scroll_depth_percent": scroll_depth_percent,
        "hover_duration_ms": hover_duration_ms,
        "session_duration_sec": session_duration_sec,
        "items_viewed_in_session": session.product_views,
        "repeat_product_view_count": repeat_product_view_count,
        "time_since_last_event_ms": time_since_last_event_ms,
        "recommendation_rank": recommendation["rank"],
        "recommendation_clicked": recommendation["clicked"],
        "recommendation_algorithm": recommendation["algorithm"],
        "click_position": recommendation["click_position"],
        "engagement_score": engagement_score,
        "purchase_probability": purchase_probability,
        "cart_abandonment_probability": round(random.uniform(0, 1), 4),
        "ab_test_group": random.choice(["A", "B", "C"]),
        "payment_method": session.payment_method if event_type in PAYMENT_EVENT_TYPES else None,
        "device_type": session.device_type,
        "operating_system": session.operating_system,
        "browser": session.browser,
        "network_type": session.network_type,
        "app_version": session.app_version,
        "traffic_source": random.choice(TRAFFIC_SOURCES),
        "campaign_id": random.randint(1000, 9999),
        "api_latency_ms": random.randint(20, 2000),
        "page_load_time_ms": random.randint(100, 8000),
        "fraud_score": round(random.uniform(0, 1), 4),
        "country": country,
        "city": city,
        "ip_address": draw_ip_address(),
        "schema_version": "4.0",
        "source": "real-time-personalization-generator",
    }


# Created on first use, so importing this module never validates NUM_USERS.
_default_pool = None


def generate_event():
    # Next event from a random active session of the shared pool.
    global _default_pool
    if _default_pool is None:
        _default_pool = SessionPool()
    return _default_pool.next_event()


def on_delivery(err, msg):
    if err is not None:
        delivery_stats["failed"] += 1
        print(
            f"[producer] delivery failed: topic={msg.topic()} error={err}",
            file=sys.stderr,
            flush=True,
        )
    else:
        delivery_stats["delivered"] += 1


def produce_event(producer, event):
    value = json.dumps(event).encode("utf-8")

    # Keyed by user_id so each user's events stay in order on one partition.
    key = str(event["user_id"])

    try:
        producer.produce("user_events", key=key, value=value, on_delivery=on_delivery)
    except BufferError:
        # Local queue full: serve callbacks for up to 1s, then retry once.
        producer.poll(1)
        producer.produce("user_events", key=key, value=value, on_delivery=on_delivery)

    # Serve delivery callbacks without blocking.
    producer.poll(0)


def producer_config():
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Idempotent producer: the broker drops retried duplicates and keeps
        # each partition in send order, so a retry can neither duplicate nor
        # reorder a user's events (one user = one key = one partition).
        "enable.idempotence": True,
    }


def stop_on_first_signal(signum, frame):
    # The first SIGINT or SIGTERM stops the producer. Later ones are ignored for
    # the rest of the shutdown (GNU timeout signals the child and then its
    # process group; a user may press Ctrl+C twice), so the final flush, which
    # FLUSH_TIMEOUT_SECONDS bounds, and the summary always run. Ignoring them
    # here, before raising, also drops a second signal that is already pending.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise KeyboardInterrupt


def install_signal_handlers():
    # SIGTERM stops the producer exactly like Ctrl+C.
    signal.signal(signal.SIGINT, stop_on_first_signal)
    signal.signal(signal.SIGTERM, stop_on_first_signal)


def pause_between_events():
    time.sleep(random.uniform(0.05, 1.5))


def fixed_pause(seconds):
    def pause():
        if seconds > 0:
            time.sleep(seconds)

    return pause


class RunSettings(NamedTuple):
    plan: UserPlan | None
    pause: Callable[[], None]
    quiet: bool


# Settings that only apply with PRODUCER_ONE_PASS=1.
PLAN_SETTINGS = ["PRODUCER_SEED", "PRODUCER_TOTAL_SESSIONS", "PRODUCER_SHARD"]


def read_flag(environ, name):
    value = environ.get(name, "")
    if value not in ("", "0", "1"):
        raise ValueError(f"{name} must be 0 or 1, got {value!r}")
    return value == "1"


def read_int(environ, name, minimum):
    value = environ.get(name, "")
    if value == "":
        raise ValueError(f"{name} is required with PRODUCER_ONE_PASS=1")
    try:
        number = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None
    if number < minimum:
        raise ValueError(f"{name} must be at least {minimum}, got {number}")
    return number


def read_shard(environ):
    value = environ.get("PRODUCER_SHARD", "") or "0/1"
    index, _, count = value.partition("/")
    if not (index.isdigit() and count.isdigit() and int(index) < int(count)):
        raise ValueError(f"PRODUCER_SHARD must be i/n with 0 <= i < n, got {value!r}")
    return int(index), int(count)


def read_settings(environ):
    """Run settings from the environment; all unset gives today's endless run.

    PRODUCER_ONE_PASS=1 with PRODUCER_SEED, PRODUCER_TOTAL_SESSIONS and optionally
    PRODUCER_SHARD=i/n (default 0/1) runs to a session target (see SessionPool).
    PRODUCER_PAUSE_SECONDS replaces the random pause between events (0: none).
    PRODUCER_QUIET=1 prints no events; the stderr summary is unchanged.
    """
    plan = None
    if read_flag(environ, "PRODUCER_ONE_PASS"):
        shard_index, shard_count = read_shard(environ)
        plan = UserPlan(
            seed=read_int(environ, "PRODUCER_SEED", -(2**63)),
            shard_index=shard_index,
            shard_count=shard_count,
            total_sessions=read_int(environ, "PRODUCER_TOTAL_SESSIONS", 1),
        )
    else:
        for name in PLAN_SETTINGS:
            if environ.get(name, ""):
                raise ValueError(f"{name} needs PRODUCER_ONE_PASS=1")

    pause = pause_between_events
    seconds = environ.get("PRODUCER_PAUSE_SECONDS", "")
    if seconds != "":
        try:
            value = float(seconds)
        except ValueError:
            value = math.nan
        if not (math.isfinite(value) and value >= 0):
            raise ValueError(
                f"PRODUCER_PAUSE_SECONDS must be a number of seconds >= 0, got {seconds!r}"
            )
        pause = fixed_pause(value)

    return RunSettings(plan=plan, pause=pause, quiet=read_flag(environ, "PRODUCER_QUIET"))


PROGRESS_EVERY = 100_000  # events, for a run with a session target


def exit_status(undelivered):
    # 1 if any event failed or was still queued after the final flush.
    return 1 if delivery_stats["failed"] or undelivered else 0


def run(producer, pool, pause=pause_between_events, quiet=False):
    """Produce until Ctrl+C, SIGTERM or the pool's session target, flush, print the
    summary, return the exit status."""
    events = 0
    try:
        while True:
            event = pool.next_event()
            if event is None:  # a plan's sessions have all finished
                break

            produce_event(producer, event)
            events += 1

            if not quiet:
                print(json.dumps(event, indent=2))
            if pool.plan is not None and events % PROGRESS_EVERY == 0:
                print(
                    f"[producer] progress: events={events} sessions_started="
                    f"{pool.sessions_started}/{pool.sessions_target}",
                    file=sys.stderr,
                    flush=True,
                )

            pause()

    except KeyboardInterrupt:
        pass

    finally:
        undelivered = producer.flush(FLUSH_TIMEOUT_SECONDS)
        if pool.plan is not None:
            print(
                f"[producer] sessions: shard={pool.plan.shard_index}/{pool.plan.shard_count} "
                f"users={len(pool.shard_user_ids)} started={pool.sessions_started} "
                f"completed={pool.sessions_completed} target={pool.sessions_target}",
                file=sys.stderr,
                flush=True,
            )
        print(
            f"[producer] summary: delivered={delivery_stats['delivered']} "
            f"failed={delivery_stats['failed']} undelivered={undelivered}",
            file=sys.stderr,
            flush=True,
        )

    return exit_status(undelivered)


def main():
    install_signal_handlers()

    try:
        settings = read_settings(os.environ)
        pool = SessionPool(plan=settings.plan)
    except ValueError as exc:
        print(f"[producer] error: {exc}", file=sys.stderr, flush=True)
        return 2

    return run(Producer(producer_config()), pool, pause=settings.pause, quiet=settings.quiet)


if __name__ == "__main__":
    sys.exit(main())
