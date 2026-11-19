import copy
import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]

VALID_EVENT = {
    "event_id": "valid",
    "session_id": "s-1",
    "user_id": 7,
    "user_name": "Test User",
    "email": "test@example.com",
    "gender": "other",
    "age": 30,
    "membership_tier": "plus",
    "loyalty_points": 100,
    "preferred_language": "english",
    "home_city": "Pune",
    "home_state": "Maharashtra",
    "event_time": "2026-10-25T15:24:46.105673+00:00",
    "event_type": "product_view",
    "user_journey_stage": "discovery",
    "user_segment": "returning_user",
    "is_prime_user": True,
    "product_id": 10000,
    "product_name": "air_fryer",
    "category": "home",
    "quantity": 2,
    "original_price": 1000.0,
    "discount_percent": 10.0,
    "discounted_price": 900.0,
    "cart_value": 1800.0,
    "inventory_remaining": 50,
    "search_query": "iphone",
    "time_on_page_sec": 12.5,
    "scroll_depth_percent": 40,
    "hover_duration_ms": 300,
    "session_duration_sec": 600,
    "items_viewed_in_session": 3,
    "repeat_product_view_count": 1,
    "time_since_last_event_ms": 1000,
    "recommendation_rank": 4,
    "recommendation_clicked": False,
    "recommendation_algorithm": "hybrid_v2",
    "click_position": 2,
    "engagement_score": 5.5,
    "purchase_probability": 0.5,
    "cart_abandonment_probability": 0.2,
    "ab_test_group": "A",
    "payment_method": "upi",
    "device_type": "mobile",
    "operating_system": "android",
    "browser": "chrome",
    "network_type": "wifi",
    "app_version": "3.1",
    "traffic_source": "organic",
    "campaign_id": 1234,
    "api_latency_ms": 120,
    "page_load_time_ms": 800,
    "fraud_score": 0.1,
    "country": "India",
    "city": "Pune",
    "ip_address": "10.0.0.1",
    "schema_version": "4.0",
    "source": "real-time-personalization-generator",
}

# VALID_EVENT's cart fields fit a cart event: 900.0 * 2 = 1800.0.
PAYMENT = {"event_type": "payment_success"}

INVALID_CHANGES = {
    # Required fields and contract.
    "blank_event_id": {"event_id": "   "},
    "null_session_id": {"session_id": None},
    "null_source": {"source": None},
    "user_id_0": {"user_id": 0},
    "unknown_event_type": {"event_type": "teleport"},
    "bad_schema_version": {"schema_version": "3.0"},
    "malformed_event_time": {"event_time": "not-a-timestamp"},
    "event_time_2099": {"event_time": "2099-01-01T00:00:00+00:00"},
    # Scores.
    "probability_1_5": {"purchase_probability": 1.5},
    "null_fraud_score": {"fraud_score": None},
    "negative_engagement_score": {"engagement_score": -0.5},
    "null_engagement_score": {"engagement_score": None},
    # Product and price fields.
    "discounted_gt_original": {"discounted_price": 1200.0},
    "quantity_0": {"quantity": 0},
    "discount_percent_101": {"discount_percent": 101.0},
    "negative_original_price": {"original_price": -1.0},
    "product_view_null_quantity": {"quantity": None},
    "product_view_null_product_name": {"product_name": None},
    "product_view_null_product_id": {"product_id": None},
    "login_negative_price": {"event_type": "login", "discounted_price": -1.0},
    # cart_value.
    "payment_null_cart_value": {**PAYMENT, "cart_value": None},
    "payment_negative_cart_value": {**PAYMENT, "cart_value": -1800.0},
    "payment_mismatched_cart_value": {**PAYMENT, "cart_value": 1800.02},
    "product_view_negative_cart_value": {"cart_value": -1.0},
}

# Raw Kafka values that never parse into an event; each must reach the DLQ as is.
RAW_INVALID_VALUES = {
    "not_json": b"this is not json",
    "empty_value": b"",
    "null_value": None,
}

PRODUCT_AND_PRICE_FIELDS = [
    "product_id",
    "product_name",
    "category",
    "quantity",
    "original_price",
    "discount_percent",
    "discounted_price",
    "cart_value",
]

# Non-commerce events may leave product and price fields NULL; a cart event
# whose cart_value is within rounding of discounted_price * quantity passes, and
# event_time may run ahead of the stream's clock by less than an hour.
VALID_CHANGES = {
    "login_null_product": {
        "event_type": "login",
        **{field: None for field in PRODUCT_AND_PRICE_FIELDS},
    },
    "payment_matching_cart_value": {**PAYMENT},
    "payment_cart_value_within_rounding": {**PAYMENT, "cart_value": 1800.01},
    "product_view_null_cart_value": {"cart_value": None},
    "engagement_score_0": {"engagement_score": 0.0},
    "event_time_30_minutes_ahead": {
        "event_time": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
    },
}


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_stream_validation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def read_stream():
    # Importing must not start Spark or Kafka: the work lives in main().
    spec = importlib.util.spec_from_file_location(
        "read_stream_under_test", ROOT / "spark-streaming" / "read_stream.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def split(spark, read_stream):
    # Each case is identified by name: event_id for JSON events (session_id for
    # the blank-event_id case, whose event_id can't carry it), else the raw value.
    events = [VALID_EVENT]
    for name, change in {**VALID_CHANGES, **INVALID_CHANGES}.items():
        event = copy.deepcopy(VALID_EVENT)
        event["event_id"] = name
        event.update(change)
        if name == "blank_event_id":
            event["session_id"] = name
        events.append(event)

    values = [json.dumps(event).encode("utf-8") for event in events]
    values += list(RAW_INVALID_VALUES.values())
    raw_df = spark.createDataFrame(
        [(None if value is None else bytearray(value),) for value in values],
        "value binary",
    )
    valid_df, invalid_df = read_stream.split_valid_invalid(
        read_stream.build_validated(read_stream.parse_events(raw_df))
    )
    valid_ids = sorted(row.event_id for row in valid_df.collect())
    invalid_ids = sorted(case_name(row.value) for row in invalid_df.collect())
    return values, valid_df, invalid_df, valid_ids, invalid_ids


def case_name(dlq_value):
    for name, raw in RAW_INVALID_VALUES.items():
        if dlq_value == (None if raw is None else raw.decode("utf-8")):
            return name
    event = json.loads(dlq_value)
    return event["session_id"] if event["session_id"] == "blank_event_id" else event["event_id"]


def test_import_has_no_side_effects():
    # Fresh interpreter, so this test's own SparkSession can't hide one.
    code = (
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location('rs', {str(ROOT / 'spark-streaming' / 'read_stream.py')!r})\n"
        "module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)\n"
        "from pyspark import SparkContext\n"
        "assert SparkContext._active_spark_context is None, 'import started Spark'\n"
        "assert callable(module.main)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr


def test_our_signal_handlers_are_installed_after_spark_starts(tmp_path):
    # Fresh interpreter, so main() creates a real local SparkContext, which installs
    # PySpark's own SIGINT handler. setLogLevel is main()'s first call after the
    # session exists; the spy records which handlers are installed at that point
    # and then requests a stop, so main() starts no query and needs no Kafka.
    code = (
        "import importlib.util, json, signal\n"
        "from pyspark import SparkContext\n"
        f"spec = importlib.util.spec_from_file_location('rs', {str(ROOT / 'spark-streaming' / 'read_stream.py')!r})\n"
        "module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)\n"
        "seen = {}\n"
        "original = SparkContext.setLogLevel\n"
        "def spy(self, level):\n"
        "    seen['real_spark_context'] = SparkContext._active_spark_context is self\n"
        "    for name in ('SIGINT', 'SIGTERM'):\n"
        "        handler = signal.getsignal(getattr(signal, name))\n"
        "        seen[name] = getattr(handler, '__qualname__', repr(handler))\n"
        "    module.stop_requested.set()\n"
        "    return original(self, level)\n"
        "SparkContext.setLogLevel = spy\n"
        "module.main()\n"
        "print(json.dumps(seen))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=180,
        env={
            **{k: v for k, v in os.environ.items() if k != "STREAM_STOP_FILE"},
            "PROJECT1_ROOT": str(tmp_path),
        },
    )
    assert result.returncode == 0, result.stderr

    *_, stopped_line, seen_line = result.stdout.strip().splitlines()
    assert stopped_line == "[stream] stopped cleanly"
    assert json.loads(seen_line) == {
        "real_spark_context": True,
        "SIGINT": "request_stop",
        "SIGTERM": "request_stop",
    }


def test_valid_row_passes(split):
    _, _, _, valid_ids, _ = split
    assert valid_ids == sorted(["valid", *VALID_CHANGES])


@pytest.mark.parametrize("name", sorted(INVALID_CHANGES))
def test_invalid_rows_go_to_dlq(split, name):
    _, _, _, _, invalid_ids = split
    assert name in invalid_ids


@pytest.mark.parametrize("name", sorted(RAW_INVALID_VALUES))
def test_unparseable_values_go_to_dlq_unchanged(split, name):
    _, _, _, _, invalid_ids = split
    assert name in invalid_ids


def test_dlq_holds_exactly_the_invalid_cases(split):
    _, _, _, _, invalid_ids = split
    assert invalid_ids == sorted([*INVALID_CHANGES, *RAW_INVALID_VALUES])


def test_every_row_lands_in_exactly_one_branch(split):
    values, _, _, valid_ids, invalid_ids = split
    assert len(valid_ids) + len(invalid_ids) == len(values)
    assert not set(valid_ids) & set(invalid_ids)


def test_bronze_and_dlq_columns_unchanged(split, read_stream):
    _, valid_df, invalid_df, _, _ = split
    expected = [field.name for field in read_stream.event_schema.fields] + ["event_timestamp"]
    assert valid_df.columns == expected
    assert len(valid_df.columns) == 59
    assert invalid_df.columns == ["value"]


def test_kafka_bootstrap_servers_from_env(monkeypatch):
    path = ROOT / "spark-streaming" / "read_stream.py"

    def load():
        spec = importlib.util.spec_from_file_location("read_stream_env_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    assert load().KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.test:19092")
    assert load().KAFKA_BOOTSTRAP_SERVERS == "broker.test:19092"

    # The source and the DLQ sink both use the constant; no other hardcoded address.
    source = path.read_text()
    assert source.count('.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)') == 2
    assert source.count("localhost:9092") == 1


def load_read_stream(monkeypatch, name, stop_file=None):
    # A fresh module per test, so stop_requested starts unset.
    if stop_file is None:
        monkeypatch.delenv("STREAM_STOP_FILE", raising=False)
    else:
        monkeypatch.setenv("STREAM_STOP_FILE", str(stop_file))
    spec = importlib.util.spec_from_file_location(name, ROOT / "spark-streaming" / "read_stream.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeSparkStartingUp:
    """SparkSession stand-in: a stop arrives while Spark starts."""

    def __init__(self, request_stop):
        self.request_stop = request_stop
        self.started = False
        self.stopped = False
        self.sparkContext = self

    # Builder chain.
    @property
    def builder(self):
        return self

    def appName(self, name):
        return self

    def config(self, key, value):
        return self

    def getOrCreate(self):
        self.started = True
        self.request_stop()  # Ctrl+C / SIGTERM / the launcher's stop file, during start-up
        return self

    # Session.
    def setLogLevel(self, level):
        pass

    @property
    def readStream(self):
        raise AssertionError("a query was started after the stop request")

    @property
    def streams(self):
        raise AssertionError("waited for queries after the stop request")

    def stop(self):
        self.stopped = True


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM], ids=["SIGINT", "SIGTERM"])
def test_stop_during_startup_exits_cleanly(monkeypatch, capsys, signum):
    module = load_read_stream(monkeypatch, "read_stream_startup_under_test")
    fake = FakeSparkStartingUp(lambda: os.kill(os.getpid(), signum))
    monkeypatch.setattr(module, "SparkSession", fake)

    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        module.main()  # returns normally: no KeyboardInterrupt, no traceback
        assert signal.getsignal(signum) is module.request_stop
    finally:
        for s, handler in handlers.items():
            signal.signal(s, handler)

    assert module.stop_requested.is_set()
    assert fake.stopped
    assert capsys.readouterr().out.strip() == "[stream] stopped cleanly"


# --- the launcher's stop file (scripts/run_stream.sh writes it on Ctrl+C / SIGTERM) ----


def test_should_stop_only_when_the_stop_file_exists(monkeypatch, tmp_path):
    stop_file = tmp_path / "stop"
    module = load_read_stream(monkeypatch, "read_stream_stop_file", stop_file)
    assert not module.should_stop()  # path set, file not written yet

    stop_file.touch()
    assert module.should_stop()
    assert module.stop_requested.is_set()

    unset = load_read_stream(monkeypatch, "read_stream_no_stop_file")
    assert not unset.should_stop()  # no STREAM_STOP_FILE: signals only


class NoSpark:
    @property
    def builder(self):
        raise AssertionError("Spark was started although the stop file existed")


def test_stop_file_present_at_start_exits_without_starting_spark(monkeypatch, capsys, tmp_path):
    stop_file = tmp_path / "stop"
    stop_file.touch()
    module = load_read_stream(monkeypatch, "read_stream_stop_at_start", stop_file)
    monkeypatch.setattr(module, "SparkSession", NoSpark())

    module.main()

    assert capsys.readouterr().out.strip() == "[stream] stopped cleanly"


def test_stop_file_during_startup_starts_no_query(monkeypatch, capsys, tmp_path):
    stop_file = tmp_path / "stop"
    module = load_read_stream(monkeypatch, "read_stream_stop_file_startup", stop_file)
    fake = FakeSparkStartingUp(stop_file.touch)
    monkeypatch.setattr(module, "SparkSession", fake)

    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        module.main()
    finally:
        for s, handler in handlers.items():
            signal.signal(s, handler)

    assert fake.started and fake.stopped
    assert capsys.readouterr().out.strip() == "[stream] stopped cleanly"


class FakeQuery:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


class FakeRunningSpark(FakeSparkStartingUp):
    """Both queries start; the stop file appears on the second 1-second wait."""

    def __init__(self, stop_file):
        super().__init__(lambda: None)
        self.stop_file = stop_file
        self.waits = 0

    @property
    def readStream(self):
        return self

    def format(self, name):
        return self

    def option(self, key, value):
        return self

    def load(self):
        return "raw"

    @property
    def streams(self):
        return self

    def awaitAnyTermination(self, timeout):
        assert timeout == 1
        self.waits += 1
        if self.waits == 2:
            self.stop_file.touch()
        return False


def test_stop_file_while_running_stops_both_queries(monkeypatch, capsys, tmp_path):
    stop_file = tmp_path / "stop"
    module = load_read_stream(monkeypatch, "read_stream_stop_file_running", stop_file)
    fake = FakeRunningSpark(stop_file)
    monkeypatch.setattr(module, "SparkSession", fake)
    monkeypatch.setattr(module, "parse_events", lambda df: df)
    monkeypatch.setattr(module, "build_validated", lambda df: df)
    monkeypatch.setattr(module, "split_valid_invalid", lambda df: ("valid", "invalid"))
    queries = {"bronze": FakeQuery(), "dlq": FakeQuery()}
    monkeypatch.setattr(module, "start_bronze_query", lambda df: queries["bronze"])
    monkeypatch.setattr(module, "start_dlq_query", lambda df: queries["dlq"])

    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        module.main()
    finally:
        for s, handler in handlers.items():
            signal.signal(s, handler)

    assert fake.waits == 2  # stopped on the first check after the file appeared
    assert queries["bronze"].stopped and queries["dlq"].stopped
    assert fake.stopped
    assert capsys.readouterr().out.strip() == "[stream] stopped cleanly"


# --- scripts/run_stream.sh with a fake spark-submit ------------------------------------

RUN_STREAM = ROOT / "scripts" / "run_stream.sh"

FAKE_SPARK_SUBMIT = """#!/usr/bin/env bash
{
  echo "args=$*"
  echo "stop_file=${STREAM_STOP_FILE:-}"
  if [ -e "${STREAM_STOP_FILE}" ]; then echo "existed_at_start=yes"; else echo "existed_at_start=no"; fi
  grep '^SigIgn' /proc/$$/status
  echo "started"
} > "${FAKE_LOG}"
if [ "${FAKE_MODE}" = "exit" ]; then
  exit "${FAKE_EXIT}"
fi
for _ in $(seq 1 400); do
  if [ -e "${STREAM_STOP_FILE}" ]; then
    echo "saw_stop_file" >> "${FAKE_LOG}"
    exit 0
  fi
  sleep 0.05
done
exit 9
"""


def launcher_env(tmp_path, **extra):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    fake = bin_dir / "spark-submit"
    fake.write_text(FAKE_SPARK_SUBMIT)
    fake.chmod(0o755)
    stop_root = tmp_path / "tmp"
    stop_root.mkdir(exist_ok=True)
    env = {k: v for k, v in os.environ.items() if k != "STREAM_STOP_FILE"}
    # The launcher repairs the checkpoints under PROJECT1_ROOT before spark-submit:
    # never let a test touch the repository's real checkpoints.
    project_root = tmp_path / "project"
    project_root.mkdir(exist_ok=True)
    env.update(
        PATH=f"{bin_dir}:{env['PATH']}",
        TMPDIR=str(stop_root),
        PROJECT1_ROOT=str(project_root),
        **extra,
    )
    return env, stop_root


def fake_log(path):
    text = path.read_text()
    fields = {}
    for line in text.splitlines():
        if line.startswith("SigIgn:"):
            fields["SigIgn"] = line.split(":", 1)[1]
        elif "=" in line:
            key, value = line.split("=", 1)
            fields[key] = value
    return fields, text


def test_launcher_is_valid_bash():
    result = subprocess.run(["bash", "-n", str(RUN_STREAM)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_launcher_gives_each_run_a_fresh_stop_path_and_returns_spark_submit_status(tmp_path):
    paths = []
    for run, exit_code in enumerate((3, 0)):
        log = tmp_path / f"fake{run}.log"
        env, stop_root = launcher_env(
            tmp_path, FAKE_LOG=str(log), FAKE_MODE="exit", FAKE_EXIT=str(exit_code)
        )
        result = subprocess.run(
            [str(RUN_STREAM)], env=env, capture_output=True, text=True, timeout=30
        )

        assert result.returncode == exit_code, result.stderr
        fields, _ = fake_log(log)
        assert "spark-streaming/read_stream.py" in fields["args"]
        assert "spark-sql-kafka-0-10_2.13:4.1.2" in fields["args"]
        assert fields["stop_file"].startswith(str(stop_root))
        assert fields["existed_at_start"] == "no"
        assert list(stop_root.iterdir()) == []  # removed at exit
        paths.append(fields["stop_file"])
    assert paths[0] != paths[1]


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM], ids=["SIGINT", "SIGTERM"])
def test_launcher_turns_a_group_signal_into_the_stop_file(tmp_path, signum):
    # The group signal arrives while "spark-submit" is starting (no driver yet).
    log = tmp_path / "fake.log"
    env, stop_root = launcher_env(tmp_path, FAKE_LOG=str(log), FAKE_MODE="wait")
    proc = subprocess.Popen(
        [str(RUN_STREAM)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 20
        while not (log.exists() and "started" in log.read_text()):
            assert time.monotonic() < deadline, "fake spark-submit never started"
            time.sleep(0.05)
        os.killpg(proc.pid, signum)
        os.killpg(proc.pid, signum)  # a second signal: the launcher keeps waiting
        _, stderr = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)

    fields, text = fake_log(log)
    assert proc.returncode == 0, stderr
    assert "saw_stop_file" in text
    ignored = int(fields["SigIgn"].strip(), 16)
    assert ignored & (1 << (signal.SIGINT - 1)), "spark-submit must ignore SIGINT"
    assert ignored & (1 << (signal.SIGTERM - 1)), "spark-submit must ignore SIGTERM"
    assert list(stop_root.iterdir()) == []


# --- STREAM_MAX_OFFSETS_PER_TRIGGER ----------------------------------------------------


class OptionRecordingSpark(FakeRunningSpark):
    """Records the Kafka source options; stops on the first wait."""

    def __init__(self, stop_file):
        super().__init__(stop_file)
        self.options = {}

    def option(self, key, value):
        self.options[key] = value
        return self

    def awaitAnyTermination(self, timeout):
        self.stop_file.touch()
        return False


@pytest.mark.parametrize(("value", "expected"), [("", None), ("300000", "300000")])
def test_max_offsets_per_trigger_only_when_set(monkeypatch, capsys, tmp_path, value, expected):
    monkeypatch.setenv("STREAM_MAX_OFFSETS_PER_TRIGGER", value)
    stop_file = tmp_path / "stop"
    module = load_read_stream(monkeypatch, "read_stream_max_offsets", stop_file)
    fake = OptionRecordingSpark(stop_file)
    monkeypatch.setattr(module, "SparkSession", fake)
    monkeypatch.setattr(module, "parse_events", lambda df: df)
    monkeypatch.setattr(module, "build_validated", lambda df: df)
    monkeypatch.setattr(module, "split_valid_invalid", lambda df: ("valid", "invalid"))
    monkeypatch.setattr(module, "start_bronze_query", lambda df: FakeQuery())
    monkeypatch.setattr(module, "start_dlq_query", lambda df: FakeQuery())

    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        module.main()
    finally:
        for s, handler in handlers.items():
            signal.signal(s, handler)

    assert fake.options.get("maxOffsetsPerTrigger") == expected
    assert fake.options["subscribe"] == "user_events"
    assert fake.options["startingOffsets"] == "earliest"
    assert capsys.readouterr().out.strip() == "[stream] stopped cleanly"


@pytest.mark.parametrize("value", ["0", "-5", "1e5", "lots"])
def test_bad_max_offsets_per_trigger_stops_before_spark(monkeypatch, tmp_path, value):
    monkeypatch.setenv("STREAM_MAX_OFFSETS_PER_TRIGGER", value)
    module = load_read_stream(monkeypatch, "read_stream_bad_max_offsets", tmp_path / "stop")
    monkeypatch.setattr(module, "SparkSession", NoSpark())

    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        with pytest.raises(SystemExit) as stop:
            module.main()
    finally:
        for s, handler in handlers.items():
            signal.signal(s, handler)

    assert str(stop.value) == (
        f"[stream] error: STREAM_MAX_OFFSETS_PER_TRIGGER must be a positive integer, got {value!r}"
    )
