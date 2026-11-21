"""reference-data/check_bronze_against_export.py and where the DAG runs it."""

import ast
import csv
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "reference-data" / "check_bronze_against_export.py"
DAG = ROOT / "dags" / "project1_personalization_pipeline.py"

# The committed export, where the producer looks for it under PROJECT1_ROOT.
SHOP_MASTER = Path("reference-data") / "shop_master"

# A real customer of the committed export (IDs 1-3 don't exist) and ids it lacks.
CUSTOMER = 4
NOT_CUSTOMERS = (999998, 999999)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_bronze_export_check")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_root(spark, root, user_ids):
    """A PROJECT1_ROOT with the committed shop CSVs and a Bronze of these user_ids."""
    shutil.copytree(ROOT / SHOP_MASTER, root / SHOP_MASTER)
    bronze = spark.createDataFrame([(user_id,) for user_id in user_ids], "user_id int")
    bronze.write.parquet(str(root / "data" / "bronze" / "user_events"))
    return root


def run_check(root, cwd, **env):
    clean = {
        key: value
        for key, value in os.environ.items()
        if key not in ("SHOP_MASTER_DIR", "NUM_USERS", "PROJECT1_ROOT")
    }
    return subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=cwd,
        env={**clean, "PROJECT1_ROOT": str(root), **env},
        capture_output=True,
        text=True,
        timeout=600,
    )


def committed_customer_count():
    with open(ROOT / SHOP_MASTER / "customers.csv", newline="", encoding="utf-8") as file:
        return sum(1 for _ in csv.DictReader(file))


def tree(root):
    return sorted(str(path.relative_to(root)) for path in root.rglob("*"))


def test_passes_when_every_bronze_user_is_a_customer(spark, tmp_path):
    root = make_root(spark, tmp_path / "root", [4, 5, 6])

    result = run_check(root, cwd=tmp_path)

    assert result.returncode == 0, result.stderr[-3000:]
    customers_csv = root / SHOP_MASTER / "customers.csv"
    expected = (
        f"[bronze-check] ok: all 3 distinct user_id in Bronze are among the "
        f"{committed_customer_count()} customers in {customers_csv}"
    )
    assert expected in result.stdout.splitlines()


def test_fails_on_unknown_users_and_writes_nothing(spark, tmp_path):
    root = make_root(spark, tmp_path / "root", [CUSTOMER, *NOT_CUSTOMERS, None])
    before = tree(root)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()

    result = run_check(root, cwd=elsewhere)

    assert result.returncode == 1
    customers_csv = root / SHOP_MASTER / "customers.csv"
    expected = (
        f"[bronze-check] error: 2 of 3 distinct user_id in Bronze are not customers in "
        f"{customers_csv} (first: 999998, 999999). The users dim would not match Bronze. "
        "Set SHOP_MASTER_DIR to the export Bronze was produced from. Nothing was written."
    )
    assert expected in result.stderr.splitlines()
    assert "[bronze-check] ok" not in result.stdout
    # No dims, Silver, Gold or logs; nothing in the working directory either.
    assert tree(root) == before
    assert not (root / "data" / "dim").exists()
    assert not (root / "logs").exists()
    assert list(elsewhere.iterdir()) == []


def write_export_with_extra_customer(folder, new_id):
    """A copy of the committed export plus one customer that passes the producer's rules.

    The new row copies customer 4's row, so every value parses (producer.py
    parse_customer: integer age and loyalty_points, is_prime_user 't'/'f', ISO
    signup_date) and every column is present; customer_id is new (duplicates are
    refused), and the name and email are new too.
    """
    shutil.copytree(ROOT / SHOP_MASTER, folder)
    with open(folder / "customers.csv", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        columns = reader.fieldnames
        rows = list(reader)
    template = next(row for row in rows if int(row["customer_id"]) == CUSTOMER)
    assert all(int(row["customer_id"]) != new_id for row in rows)
    assert all(row["email"] != f"test.customer{new_id}@example.net" for row in rows)
    extra = {
        **template,
        "customer_id": str(new_id),
        "customer_name": "Test Customer",
        "email": f"test.customer{new_id}@example.net",
    }
    with open(folder / "customers.csv", "a", newline="", encoding="utf-8") as file:
        csv.DictWriter(file, fieldnames=columns, lineterminator="\n").writerow(extra)
    return len(rows) + 1


def test_reads_the_export_in_shop_master_dir(spark, tmp_path):
    root = make_root(spark, tmp_path / "root", [CUSTOMER, NOT_CUSTOMERS[1]])
    customers = write_export_with_extra_customer(root / "exports" / "shop", NOT_CUSTOMERS[1])

    with_export = run_check(root, cwd=tmp_path, SHOP_MASTER_DIR="exports/shop")
    without = run_check(root, cwd=tmp_path)

    assert with_export.returncode == 0, with_export.stderr[-3000:]
    expected = (
        f"[bronze-check] ok: all 2 distinct user_id in Bronze are among the {customers} "
        f"customers in {root / 'exports' / 'shop' / 'customers.csv'}"
    )
    assert expected in with_export.stdout.splitlines()

    assert without.returncode == 1
    prefix = "[bronze-check] error: 1 of 2 distinct user_id in Bronze"
    error_lines = [line for line in without.stderr.splitlines() if line.startswith(prefix)]
    assert len(error_lines) == 1 and "(first: 999999)" in error_lines[0]


def test_has_main_and_starts_no_spark_on_import():
    # Fresh interpreter, so this module's own SparkSession can't hide one.
    code = (
        "import importlib.util\n"
        "from pyspark import SparkContext\n"
        f"spec = importlib.util.spec_from_file_location('bronze_check', {str(SCRIPT)!r})\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        "assert callable(module.main)\n"
        "assert SparkContext._active_spark_context is None, 'import started Spark'\n"
        "print('ok')\n"
    )
    env = {key: value for key, value in os.environ.items() if key != "SHOP_MASTER_DIR"}
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120, env=env
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip() == "ok"


# --- the DAG, read with ast: Airflow is not installed in CI --------------------------


def dag_tree():
    return ast.parse(DAG.read_text())


def operators(tree):
    """{variable name: BashOperator call} for every task assigned in the DAG."""
    found = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Call)
            and getattr(node.value.func, "id", None) == "BashOperator"
        ):
            found[node.targets[0].id] = node.value
    return found


def keyword(call, name):
    return next((kw.value for kw in call.keywords if kw.arg == name), None)


def bash_command(call):
    # bash_command=literal("""...""")
    return keyword(call, "bash_command").args[0].value


def chain(tree):
    """The task variables of the a >> b >> c statement, in order."""
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.BinOp)
            and isinstance(node.value.op, ast.RShift)
        ):
            names, expr = [], node.value
            while isinstance(expr, ast.BinOp):
                names.insert(0, expr.right.id)
                expr = expr.left
            return [expr.id, *names]
    raise AssertionError("no >> chain in the DAG")


def test_dag_checks_bronze_before_any_write():
    tree = dag_tree()
    tasks = operators(tree)

    assert chain(tree) == [
        "verify_bronze_exists",
        "verify_bronze_matches_export",
        "create_reference_data",
        "bronze_to_silver",
        "verify_silver_exists",
        "silver_to_gold",
        "verify_gold_exists",
        "gold_to_postgres",
        "verify_postgres_table",
    ]
    # Each task's id is its variable name, and every task is in the chain.
    task_ids = {name: keyword(call, "task_id").value for name, call in tasks.items()}
    assert task_ids == {name: name for name in chain(tree)}

    guard = tasks["verify_bronze_matches_export"]
    assert "spark-submit reference-data/check_bronze_against_export.py" in bash_command(guard)
    # Preflight: the python on PATH must be the project's venv, checked before Spark starts.
    command = bash_command(guard)
    preflight = 'python -c "import confluent_kafka, psycopg, pyspark" || {'
    assert preflight in command
    assert "start Airflow with scripts/run_airflow.sh" in command
    assert command.index(preflight) < command.index("spark-submit")
    assert keyword(guard, "retries").value == 0
    # Every other task keeps the DAG's default_args retries.
    with_retries = [name for name, call in tasks.items() if keyword(call, "retries") is not None]
    assert with_retries == ["verify_bronze_matches_export"]
    # Tasks inherit Airflow's environment: no task replaces it.
    assert all(keyword(call, "env") is None for call in tasks.values())
    publish = (
        "PUBLISH_REFUSE_SHRINK=1 spark-submit --packages org.postgresql:postgresql:42.7.4 "
        "spark-batch/publish_gold_to_postgres.py"
    )
    assert publish in bash_command(tasks["gold_to_postgres"])


def test_dag_is_triggered_by_hand():
    dag_call = next(
        node
        for node in ast.walk(dag_tree())
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "DAG"
    )
    schedule = keyword(dag_call, "schedule")
    assert isinstance(schedule, ast.Constant) and schedule.value is None
    assert keyword(dag_call, "catchup").value is False
    assert keyword(dag_call, "max_active_runs").value == 1
