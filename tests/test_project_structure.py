import ast
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


REQUIRED_PATHS = [
    "README.md",
    "docker-compose.yml",
    "data-generator/producer.py",
    "spark-streaming/read_stream.py",
    "spark-batch/bronze_to_silver.py",
    "spark-batch/silver_to_gold.py",
    "spark-batch/publish_gold_to_postgres.py",
    "api/main.py",
    "api/requirements.txt",
    "dags/project1_personalization_pipeline.py",
    "requirements-airflow.txt",
]


def test_required_project_files_exist():
    missing = [path for path in REQUIRED_PATHS if not (ROOT / path).exists()]
    assert not missing, f"Missing required project files: {missing}"


IGNORED_RUNTIME_PATHS = [
    "venv/",
    "venv-airflow/",
    "data/",
    "checkpoints/",
    "logs/",
    "airflow_home/",
]


def test_runtime_folders_are_gitignored():
    if shutil.which("git") is None or not (ROOT / ".git").exists():
        pytest.skip("git or the .git directory is not available")

    # --no-index: judge by the ignore rules alone, even for paths that exist.
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", "--verbose", "--non-matching", *IGNORED_RUNTIME_PATHS],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    not_ignored = [
        line.split("\t")[-1]
        for line in result.stdout.splitlines()
        if line.startswith("::")
    ]
    assert not not_ignored, f"Runtime paths not ignored by git: {not_ignored}"


def module_list_constant(relative_path, name):
    # Read a module-level list literal without importing the module
    # (read_stream.py starts a SparkSession on import).
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {relative_path}")


def test_stream_event_types_match_producer():
    producer_types = module_list_constant("data-generator/producer.py", "EVENT_TYPES")
    stream_types = module_list_constant("spark-streaming/read_stream.py", "EVENT_TYPES")
    assert stream_types == producer_types
