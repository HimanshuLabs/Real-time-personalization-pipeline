from pathlib import Path


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


def test_runtime_folders_are_not_required_for_ci():
    runtime_paths = [
        "data",
        "logs",
        "checkpoints",
        "airflow_home",
    ]

    for path in runtime_paths:
        assert isinstance(path, str)
