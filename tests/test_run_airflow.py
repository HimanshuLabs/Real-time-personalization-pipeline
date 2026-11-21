"""scripts/run_airflow.sh with a fake Airflow: no Airflow install needed."""

import os
import shutil
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_airflow.sh"

# Prints what the real airflow would receive.
FAKE_AIRFLOW = """#!/usr/bin/env bash
printf 'PATH_FIRST=%s\\n' "${PATH%%:*}"
printf 'PYSPARK_PYTHON=%s\\n' "${PYSPARK_PYTHON-<unset>}"
printf 'PYSPARK_DRIVER_PYTHON=%s\\n' "${PYSPARK_DRIVER_PYTHON-<unset>}"
printf 'AIRFLOW_HOME=%s\\n' "${AIRFLOW_HOME-<unset>}"
printf 'PWD=%s\\n' "$PWD"
for arg in "$@"; do printf 'ARG=%s\\n' "$arg"; done
for name in $(compgen -e); do
  case "$name" in AIRFLOW__*) printf 'INHERITED=%s\\n' "$name" ;; esac
done
"""


def write_executable(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def make_repo(tmp_path, python=True, airflow=True):
    """A copy of the script in a repository folder whose path has spaces."""
    repo = tmp_path / "a repo with spaces"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy(SCRIPT, repo / "scripts" / "run_airflow.sh")
    if python:
        write_executable(repo / "venv" / "bin" / "python", "#!/usr/bin/env bash\nexit 0\n")
    if airflow:
        write_executable(repo / "venv-airflow" / "bin" / "airflow", FAKE_AIRFLOW)
    return repo


def run(repo, cwd, *args, **env):
    dropped = ("AIRFLOW_HOME", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON")
    clean = {
        key: value
        for key, value in os.environ.items()
        if key not in dropped and not key.startswith("AIRFLOW__")
    }
    return subprocess.run(
        ["bash", str(repo / "scripts" / "run_airflow.sh"), *args],
        cwd=cwd,
        env={**clean, **env},
        capture_output=True,
        text=True,
        timeout=60,
    )


def values(stdout):
    return [line.split("=", 1) for line in stdout.splitlines()]


def test_runs_airflow_from_its_venv_with_the_project_venv_first(tmp_path):
    repo = make_repo(tmp_path)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()

    result = run(repo, elsewhere, "tasks", "test", "a dag", "a task")

    assert result.returncode == 0, result.stderr
    assert values(result.stdout) == [
        ["PATH_FIRST", str(repo / "venv" / "bin")],
        ["PYSPARK_PYTHON", "python"],
        ["PYSPARK_DRIVER_PYTHON", "python"],
        ["AIRFLOW_HOME", str(repo / "airflow_home")],
        ["PWD", str(repo)],
        ["ARG", "tasks"],
        ["ARG", "test"],
        ["ARG", "a dag"],
        ["ARG", "a task"],
    ]


def test_keeps_an_airflow_home_that_is_already_set(tmp_path):
    repo = make_repo(tmp_path)
    home = tmp_path / "other home"

    result = run(repo, tmp_path, "version", AIRFLOW_HOME=str(home))

    assert result.returncode == 0, result.stderr
    assert ["AIRFLOW_HOME", str(home)] in values(result.stdout)


def test_replaces_pyspark_paths_with_the_plain_name(tmp_path):
    repo = make_repo(tmp_path)

    result = run(
        repo,
        tmp_path,
        "version",
        PYSPARK_PYTHON="/some path/python",
        PYSPARK_DRIVER_PYTHON="/some path/python",
    )

    assert result.returncode == 0, result.stderr
    assert ["PYSPARK_PYTHON", "python"] in values(result.stdout)
    assert ["PYSPARK_DRIVER_PYTHON", "python"] in values(result.stdout)


def test_ignores_inherited_airflow_settings_and_never_prints_values(tmp_path):
    repo = make_repo(tmp_path)
    secret = "postgresql://someone:s3cret-value@db.example:5432/other_airflow"
    other_dags = "/another project/dags"
    inherited = {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": secret,
        "AIRFLOW__CORE__DAGS_FOLDER": other_dags,
    }

    # The fake airflow reports AIRFLOW__ names when it does receive them.
    direct = subprocess.run(
        [str(repo / "venv-airflow" / "bin" / "airflow")],
        env={**os.environ, **inherited},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert ["INHERITED", "AIRFLOW__CORE__DAGS_FOLDER"] in values(direct.stdout)

    result = run(repo, tmp_path, "version", **inherited)

    assert result.returncode == 0, result.stderr
    received = values(result.stdout)
    assert [line for line in received if line[0] == "INHERITED"] == []
    assert ["AIRFLOW_HOME", str(repo / "airflow_home")] in received
    expected = (
        "[airflow] ignoring inherited settings: "
        "AIRFLOW__CORE__DAGS_FOLDER AIRFLOW__DATABASE__SQL_ALCHEMY_CONN; "
        f"this Airflow is configured by {repo}/airflow_home/airflow.cfg\n"
    )
    assert result.stderr == expected
    for value in (secret, "s3cret-value", other_dags):
        assert value not in result.stdout
        assert value not in result.stderr


def test_missing_project_venv_is_a_clear_error(tmp_path):
    repo = make_repo(tmp_path, python=False)

    result = run(repo, tmp_path, "version")

    assert result.returncode == 1
    assert result.stdout == ""
    expected = (
        f"[airflow] error: {repo}/venv/bin/python not found; "
        "create the project venv first (README, Quick start)\n"
    )
    assert result.stderr == expected


def test_missing_airflow_venv_is_a_clear_error(tmp_path):
    repo = make_repo(tmp_path, airflow=False)

    result = run(repo, tmp_path, "version")

    assert result.returncode == 1
    assert result.stdout == ""
    expected = (
        f"[airflow] error: {repo}/venv-airflow/bin/airflow not found; "
        "install Airflow into venv-airflow (README, Operations, Airflow)\n"
    )
    assert result.stderr == expected


def test_script_is_executable_in_the_repository():
    assert os.access(SCRIPT, os.X_OK)
