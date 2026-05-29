import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

EXCLUDED_DIRS = {
    ".git",
    ".github",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "venv",
    "venv-airflow",
    "airflow_home",
    "data",
    "logs",
    "checkpoints",
}


def should_skip(path: Path) -> bool:
    return any(part in EXCLUDED_DIRS for part in path.parts)


def test_python_files_have_valid_syntax():
    python_files = [
        path
        for path in ROOT.rglob("*.py")
        if not should_skip(path.relative_to(ROOT))
    ]

    assert python_files, "No Python files found to validate."

    broken_files = []

    for path in python_files:
        try:
            ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            broken_files.append(f"{path.relative_to(ROOT)}: {exc}")

    assert not broken_files, "Python syntax errors found:\n" + "\n".join(broken_files)
