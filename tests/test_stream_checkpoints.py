import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_stream_validation import RUN_STREAM, fake_log, launcher_env

ROOT = Path(__file__).resolve().parents[1]
REPAIR_SCRIPT = ROOT / "scripts" / "repair_stream_checkpoints.py"

BRONZE = "checkpoints/bronze_user_events"
DLQ = "checkpoints/dlq"
SINK_METADATA = "data/bronze/user_events/_spark_metadata"

COMMIT = b'v1\n{"nextBatchWatermarkMs":0}'
# A Hadoop checksum header and one CRC32; the repair copies it, never reads it.
COMMIT_CRC = b"crc\x00\x00\x00\x02\x00\xf3\x2a\xbf\xdd"


@pytest.fixture(scope="module")
def repair_module():
    spec = importlib.util.spec_from_file_location("repair_under_test", REPAIR_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_checkpoint(root, query, offsets, commits):
    """A fake checkpoint: offsets/0..N and commits/0..M, each with its .crc."""
    checkpoint = root / query
    for kind, last in (("offsets", offsets), ("commits", commits)):
        folder = checkpoint / kind
        folder.mkdir(parents=True)
        for batch_id in range(last + 1):
            body = COMMIT if kind == "commits" else f"v1\noffsets of batch {batch_id}".encode()
            (folder / str(batch_id)).write_bytes(body)
            (folder / f".{batch_id}.crc").write_bytes(COMMIT_CRC if kind == "commits" else b"x")
    (checkpoint / "metadata").write_text('{"id":"query"}')
    return checkpoint


def make_sink_metadata(root, *names):
    folder = root / SINK_METADATA
    folder.mkdir(parents=True, exist_ok=True)
    for name in names:
        (folder / name).write_text("v1\n")


def snapshot(root):
    """Every file under root with its bytes, to compare runs."""
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def run_repair(root):
    return subprocess.run(
        [sys.executable, str(REPAIR_SCRIPT)],
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "PROJECT1_ROOT": str(root)},
    )


def test_bronze_pending_batch_in_sink_metadata_gets_a_commit(tmp_path):
    bronze = make_checkpoint(tmp_path, BRONZE, offsets=5, commits=4)
    make_sink_metadata(tmp_path, "4", "5")

    result = run_repair(tmp_path)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "[repair] bronze_user_events: batch 5 already in Bronze (sink metadata); " "wrote commits/5"
    ]
    # Same format (bytes) as the latest existing commit, and its checksum file.
    assert (bronze / "commits" / "5").read_bytes() == COMMIT
    assert (bronze / "commits" / ".5.crc").read_bytes() == COMMIT_CRC
    assert (bronze / "offsets" / "5").exists()
    assert not (bronze / "_repaired").exists()
    assert not [p for p in (bronze / "commits").iterdir() if p.name.endswith(".tmp")]


def test_bronze_pending_batch_counts_compact_sink_metadata(tmp_path):
    bronze = make_checkpoint(tmp_path, BRONZE, offsets=9, commits=8)
    make_sink_metadata(tmp_path, "8", "9.compact")

    assert run_repair(tmp_path).returncode == 0

    assert (bronze / "commits" / "9").read_bytes() == COMMIT
    assert (bronze / "offsets" / "9").exists()


def test_bronze_pending_batch_without_sink_metadata_moves_offsets(tmp_path):
    bronze = make_checkpoint(tmp_path, BRONZE, offsets=5, commits=4)
    make_sink_metadata(tmp_path, "4")
    planned = (bronze / "offsets" / "5").read_bytes()

    result = run_repair(tmp_path)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "[repair] bronze_user_events: batch 5 planned but not committed; moved offsets/5 "
        "to _repaired/offsets-5, Spark replans it from commit 4"
    ]
    assert not (bronze / "offsets" / "5").exists()
    assert not (bronze / "offsets" / ".5.crc").exists()
    assert (bronze / "_repaired" / "offsets-5").read_bytes() == planned
    assert (bronze / "_repaired" / ".offsets-5.crc").read_bytes() == b"x"
    assert not (bronze / "commits" / "5").exists()
    assert (bronze / "offsets" / "4").exists()


def test_dlq_pending_batch_moves_offsets_even_with_bronze_metadata(tmp_path):
    # The DLQ is a Kafka sink: Bronze's sink log says nothing about it.
    dlq = make_checkpoint(tmp_path, DLQ, offsets=3, commits=2)
    make_sink_metadata(tmp_path, "3")

    result = run_repair(tmp_path)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "[repair] dlq: batch 3 planned but not committed; moved offsets/3 "
        "to _repaired/offsets-3, Spark replans it from commit 2"
    ]
    assert not (dlq / "offsets" / "3").exists()
    assert (dlq / "_repaired" / "offsets-3").exists()
    assert not (dlq / "commits" / "3").exists()


def test_both_queries_pending_one_line_each(tmp_path):
    make_checkpoint(tmp_path, BRONZE, offsets=2, commits=1)
    make_checkpoint(tmp_path, DLQ, offsets=2, commits=1)
    make_sink_metadata(tmp_path, "2")

    result = run_repair(tmp_path)

    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert len(lines) == 2
    assert lines[0].startswith("[repair] bronze_user_events: batch 2 already in Bronze")
    assert lines[1].startswith("[repair] dlq: batch 2 planned but not committed")


@pytest.mark.parametrize("layout", ["clean", "missing", "empty"])
def test_clean_missing_or_empty_checkpoint_changes_nothing(tmp_path, layout):
    if layout == "clean":
        make_checkpoint(tmp_path, BRONZE, offsets=4, commits=4)
        make_checkpoint(tmp_path, DLQ, offsets=4, commits=4)
        make_sink_metadata(tmp_path, "4")
    elif layout == "empty":
        # Created but no batch planned yet.
        (tmp_path / BRONZE / "offsets").mkdir(parents=True)
        (tmp_path / DLQ).mkdir(parents=True)
    before = snapshot(tmp_path)
    dirs_before = sorted(str(p) for p in tmp_path.rglob("*"))

    result = run_repair(tmp_path)

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert snapshot(tmp_path) == before
    assert sorted(str(p) for p in tmp_path.rglob("*")) == dirs_before


@pytest.mark.parametrize("sink_has_batch", [True, False], ids=["commit", "move"])
def test_running_twice_changes_nothing_more(tmp_path, sink_has_batch):
    make_checkpoint(tmp_path, BRONZE, offsets=5, commits=4)
    make_checkpoint(tmp_path, DLQ, offsets=5, commits=4)
    make_sink_metadata(tmp_path, *(["4", "5"] if sink_has_batch else ["4"]))

    first = run_repair(tmp_path)
    after_first = snapshot(tmp_path)
    second = run_repair(tmp_path)

    assert first.returncode == second.returncode == 0
    assert len(first.stdout.splitlines()) == 2
    assert second.stdout == ""
    assert snapshot(tmp_path) == after_first


def test_never_overwrites_an_earlier_repair(tmp_path, repair_module):
    # Batch 3 was moved once, replanned, and left pending again by a later stop.
    dlq = make_checkpoint(tmp_path, DLQ, offsets=3, commits=2)
    (dlq / "_repaired").mkdir()
    (dlq / "_repaired" / "offsets-3").write_bytes(b"earlier")

    assert repair_module.repair(tmp_path) == 1

    assert (dlq / "_repaired" / "offsets-3").read_bytes() == b"earlier"
    assert (dlq / "_repaired" / "offsets-3.1").read_bytes() == b"v1\noffsets of batch 3"


def test_first_commit_uses_the_default_commit_format(tmp_path, repair_module):
    # Batch 0 is in Bronze but no commit file exists yet to copy.
    bronze = make_checkpoint(tmp_path, BRONZE, offsets=0, commits=-1)
    make_sink_metadata(tmp_path, "0")

    assert repair_module.repair(tmp_path) == 1

    assert (bronze / "commits" / "0").read_bytes() == COMMIT
    assert not (bronze / "commits" / ".0.crc").exists()


def test_paths_resolve_from_project1_root(repair_module):
    # Imported without PROJECT1_ROOT set by this test: the repository root.
    if "PROJECT1_ROOT" not in os.environ:
        assert Path(repair_module.PROJECT_ROOT) == ROOT
    assert repair_module.BRONZE_CHECKPOINT == "checkpoints/bronze_user_events"
    assert repair_module.DLQ_CHECKPOINT == "checkpoints/dlq"


# --- scripts/run_stream.sh runs the repair just before spark-submit ----------------------


def test_launcher_repairs_before_spark_submit(tmp_path):
    log = tmp_path / "fake.log"
    env, _ = launcher_env(tmp_path, FAKE_LOG=str(log), FAKE_MODE="exit", FAKE_EXIT="0")
    root = Path(env["PROJECT1_ROOT"])
    dlq = make_checkpoint(root, DLQ, offsets=1, commits=0)

    result = subprocess.run([str(RUN_STREAM)], env=env, capture_output=True, text=True, timeout=30)

    assert result.returncode == 0, result.stderr
    assert "[repair] dlq: batch 1 planned but not committed" in result.stdout
    assert (dlq / "_repaired" / "offsets-1").exists()
    fields, _ = fake_log(log)
    assert "spark-streaming/read_stream.py" in fields["args"]


def test_launcher_does_not_start_spark_when_the_repair_fails(tmp_path):
    log = tmp_path / "fake.log"
    env, stop_root = launcher_env(tmp_path, FAKE_LOG=str(log), FAKE_MODE="exit", FAKE_EXIT="0")
    root = Path(env["PROJECT1_ROOT"])
    dlq = make_checkpoint(root, DLQ, offsets=1, commits=0)
    # _repaired is a file, so the move can't happen.
    (dlq / "_repaired").write_text("in the way")

    result = subprocess.run([str(RUN_STREAM)], env=env, capture_output=True, text=True, timeout=30)

    assert result.returncode == 1
    assert "[repair] error:" in result.stderr
    assert "Traceback" not in result.stderr
    assert not log.exists(), "spark-submit ran although the repair failed"
    assert (dlq / "offsets" / "1").exists()
    assert list(stop_root.iterdir()) == []  # the stop folder is still removed
