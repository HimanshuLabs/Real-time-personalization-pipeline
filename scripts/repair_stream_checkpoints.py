"""Repair a planned-but-uncommitted batch in the stream checkpoints before a start.

Spark 4.1.x crashes on restart with a NullPointerException in
KafkaMicroBatchStream.metrics when it replays a batch that is in the offset log
but not in the commit log (offsets/N without commits/N): metrics() runs before
latestOffset() (https://github.com/apache/spark/issues/55236). A stop can leave
such a batch. For the highest pending batch N of each checkpoint:

- Bronze (file sink): if the sink already recorded batch N in
  data/bronze/user_events/_spark_metadata (N or N.compact), the data is in Bronze,
  so write commits/N like the latest existing commit file. Otherwise move offsets/N
  into _repaired/, so Spark replans batch N from commit N-1.
- DLQ (Kafka sink, at-least-once): move offsets/N into _repaired/.

Nothing is ever deleted. Exit 0 on success (also when there is nothing to do),
1 on an error, so scripts/run_stream.sh does not start the stream.
Standard library only: it runs before spark-submit, with any python3.
"""

import os
import shutil
import sys
from pathlib import Path

# Same paths as spark-streaming/read_stream.py.
PROJECT_ROOT = os.environ.get("PROJECT1_ROOT", str(Path(__file__).resolve().parents[1]))

BRONZE_CHECKPOINT = "checkpoints/bronze_user_events"
BRONZE_SINK_METADATA = "data/bronze/user_events/_spark_metadata"
DLQ_CHECKPOINT = "checkpoints/dlq"

REPAIRED_DIR = "_repaired"

# Spark's commit-log entry for a query without watermarks; used only when the
# checkpoint has no commit file to copy yet.
DEFAULT_COMMIT = b'v1\n{"nextBatchWatermarkMs":0}'


def log(message):
    print(f"[repair] {message}", flush=True)


def batch_ids(folder):
    # Batch files are named by their batch id; skip .N.crc and temporary files.
    if not folder.is_dir():
        return []
    return sorted(int(path.name) for path in folder.iterdir() if path.name.isdigit())


def crc_path(path):
    return path.with_name(f".{path.name}.crc")


def pending_batch(checkpoint):
    """The highest batch in offsets/ without a commits/ entry, or None."""
    offsets = batch_ids(checkpoint / "offsets")
    if not offsets:
        return None
    latest = offsets[-1]
    if (checkpoint / "commits" / str(latest)).exists():
        return None
    return latest


def sink_recorded(metadata_dir, batch_id):
    return (metadata_dir / str(batch_id)).exists() or (
        metadata_dir / f"{batch_id}.compact"
    ).exists()


def free_destination(folder, name):
    # Never overwrite an earlier repair of the same batch id.
    destination = folder / name
    suffix = 1
    while destination.exists() or crc_path(destination).exists():
        destination = folder / f"{name}.{suffix}"
        suffix += 1
    return destination


def move_offsets(checkpoint, batch_id):
    source = checkpoint / "offsets" / str(batch_id)
    repaired = checkpoint / REPAIRED_DIR
    repaired.mkdir(exist_ok=True)
    destination = free_destination(repaired, f"offsets-{batch_id}")

    if crc_path(source).exists():
        shutil.move(crc_path(source), crc_path(destination))
    shutil.move(source, destination)
    replan_from = f"commit {batch_id - 1}" if batch_id > 0 else "the starting offsets"
    log(
        f"{checkpoint.name}: batch {batch_id} planned but not committed; moved "
        f"offsets/{batch_id} to {REPAIRED_DIR}/{destination.name}, Spark replans it "
        f"from {replan_from}"
    )


def write_commit(checkpoint, batch_id):
    commits = checkpoint / "commits"
    commits.mkdir(exist_ok=True)
    existing = batch_ids(commits)
    if existing:
        template = commits / str(existing[-1])
        content = template.read_bytes()
        template_crc = crc_path(template)
    else:
        content = DEFAULT_COMMIT
        template_crc = None

    target = commits / str(batch_id)
    temporary = commits / f".{batch_id}.repair.tmp"
    temporary.write_bytes(content)
    # Same bytes as the template, so its Hadoop checksum file is valid for it too.
    if template_crc is not None and template_crc.exists():
        shutil.copyfile(template_crc, crc_path(target))
    os.replace(temporary, target)
    log(
        f"{checkpoint.name}: batch {batch_id} already in Bronze (sink metadata); "
        f"wrote commits/{batch_id}"
    )


def repair(root):
    """Repair both checkpoints under root. Returns the number of actions taken."""
    root = Path(root)
    actions = 0

    bronze = root / BRONZE_CHECKPOINT
    batch_id = pending_batch(bronze)
    if batch_id is not None:
        if sink_recorded(root / BRONZE_SINK_METADATA, batch_id):
            write_commit(bronze, batch_id)
        else:
            move_offsets(bronze, batch_id)
        actions += 1

    dlq = root / DLQ_CHECKPOINT
    batch_id = pending_batch(dlq)
    if batch_id is not None:
        move_offsets(dlq, batch_id)
        actions += 1

    return actions


def main():
    try:
        repair(PROJECT_ROOT)
    except OSError as exc:
        print(f"[repair] error: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
