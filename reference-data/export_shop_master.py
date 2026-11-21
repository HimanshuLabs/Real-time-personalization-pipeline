"""Export the batch lakehouse's current customers and products for the producer, read-only.

Writes customers.csv and products.csv into SHOP_MASTER_DIR (a relative path
resolves from PROJECT1_ROOT, like producer.py), from the current SCD2 rows of
the batch lakehouse's warehouse.dim_customer and warehouse.dim_product:
is_current, without its unknown (0) and invalid-reference (-1) members, ordered
by numeric id. The files
have the producer's columns (data-generator/producer.py CUSTOMER_COLUMNS and
PRODUCT_COLUMNS) in PostgreSQL's CSV format, so an export of the data behind the
committed reference-data/shop_master gives those files byte for byte.

Read-only: one REPEATABLE READ READ ONLY transaction (both files are one
snapshot) in a session with default_transaction_read_only=on and a statement
timeout, SELECT and COPY TO only. It reads as p2_reader by default, the batch
lakehouse's read-only role, which has SELECT on warehouse
(sql/warehouse/create_schemas.sql in Batch-Lakehouse-Marketing-Analytics).
Before writing, every current row must have a positive integer id, no NULL in a
producer column and be the only current row of its id; otherwise nothing is
written. The files are written as temporary files and renamed into place, and
the committed reference-data/shop_master is never written.

Settings (environment): SHOP_MASTER_DIR (required), P2_EXPORT_HOST (127.0.0.1),
P2_EXPORT_PORT (5434), P2_EXPORT_DB (marketing_analytics), P2_EXPORT_USER
(p2_reader), P2_EXPORT_PASSWORD (required; never printed). Exit 0 on success,
2 on a refused setting, a failed check or a database error.
"""

import hashlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import IsolationLevel, sql

PROJECT_ROOT = os.environ.get("PROJECT1_ROOT", str(Path(__file__).resolve().parents[1]))

# The committed export, which this script never writes.
COMMITTED_SHOP_MASTER = Path(__file__).resolve().parent / "shop_master"

# Must match CUSTOMER_COLUMNS and PRODUCT_COLUMNS in data-generator/producer.py.
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

# (file, table, surrogate key, natural key, columns)
EXPORTS = [
    ("customers.csv", "dim_customer", "customer_sk", "customer_id", CUSTOMER_COLUMNS),
    ("products.csv", "dim_product", "product_sk", "product_id", PRODUCT_COLUMNS),
]

# A positive integer that fits Bronze's 32-bit user_id and product_id.
ID_PATTERN = "^[1-9][0-9]{0,8}$"

SESSION_OPTIONS = (
    "-c default_transaction_read_only=on -c statement_timeout=120000 "
    "-c DateStyle=ISO -c timezone=UTC"
)
CONNECT_TIMEOUT_SECONDS = 10


class ExportError(Exception):
    """A refused setting or a failed check; the message is printed as is."""


@dataclass(frozen=True)
class ExportSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def read_settings(environ):
    """(target folder, connection settings) from the environment, or ExportError."""
    folder = environ.get("SHOP_MASTER_DIR", "")
    if not folder:
        raise ExportError("SHOP_MASTER_DIR is not set")
    target = Path(os.path.join(environ.get("PROJECT1_ROOT", PROJECT_ROOT), folder)).resolve()
    if target == COMMITTED_SHOP_MASTER:
        raise ExportError(f"{target} is the committed reference data; export to another folder")

    password = environ.get("P2_EXPORT_PASSWORD", "")
    if not password:
        raise ExportError("P2_EXPORT_PASSWORD is not set")

    port = environ.get("P2_EXPORT_PORT", "5434")
    if not port.isdigit() or not 0 < int(port) < 65536:
        raise ExportError(f"P2_EXPORT_PORT must be a port number, got {port!r}")

    settings = ExportSettings(
        host=environ.get("P2_EXPORT_HOST", "127.0.0.1"),
        port=int(port),
        dbname=environ.get("P2_EXPORT_DB", "marketing_analytics"),
        user=environ.get("P2_EXPORT_USER", "p2_reader"),
        password=password,
    )
    return target, settings


def connect(settings):
    # Every transaction of this session is read-only unless one asks otherwise,
    # and export() asks for READ ONLY explicitly as well.
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
        options=SESSION_OPTIONS,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
    )


def check_rows(conn, table, surrogate_key, natural_key, columns):
    """The number of current rows to export; ExportError if any would break the producer."""
    name = f"warehouse.{table}"
    [(can_select,)] = conn.execute("SELECT has_table_privilege(%s, 'SELECT')", (name,)).fetchall()
    if not can_select:
        raise ExportError(
            f"the session's role cannot SELECT {name}; use a role that can, read-only"
        )

    query = sql.SQL(
        "SELECT count(*), count(DISTINCT {key}),"
        " count(*) FILTER (WHERE {key} !~ %s),"
        " count(*) FILTER (WHERE {any_null})"
        " FROM {table} WHERE is_current AND {sk} > 0"
    ).format(
        key=sql.Identifier(natural_key),
        any_null=sql.SQL(" OR ").join(
            sql.SQL("{} IS NULL").format(sql.Identifier(column)) for column in columns
        ),
        table=sql.Identifier("warehouse", table),
        sk=sql.Identifier(surrogate_key),
    )
    [(rows, distinct_ids, bad_ids, null_rows)] = conn.execute(query, (ID_PATTERN,)).fetchall()

    if rows == 0:
        raise ExportError(f"{name}: no current rows")
    if bad_ids:
        raise ExportError(
            f"{name}: {bad_ids} current row(s) whose {natural_key} is not a positive integer"
        )
    if null_rows:
        raise ExportError(f"{name}: {null_rows} current row(s) with a NULL required column")
    if distinct_ids != rows:
        raise ExportError(f"{name}: {rows} current rows but {distinct_ids} distinct {natural_key}")
    return rows


def copy_rows(conn, table, surrogate_key, natural_key, columns, path):
    """COPY the current rows into path in PostgreSQL's CSV format; returns the sha256."""
    query = sql.SQL(
        "COPY (SELECT {columns} FROM {table} WHERE is_current AND {sk} > 0"
        " ORDER BY {key}::bigint) TO STDOUT WITH (FORMAT csv, HEADER)"
    ).format(
        columns=sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        table=sql.Identifier("warehouse", table),
        sk=sql.Identifier(surrogate_key),
        key=sql.Identifier(natural_key),
    )
    digest = hashlib.sha256()
    with open(path, "wb") as file, conn.cursor().copy(query) as copy:
        for chunk in copy:
            file.write(chunk)
            digest.update(chunk)
        file.flush()
        os.fsync(file.fileno())
    return digest.hexdigest()


def export(conn, target):
    """Check, then write both files from one read-only snapshot. Returns the report lines."""
    conn.read_only = True
    conn.isolation_level = IsolationLevel.REPEATABLE_READ

    temporary = {file_name: target / f".{file_name}.export-tmp" for file_name, *_ in EXPORTS}
    try:
        with conn.transaction():
            [(read_only,)] = conn.execute("SHOW transaction_read_only").fetchall()
            if read_only != "on":
                raise ExportError("the transaction is not read-only")

            counts = {file_name: check_rows(conn, *spec) for file_name, *spec in EXPORTS}
            target.mkdir(parents=True, exist_ok=True)
            digests = {
                file_name: copy_rows(conn, *spec, temporary[file_name])
                for file_name, *spec in EXPORTS
            }

        for file_name, path in temporary.items():
            os.replace(path, target / file_name)
        directory = os.open(target, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        for path in temporary.values():
            path.unlink(missing_ok=True)

    return [
        f"[export] {target / file_name}: {counts[file_name]} rows, sha256 {digests[file_name]}"
        for file_name, *_ in EXPORTS
    ]


def main():
    try:
        target, settings = read_settings(os.environ)
    except ExportError as exc:
        print(f"[export] error: {exc}", file=sys.stderr, flush=True)
        return 2

    try:
        with connect(settings) as conn:
            lines = export(conn, target)
    except (ExportError, psycopg.Error) as exc:
        # One line. The driver's message names the server and role, never the password.
        detail = str(exc) if isinstance(exc, ExportError) else f"{type(exc).__name__}: {exc}"
        print(
            f"[export] error: {' '.join(detail.split())}; nothing was written",
            file=sys.stderr,
            flush=True,
        )
        return 2

    for line in lines:
        print(line, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
