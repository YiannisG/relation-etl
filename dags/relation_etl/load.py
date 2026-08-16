from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path

from .transform import TransformResult

logger = logging.getLogger("etl.load")

MAX_LOAD_RETRIES = 3
LOAD_RETRY_BACKOFF_SECONDS = 1.0


class SchemaMismatchError(ValueError):
    """Raised when a record contains a field with real data that the load
    schema doesn't know about. Fails the load loudly and before any writes
    happen, rather than silently dropping data - the current column lists
    (GENE_COLS, TX_COLS, EXON_COLS) are the only place that could otherwise
    happen invisibly, since _row() only ever looks up the columns it's told
    to. A field that's null in every record it appears in is exempt - there's
    no data there to lose, so it's not worth failing the run over.
    """


def _check_known_columns(records: list[dict], cols: list[str], table: str) -> None:
    known = set(cols)
    unknown_with_data: set[str] = set()
    for record in records:
        for field_name in set(record.keys()) - known:
            if record[field_name] is not None:
                unknown_with_data.add(field_name)
    if unknown_with_data:
        raise SchemaMismatchError(
            f"{table}: record(s) contain field(s) with data not in the load "
            f"schema: {sorted(unknown_with_data)}. Add them to the column "
            f"list and CREATE TABLE statement for '{table}' in load.py, or "
            "confirm the source data change is intentional before proceeding."
        )


SCHEMA = """
CREATE TABLE IF NOT EXISTS genes (
    gene_id       TEXT PRIMARY KEY,
    gene_name     TEXT,
    biotype       TEXT,
    chromosome    TEXT,
    start         INTEGER,
    end           INTEGER,
    gc_content    REAL,
    synonyms      TEXT,   -- JSON-encoded list
    pathways      TEXT,   -- JSON-encoded list
    description   TEXT
);

CREATE TABLE IF NOT EXISTS transcripts (
    transcript_id TEXT PRIMARY KEY,
    gene_id       TEXT NOT NULL REFERENCES genes(gene_id),
    biotype       TEXT,
    length        INTEGER,
    exon_count    INTEGER,
    is_canonical  INTEGER,  -- 0/1
    cds_length    INTEGER,
    feature_flags TEXT      -- JSON-encoded list
);

CREATE TABLE IF NOT EXISTS exons (
    exon_id       TEXT PRIMARY KEY,
    transcript_id TEXT NOT NULL REFERENCES transcripts(transcript_id),
    start         INTEGER,
    end           INTEGER,
    phase         INTEGER,
    is_coding     INTEGER  -- 0/1
);

CREATE TABLE IF NOT EXISTS etl_quarantine (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        TEXT,
    loaded_at     TEXT,
    source_table  TEXT,
    reason        TEXT,
    record_json   TEXT
);

CREATE TABLE IF NOT EXISTS etl_merge_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        TEXT,
    loaded_at     TEXT,
    note          TEXT
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id        TEXT PRIMARY KEY,
    started_at    TEXT,
    finished_at   TEXT,
    genes_loaded  INTEGER,
    transcripts_loaded INTEGER,
    exons_loaded  INTEGER,
    quarantined   INTEGER
);
"""

GENE_COLS = [
    "gene_id",
    "gene_name",
    "biotype",
    "chromosome",
    "start",
    "end",
    "gc_content",
    "synonyms",
    "pathways",
    "description",
]
TX_COLS = [
    "transcript_id",
    "gene_id",
    "biotype",
    "length",
    "exon_count",
    "is_canonical",
    "cds_length",
    "feature_flags",
]
EXON_COLS = ["exon_id", "transcript_id", "start", "end", "phase", "is_coding"]


def _row(record: dict, cols: list[str]) -> tuple:
    out = []
    for c in cols:
        v = record.get(c)
        if isinstance(v, (list, dict)):
            v = json.dumps(v)
        elif isinstance(v, bool):
            v = int(v)
        out.append(v)
    return tuple(out)


def _upsert(conn: sqlite3.Connection, table: str, cols: list[str], rows: list[tuple]):
    if not rows:
        return
    # ON CONFLICT targets cols[0] - this only works because GENE_COLS,
    # TX_COLS, and EXON_COLS all list their primary key first by
    # convention. Not runtime-enforced against the schema; if one of
    # those lists is ever reordered, this breaks silently.
    pk_col = cols[0]
    placeholders = ",".join("?" for _ in cols)
    updates = ",".join(f"{c}=excluded.{c}" for c in cols[1:])  # skip PK
    sql = (
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT({pk_col}) DO UPDATE SET {updates}"
    )
    conn.executemany(sql, rows)


def load(result: TransformResult, db_path: str, run_id: str | None = None) -> str:
    """If there are undefined fields with non-empty data, fail on load. The extra fields need to be considered and appropriately added to the data model"""
    _check_known_columns(result.genes, GENE_COLS, "genes")
    _check_known_columns(result.transcripts, TX_COLS, "transcripts")
    _check_known_columns(result.exons, EXON_COLS, "exons")

    run_id = run_id or datetime.now(UTC).strftime("run-%Y%m%dT%H%M%SZ")
    started_at = datetime.now(UTC).isoformat()

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    attempt = 0
    while True:
        attempt += 1
        conn = None
        try:
            conn = sqlite3.connect(db_path, timeout=30)
            # WAL mode avoids creating/deleting a separate -journal file next
            # to the main db file on every write - the default rollback-
            # journal mode's need to do that is a common source of "disk I/O
            # error" on Docker bind-mounted volumes. busy_timeout makes
            # sqlite3 wait and retry internally on a transient lock instead
            # of failing immediately.
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.executescript(SCHEMA)

            _upsert(
                conn, "genes", GENE_COLS, [_row(g, GENE_COLS) for g in result.genes]
            )
            _upsert(
                conn,
                "transcripts",
                TX_COLS,
                [_row(t, TX_COLS) for t in result.transcripts],
            )
            _upsert(
                conn, "exons", EXON_COLS, [_row(e, EXON_COLS) for e in result.exons]
            )

            now = datetime.now(UTC).isoformat()
            conn.executemany(
                "INSERT INTO etl_quarantine (run_id, loaded_at, source_table, "
                "reason, record_json) VALUES (?, ?, ?, ?, ?)",
                [
                    (run_id, now, q.table, q.reason, json.dumps(q.record))
                    for q in result.quarantine
                ],
            )
            conn.executemany(
                "INSERT INTO etl_merge_log (run_id, loaded_at, note) VALUES (?, ?, ?)",
                [(run_id, now, note) for note in result.merge_log],
            )
            conn.execute(
                "INSERT OR REPLACE INTO etl_run_log (run_id, started_at, finished_at, "
                "genes_loaded, transcripts_loaded, exons_loaded, quarantined) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    started_at,
                    now,
                    len(result.genes),
                    len(result.transcripts),
                    len(result.exons),
                    len(result.quarantine),
                ),
            )
            conn.commit()
            logger.info("load complete: run_id=%s db=%s", run_id, db_path)
            return run_id
        except sqlite3.OperationalError as e:
            # Transient (disk I/O error, database is locked, etc) - retry
            # with backoff. Anything else (IntegrityError and similar) is a
            # real bug in the data or schema and should fail immediately,
            # so only OperationalError is caught here.
            if attempt > MAX_LOAD_RETRIES:
                raise
            delay = LOAD_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                f"sqlite operational error on attempt {attempt}/{MAX_LOAD_RETRIES}: "
                f"{e}; retrying in {delay:.1f}s"
            )
            time.sleep(delay)
            continue
        finally:
            if conn is not None:
                conn.close()
