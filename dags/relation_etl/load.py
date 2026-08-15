from __future__ import annotations

import json
import logging
import sqlite3
from datetime import UTC, datetime

from .transform import TransformResult

logger = logging.getLogger("etl.load")

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
    placeholders = ",".join("?" for _ in cols)
    updates = ",".join(f"{c}=excluded.{c}" for c in cols[1:])  # skip PK
    sql = (
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT({cols[0]}) DO UPDATE SET {updates}"
    )
    conn.executemany(sql, rows)


def load(result: TransformResult, db_path: str, run_id: str | None = None) -> str:
    run_id = run_id or datetime.now(UTC).strftime("run-%Y%m%dT%H%M%SZ")
    started_at = datetime.now(UTC).isoformat()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(SCHEMA)

        _upsert(conn, "genes", GENE_COLS, [_row(g, GENE_COLS) for g in result.genes])
        _upsert(
            conn, "transcripts", TX_COLS, [_row(t, TX_COLS) for t in result.transcripts]
        )
        _upsert(conn, "exons", EXON_COLS, [_row(e, EXON_COLS) for e in result.exons])

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
    finally:
        conn.close()
    return run_id
