"""Durable SQLite manifest for committed ClickHouse message ranges."""

import hashlib
import json
import os
import sqlite3
import time
from collections import defaultdict
from contextlib import contextmanager

MICROSECONDS_PER_DAY = 86_400_000_000


def resolve_manifest_path(configured_path, config_directory):
    """Resolve a configured manifest file relative to the configuration file."""
    path = str(configured_path or "replication.sqlite3").strip()
    if not path:
        path = "replication.sqlite3"
    if not os.path.isabs(path):
        path = os.path.join(config_directory, path)
    return os.path.abspath(path)


class ManifestCursorExpired(Exception):
    """Raised when a consumer cursor predates retained manifest rows."""

    def __init__(self, earliest_seq):
        super().__init__(
            f"Manifest cursor expired; earliest sequence is {earliest_seq}"
        )
        self.earliest_seq = earliest_seq


def decode_insert_payload(raw_payload):
    """Normalize legacy string-encoded and direct JSON insert payloads."""
    if isinstance(raw_payload, str):
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError as error:
            raise ValueError("Invalid JSON payload") from error
    elif isinstance(raw_payload, dict):
        payload = raw_payload
    else:
        raise ValueError("Invalid or missing JSON data")
    if not isinstance(payload, dict):
        raise ValueError("JSON payload must be an object")
    return payload


def _record_key(record):
    """Return the canonical message key from a positional insert record."""
    if not isinstance(record, (list, tuple)) or len(record) < 2:
        raise ValueError("Each record must contain msg_id and chat_id")
    try:
        return abs(int(record[1])), int(record[0])
    except (TypeError, ValueError) as exc:
        raise ValueError("Record msg_id and chat_id must be integers") from exc


def deterministic_batch_id(records):
    """Build a stable identifier from sorted message keys."""
    keys = sorted(_record_key(record) for record in records)
    if not keys:
        raise ValueError("Cannot identify an empty batch")

    digest = hashlib.sha256()
    digest.update(f"count:{len(keys)}\n".encode("ascii"))
    for chat_id, msg_id in keys:
        digest.update(f"{chat_id}:{msg_id}\n".encode("ascii"))
    return digest.hexdigest()


def normalize_batch_id(batch_id, records):
    """Validate a caller batch ID or derive one for a legacy payload."""
    if batch_id is None:
        return deterministic_batch_id(records)
    normalized = str(batch_id).strip()
    if not normalized or len(normalized) > 128:
        raise ValueError("batch_id must contain between 1 and 128 characters")
    return normalized


def group_commit_ranges(records):
    """Summarize positional message records into one range per channel."""
    grouped = defaultdict(list)
    for record in records:
        chat_id, msg_id = _record_key(record)
        grouped[chat_id].append(msg_id)

    return [
        {
            "chat_id": chat_id,
            "first_msg_id": min(msg_ids),
            "last_msg_id": max(msg_ids),
            "record_count": len(msg_ids),
        }
        for chat_id, msg_ids in sorted(grouped.items())
    ]


class ManifestStore:
    """Persist committed channel ranges and replica acknowledgements."""

    def __init__(self, database_path, busy_timeout_ms=10_000):
        self.database_path = os.path.abspath(database_path)
        self.busy_timeout_ms = int(busy_timeout_ms)
        if self.busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")

    def initialize(self):
        """Create the manifest directory and schema when absent."""
        parent = os.path.dirname(self.database_path)
        if parent:
            os.makedirs(parent, mode=0o750, exist_ok=True)

        with self._connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS replication_commits (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    committed_at_us INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    first_msg_id INTEGER NOT NULL,
                    last_msg_id INTEGER NOT NULL,
                    record_count INTEGER NOT NULL CHECK (record_count > 0),
                    UNIQUE (batch_id, chat_id)
                );
                CREATE INDEX IF NOT EXISTS ix_replication_commits_committed_at
                    ON replication_commits(committed_at_us);
                CREATE TABLE IF NOT EXISTS replication_consumers (
                    consumer TEXT PRIMARY KEY,
                    last_ack_seq INTEGER NOT NULL CHECK (last_ack_seq >= 0),
                    updated_at_us INTEGER NOT NULL
                );
                """)

    @contextmanager
    def _connection(self):
        connection = sqlite3.connect(
            self.database_path,
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
        finally:
            connection.close()

    def record_batch(self, batch_id, ranges, committed_at_us=None):
        """Record all channel ranges for a successfully inserted batch."""
        normalized_ranges = list(ranges)
        if not normalized_ranges:
            raise ValueError("At least one commit range is required")
        committed_at = (
            time.time_ns() // 1000 if committed_at_us is None else int(committed_at_us)
        )

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for item in normalized_ranges:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO replication_commits (
                            batch_id, committed_at_us, chat_id, first_msg_id,
                            last_msg_id, record_count
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            batch_id,
                            committed_at,
                            int(item["chat_id"]),
                            int(item["first_msg_id"]),
                            int(item["last_msg_id"]),
                            int(item["record_count"]),
                        ),
                    )
                rows = connection.execute(
                    """
                    SELECT seq, chat_id, first_msg_id, last_msg_id, record_count
                    FROM replication_commits
                    WHERE batch_id = ?
                    ORDER BY seq
                    """,
                    (batch_id,),
                ).fetchall()
                self._validate_stored_ranges(normalized_ranges, rows)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return [dict(row) for row in rows]

    @staticmethod
    def _validate_stored_ranges(requested_ranges, stored_rows):
        """Reject a batch ID reused for different message content."""
        requested = {
            int(item["chat_id"]): (
                int(item["first_msg_id"]),
                int(item["last_msg_id"]),
                int(item["record_count"]),
            )
            for item in requested_ranges
        }
        stored = {
            int(row["chat_id"]): (
                int(row["first_msg_id"]),
                int(row["last_msg_id"]),
                int(row["record_count"]),
            )
            for row in stored_rows
        }
        if len(requested) != len(requested_ranges) or requested != stored:
            raise ValueError("batch_id was already used for different records")

    def head(self):
        """Return retained manifest sequence bounds and server time."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT min(seq) AS earliest_seq, max(seq) AS latest_seq "
                "FROM replication_commits"
            ).fetchone()
        return {
            "earliest_seq": row["earliest_seq"],
            "latest_seq": row["latest_seq"] or 0,
            "server_time_us": time.time_ns() // 1000,
        }

    def commits_after(self, after_seq, limit):
        """Return one ordered manifest page after a durable consumer cursor."""
        cursor = int(after_seq)
        page_size = int(limit)
        if cursor < 0:
            raise ValueError("after_seq must be non-negative")
        if page_size < 1:
            raise ValueError("limit must be positive")

        bounds = self.head()
        earliest = bounds["earliest_seq"]
        if earliest is not None and cursor < earliest - 1:
            raise ManifestCursorExpired(earliest)

        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT seq, batch_id, committed_at_us, chat_id, first_msg_id,
                       last_msg_id, record_count
                FROM replication_commits
                WHERE seq > ?
                ORDER BY seq
                LIMIT ?
                """,
                (cursor, page_size + 1),
            ).fetchall()

        has_more = len(rows) > page_size
        page = rows[:page_size]
        return {
            "commits": [dict(row) for row in page],
            "next_seq": page[-1]["seq"] if page else cursor,
            "has_more": has_more,
        }

    def acknowledge(self, consumer, seq, updated_at_us=None):
        """Persist and return a monotonic consumer acknowledgement."""
        name = str(consumer).strip()
        acknowledged_seq = int(seq)
        if not name or len(name) > 128:
            raise ValueError("consumer must contain between 1 and 128 characters")
        if acknowledged_seq < 0:
            raise ValueError("seq must be non-negative")
        updated_at = updated_at_us or time.time_ns() // 1000

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT last_ack_seq FROM replication_consumers WHERE consumer = ?",
                    (name,),
                ).fetchone()
                current = row["last_ack_seq"] if row else 0
                stored = max(current, acknowledged_seq)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO replication_consumers (
                        consumer, last_ack_seq, updated_at_us
                    ) VALUES (?, ?, ?)
                    """,
                    (name, stored, updated_at),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return stored

    def cleanup(self, retention_days=90, batch_size=10_000, now_us=None):
        """Delete one acknowledged, expired chunk and reclaim free pages."""
        days = int(retention_days)
        limit = int(batch_size)
        if days < 1 or limit < 1:
            raise ValueError("retention_days and batch_size must be positive")
        current_time = time.time_ns() // 1000 if now_us is None else int(now_us)
        cutoff = current_time - days * MICROSECONDS_PER_DAY

        with self._connection() as connection:
            row = connection.execute(
                "SELECT min(last_ack_seq) AS minimum_ack FROM replication_consumers"
            ).fetchone()
            minimum_ack = row["minimum_ack"]
            if minimum_ack is None:
                return 0
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    """
                    DELETE FROM replication_commits
                    WHERE seq IN (
                        SELECT seq FROM replication_commits
                        WHERE committed_at_us < ? AND seq <= ?
                        ORDER BY seq LIMIT ?
                    )
                    """,
                    (cutoff, minimum_ack, limit),
                )
                deleted = max(cursor.rowcount, 0)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            connection.execute("PRAGMA incremental_vacuum(100)")
            connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
        return deleted

    def status(self):
        """Return lightweight manifest and consumer metrics."""
        bounds = self.head()
        with self._connection() as connection:
            commit_count = connection.execute(
                "SELECT count(*) FROM replication_commits"
            ).fetchone()[0]
            consumers = connection.execute("""
                SELECT consumer, last_ack_seq, updated_at_us
                FROM replication_consumers ORDER BY consumer
                """).fetchall()
        bounds.update(
            {
                "commit_count": commit_count,
                "database_size": os.path.getsize(self.database_path),
                "consumers": [dict(row) for row in consumers],
            }
        )
        return bounds
