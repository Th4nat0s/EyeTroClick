"""Tests for the durable replication commit manifest."""

import os
import json
import shutil
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from replication_manifest import (
    MICROSECONDS_PER_DAY,
    ManifestCursorExpired,
    ManifestStore,
    decode_insert_payload,
    deterministic_batch_id,
    group_commit_ranges,
    normalize_batch_id,
)


def _record(msg_id, chat_id):
    """Return the key-bearing prefix expected by the manifest helpers."""
    return [msg_id, chat_id]


class ManifestHelperTests(unittest.TestCase):
    """Validate deterministic IDs and range grouping."""

    def test_batch_id_is_order_independent(self):
        """Record ordering must not alter the deterministic retry key."""
        records = [_record(2, -100), _record(1, 100)]

        self.assertEqual(
            deterministic_batch_id(records),
            deterministic_batch_id(list(reversed(records))),
        )

    def test_explicit_batch_id_is_preserved(self):
        """A caller-provided durable batch identity takes precedence."""
        self.assertEqual(normalize_batch_id(" batch-1 ", [_record(1, 2)]), "batch-1")

    def test_ranges_are_grouped_by_absolute_channel(self):
        """Signed Telegram IDs must share one canonical channel range."""
        records = [_record(8, -100), _record(3, 100), _record(4, 200)]

        self.assertEqual(
            group_commit_ranges(records),
            [
                {
                    "chat_id": 100,
                    "first_msg_id": 3,
                    "last_msg_id": 8,
                    "record_count": 2,
                },
                {
                    "chat_id": 200,
                    "first_msg_id": 4,
                    "last_msg_id": 4,
                    "record_count": 1,
                },
            ],
        )

    def test_invalid_record_is_rejected(self):
        """Malformed source keys must fail before ClickHouse insertion."""
        with self.assertRaisesRegex(ValueError, "msg_id and chat_id"):
            deterministic_batch_id([["bad", 10]])

    def test_legacy_string_payload_is_decoded(self):
        """Existing ChannelDumper string-encoded requests remain supported."""
        payload = {"records": [[1, 100]], "api_key": "legacy"}

        self.assertEqual(decode_insert_payload(json.dumps(payload)), payload)

    def test_direct_json_payload_is_preserved(self):
        """New importers can send a regular JSON object with batch_id."""
        payload = {"records": [[1, 100]], "batch_id": "batch-1"}

        self.assertIs(decode_insert_payload(payload), payload)


class ManifestStoreTests(unittest.TestCase):
    """Validate persistence, retries, cursors, and cleanup."""

    def setUp(self):
        self.temporary_directory = tempfile.mkdtemp()
        database_path = os.path.join(self.temporary_directory, "manifest.sqlite3")
        self.store = ManifestStore(database_path)
        self.store.initialize()

    def tearDown(self):
        shutil.rmtree(self.temporary_directory)

    @staticmethod
    def _range(chat_id, first_id, last_id, count=1):
        return {
            "chat_id": chat_id,
            "first_msg_id": first_id,
            "last_msg_id": last_id,
            "record_count": count,
        }

    def test_retry_does_not_duplicate_a_manifest_range(self):
        """The same durable batch can be retried without duplicate work."""
        ranges = [self._range(100, 10, 20, 11)]

        first = self.store.record_batch("batch-1", ranges, committed_at_us=10)
        retried = self.store.record_batch("batch-1", ranges, committed_at_us=20)

        self.assertEqual(first, retried)
        self.assertEqual(self.store.status()["commit_count"], 1)

    def test_batch_id_cannot_hide_different_records(self):
        """A reused importer identity must describe the same channel ranges."""
        self.store.record_batch(
            "batch-1",
            [self._range(100, 10, 20, 11)],
        )

        with self.assertRaisesRegex(ValueError, "different records"):
            self.store.record_batch(
                "batch-1",
                [self._range(100, 10, 21, 12)],
            )

        commit = self.store.commits_after(0, 1)["commits"][0]
        self.assertEqual(commit["last_msg_id"], 20)

    def test_batch_id_cannot_drop_a_previously_committed_channel(self):
        """A partial retry cannot silently omit work from the original batch."""
        self.store.record_batch(
            "batch-1",
            [
                self._range(100, 10, 20, 11),
                self._range(200, 30, 40, 11),
            ],
        )

        with self.assertRaisesRegex(ValueError, "different records"):
            self.store.record_batch(
                "batch-1",
                [self._range(100, 10, 20, 11)],
            )

        self.assertEqual(self.store.status()["commit_count"], 2)

    def test_pages_are_strictly_ordered_by_sequence(self):
        """Commit order must remain independent from Telegram ID order."""
        self.store.record_batch("batch-high", [self._range(100, 200, 300)])
        self.store.record_batch("batch-low", [self._range(100, 10, 20)])

        first_page = self.store.commits_after(0, 1)
        second_page = self.store.commits_after(first_page["next_seq"], 1)

        self.assertTrue(first_page["has_more"])
        self.assertEqual(first_page["commits"][0]["first_msg_id"], 200)
        self.assertFalse(second_page["has_more"])
        self.assertEqual(second_page["commits"][0]["first_msg_id"], 10)

    def test_acknowledgement_never_regresses(self):
        """A stale acknowledgement cannot move a consumer backwards."""
        self.assertEqual(self.store.acknowledge("replica", 12), 12)
        self.assertEqual(self.store.acknowledge("replica", 4), 12)
        self.assertEqual(
            self.store.status()["consumers"][0]["last_ack_seq"],
            12,
        )

    def test_cleanup_expires_old_acknowledged_cursor(self):
        """Cleanup removes only acknowledged rows past retention."""
        now_us = 100 * MICROSECONDS_PER_DAY
        old_time = now_us - 91 * MICROSECONDS_PER_DAY
        self.store.record_batch(
            "old-batch",
            [self._range(100, 1, 1)],
            committed_at_us=old_time,
        )
        self.store.record_batch(
            "current-batch",
            [self._range(100, 2, 2)],
            committed_at_us=now_us,
        )
        latest_seq = self.store.head()["latest_seq"]
        self.store.acknowledge("replica", latest_seq, updated_at_us=now_us)

        self.assertEqual(self.store.cleanup(now_us=now_us), 1)
        with self.assertRaises(ManifestCursorExpired) as context:
            self.store.commits_after(0, 10)
        self.assertEqual(context.exception.earliest_seq, 2)

    def test_cleanup_keeps_unacknowledged_rows(self):
        """Retention cannot discard work that no consumer acknowledged."""
        self.store.record_batch(
            "old-batch",
            [self._range(100, 1, 1)],
            committed_at_us=1,
        )

        self.assertEqual(
            self.store.cleanup(now_us=100 * MICROSECONDS_PER_DAY),
            0,
        )

    def test_concurrent_writers_preserve_every_batch(self):
        """WAL and bounded transactions must support parallel producers."""

        def write_batch(index):
            return self.store.record_batch(
                f"batch-{index}",
                [self._range(100 + index, index, index)],
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(write_batch, range(40)))

        self.assertTrue(all(results))
        self.assertEqual(self.store.status()["commit_count"], 40)
        self.assertEqual(self.store.head()["latest_seq"], 40)


if __name__ == "__main__":
    unittest.main()
