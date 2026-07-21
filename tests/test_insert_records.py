"""Integration tests for ClickHouse insertion followed by manifest commit."""

# The complete ClickHouse row fixture intentionally mirrors the export schema.
# pylint: disable=duplicate-code

import importlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

import clickhouse_driver

TABLE_COLUMNS = {
    "msg_id",
    "chat_id",
    "chat_name",
    "username",
    "sender_chat_id",
    "title",
    "date",
    "insert_date",
    "document_present",
    "document_name",
    "document_type",
    "document_size",
    "msg_fwd",
    "msg_fwd_username",
    "msg_fwd_title",
    "msg_fwd_id",
    "text",
    "lang",
    "urls",
    "hashtags",
    "version",
}


class BootstrapClient:
    """Satisfy table introspection while importing the Flask application."""

    def __init__(self, **_kwargs):
        self.disconnected = False

    def execute(self, query, _parameters=None):
        """Return schema metadata and an empty minimum-date result."""
        if "system.columns" in query:
            return [(column,) for column in sorted(TABLE_COLUMNS)]
        if "SELECT min(" in query:
            return [(None,)]
        return []

    def disconnect(self):
        """Record connection cleanup."""
        self.disconnected = True


class InsertClient:
    """Capture message inserts without requiring a live ClickHouse server."""

    calls = []
    failure = None

    def __init__(self, **_kwargs):
        self.disconnected = False

    def execute(self, query, records):
        """Record or fail one configured ClickHouse insertion."""
        self.__class__.calls.append((query, records))
        if self.__class__.failure:
            raise self.__class__.failure
        return []

    def disconnect(self):
        """Record connection cleanup."""
        self.disconnected = True


def _record(msg_id=1, chat_id=100):
    """Build one valid positional record accepted by the legacy endpoint."""
    return [
        msg_id,
        chat_id,
        "channel",
        "sender",
        chat_id,
        "title",
        "2026-07-20T10:00:00+00:00",
        "2026-07-20T10:01:00+00:00",
        0,
        "",
        "",
        0,
        0,
        "",
        "",
        0,
        "message",
        "en",
        [],
        [],
        1,
    ]


class InsertRecordsIntegrationTests(unittest.TestCase):
    """Verify compatibility and cross-database write ordering."""

    @classmethod
    def setUpClass(cls):
        """Import db_svr with isolated storage and mocked introspection."""
        cls.temporary_directory = tempfile.mkdtemp()
        cls.previous_manifest_path = os.environ.get("REPLICATION_MANIFEST_PATH")
        os.environ["REPLICATION_MANIFEST_PATH"] = os.path.join(
            cls.temporary_directory, "manifest.sqlite3"
        )
        sys.modules.pop("db_svr", None)
        with patch.object(clickhouse_driver, "Client", BootstrapClient):
            cls.backend = importlib.import_module("db_svr")
        cls.backend.app.config["TESTING"] = True
        cls.client = cls.backend.app.test_client()

    @classmethod
    def tearDownClass(cls):
        """Restore environment state and remove the temporary manifest."""
        if cls.previous_manifest_path is None:
            os.environ.pop("REPLICATION_MANIFEST_PATH", None)
        else:
            os.environ["REPLICATION_MANIFEST_PATH"] = cls.previous_manifest_path
        shutil.rmtree(cls.temporary_directory)

    def setUp(self):
        """Reset fake ClickHouse behavior before every request."""
        InsertClient.calls = []
        InsertClient.failure = None

    def test_legacy_string_payload_commits_after_clickhouse(self):
        """The deployed ChannelDumper payload remains accepted and durable."""
        payload = json.dumps({"records": [_record()]})

        with patch.object(self.backend, "Client", InsertClient):
            response = self.client.post("/insert_records", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(InsertClient.calls), 1)
        batch_id = response.get_json()["batch_id"]
        commits = self.backend.replication_manifest.commits_after(0, 10)["commits"]
        self.assertEqual(commits[0]["batch_id"], batch_id)

    def test_regular_json_batch_id_is_idempotent(self):
        """A stable importer batch ID cannot duplicate manifest work."""
        payload = {"batch_id": "durable-batch", "records": [_record(2, 200)]}

        with patch.object(self.backend, "Client", InsertClient):
            first = self.client.post("/insert_records", json=payload)
            retried = self.client.post("/insert_records", json=payload)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(retried.status_code, 200)
        self.assertEqual(first.get_json()["commits"], retried.get_json()["commits"])

    def test_clickhouse_failure_does_not_create_manifest_work(self):
        """A failed master insert must never become visible to replicas."""
        before = self.backend.replication_manifest.status()["commit_count"]
        InsertClient.failure = RuntimeError("clickhouse unavailable")

        with patch.object(self.backend, "Client", InsertClient):
            response = self.client.post(
                "/insert_records",
                json={"batch_id": "failed-master", "records": [_record(3, 300)]},
            )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            self.backend.replication_manifest.status()["commit_count"],
            before,
        )

    def test_manifest_failure_requests_retry_after_master_success(self):
        """A manifest failure returns 500 after the ClickHouse write succeeds."""
        with patch.object(self.backend, "Client", InsertClient), patch.object(
            self.backend.replication_manifest,
            "record_batch",
            side_effect=RuntimeError("manifest unavailable"),
        ):
            response = self.client.post(
                "/insert_records",
                json={"batch_id": "manifest-failure", "records": [_record(4, 400)]},
            )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(len(InsertClient.calls), 1)
        self.assertIn("retry", response.get_json()["message"])


if __name__ == "__main__":
    unittest.main()
