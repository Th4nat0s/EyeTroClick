"""Tests for bounded replication queries and authenticated Flask routes."""

import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from flask import Flask

from replication_api import (
    ReplicationApiConfig,
    ReplicationQueryConfig,
    ReplicationQueryService,
    create_replication_blueprint,
)
from replication_manifest import ManifestStore

NOW = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)
CUTOFF = "2024-07-21T12:00:00+00:00"


def _message_row(msg_id, chat_id, version=1):
    """Build one ClickHouse result row in canonical projection order."""
    emitted = datetime(2026, 7, 20, 10, 0)
    inserted = datetime(2026, 7, 20, 10, 1, tzinfo=timezone.utc)
    return (
        msg_id,
        chat_id,
        "channel",
        "sender",
        chat_id,
        "title",
        emitted,
        inserted,
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
        ["https://example.test"],
        ["tag"],
        version,
    )


class FakeClient:
    """Return one queued result while recording query parameters."""

    def __init__(self, factory):
        self.factory = factory
        self.disconnected = False

    def execute(self, query, parameters, settings=None):
        """Record the query and consume the next configured response."""
        self.factory.calls.append((query, parameters, settings))
        return self.factory.responses.pop(0)

    def disconnect(self):
        """Record that the service released the client connection."""
        self.disconnected = True


class FakeClientFactory:
    """Create fake clients backed by a shared response queue."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.clients = []

    def __call__(self):
        client = FakeClient(self)
        self.clients.append(client)
        return client

    def last_call(self):
        """Return the most recently executed query and parameters."""
        return self.calls[-1]

    def pending_response_count(self):
        """Return how many configured query responses remain."""
        return len(self.responses)


def _query_service(responses, max_export_page=10):
    factory = FakeClientFactory(responses)
    service = ReplicationQueryService(
        client_factory=factory,
        metadata_provider=lambda: {
            "date_column": "date",
            "insert_date_column": "insert_date",
            "columns": {"date", "insert_date", "version"},
        },
        config=ReplicationQueryConfig(
            database_name="messages",
            table_name="master",
            max_export_page=max_export_page,
            max_ranges=5,
        ),
    )
    return service, factory


class ReplicationQueryServiceTests(unittest.TestCase):
    """Validate cutoff enforcement and version-aware keyset pagination."""

    def test_global_full_export_returns_version_cursor(self):
        """A full page cursor must identify the last physical version."""
        service, factory = _query_service(
            [[_message_row(1, 100, 2), _message_row(2, 100, 3)]],
            max_export_page=1,
        )

        page = service.full_export(
            {"emitted_after": CUTOFF, "limit": 1},
            now=NOW,
        )

        self.assertTrue(page["has_more"])
        self.assertEqual(
            page["next_cursor"],
            {"chat_id": 100, "msg_id": 1, "version": 2},
        )
        self.assertEqual(page["results"][0]["msg_id"], 1)
        self.assertIn("ORDER BY chat_id, msg_id, version", factory.last_call()[0])
        self.assertEqual(
            factory.last_call()[2],
            {"max_execution_time": 30},
        )
        self.assertTrue(factory.clients[0].disconnected)

    def test_single_channel_export_uses_channel_filter(self):
        """A targeted full export must constrain the master query."""
        service, factory = _query_service([[]])

        page = service.full_export(
            {
                "emitted_after": CUTOFF,
                "channel_id": 100123,
                "limit": 5,
            },
            now=NOW,
        )

        self.assertFalse(page["has_more"])
        self.assertEqual(factory.calls[0][1]["channel_id"], 100123)
        self.assertIn("AND chat_id = %(channel_id)s", factory.calls[0][0])

    def test_cutoff_older_than_24_months_is_clamped(self):
        """The backend must never return messages beyond retention."""
        service, factory = _query_service([[]])

        page = service.full_export(
            {"emitted_after": "2020-01-01T00:00:00Z"},
            now=NOW,
        )

        self.assertEqual(page["cutoff"], CUTOFF)
        self.assertEqual(factory.calls[0][1]["cutoff"], CUTOFF)

    def test_future_cutoff_is_rejected(self):
        """A future cutoff is invalid rather than an empty valid window."""
        service, _factory = _query_service([])

        with self.assertRaisesRegex(ValueError, "future"):
            service.full_export(
                {"emitted_after": "2027-01-01T00:00:00Z"},
                now=NOW,
            )

    def test_empty_old_range_is_explicitly_completed(self):
        """Consumers need completion metadata when cutoff removes every row."""
        service, _factory = _query_service([[]])

        page = service.range_export(
            {
                "emitted_after": CUTOFF,
                "ranges": [
                    {
                        "seq": 8,
                        "chat_id": 100,
                        "first_msg_id": 1,
                        "last_msg_id": 10,
                    }
                ],
            },
            now=NOW,
        )

        self.assertEqual(page["results"], [])
        self.assertEqual(page["completed_seqs"], [8])
        self.assertFalse(page["has_more"])

    def test_range_cursor_includes_physical_version(self):
        """Range pagination cannot skip a newer duplicate physical version."""
        service, _factory = _query_service(
            [[_message_row(5, 100, 1), _message_row(5, 100, 2)]],
            max_export_page=1,
        )

        page = service.range_export(
            {
                "emitted_after": CUTOFF,
                "limit": 1,
                "ranges": [
                    {
                        "seq": 9,
                        "chat_id": 100,
                        "first_msg_id": 5,
                        "last_msg_id": 5,
                    }
                ],
            },
            now=NOW,
        )

        self.assertTrue(page["has_more"])
        self.assertEqual(
            page["next_cursor"],
            {"range_index": 0, "msg_id": 5, "version": 1},
        )
        self.assertEqual(page["results"][0]["manifest_seq"], 9)


class ReplicationBlueprintTests(unittest.TestCase):
    """Validate authentication and manifest HTTP behavior."""

    def setUp(self):
        """Build an authenticated test API with a temporary manifest."""
        self.temporary_directory = tempfile.mkdtemp()
        manifest_path = os.path.join(self.temporary_directory, "manifest.sqlite3")
        self.manifest = ManifestStore(manifest_path)
        self.manifest.initialize()
        self.manifest.record_batch(
            "batch-1",
            [
                {
                    "chat_id": 100,
                    "first_msg_id": 1,
                    "last_msg_id": 2,
                    "record_count": 2,
                }
            ],
        )
        self.query_service = Mock()
        self.query_service.metadata_available.return_value = True
        self.query_service.full_export.return_value = {"results": []}
        self.query_service.range_export.return_value = {"results": []}
        app = Flask(__name__)
        app.register_blueprint(
            create_replication_blueprint(
                self.manifest,
                self.query_service,
                ReplicationApiConfig(api_key="secret", max_manifest_page=2),
            )
        )
        app.config["TESTING"] = True
        self.client = app.test_client()
        self.headers = {"Authorization": "Bearer secret"}

    def tearDown(self):
        shutil.rmtree(self.temporary_directory)

    def test_missing_credentials_are_rejected(self):
        """Manifest metadata must not be available anonymously."""
        response = self.client.get("/replication/head")

        self.assertEqual(response.status_code, 401)

    def test_manifest_page_is_returned_in_sequence_order(self):
        """An authorized consumer can read a bounded manifest page."""
        response = self.client.get(
            "/replication/commits?after_seq=0&limit=1",
            headers=self.headers,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["commits"][0]["seq"], 1)

    def test_oversized_manifest_page_is_rejected(self):
        """Client input cannot bypass the configured manifest limit."""
        response = self.client.get(
            "/replication/commits?after_seq=0&limit=3",
            headers=self.headers,
        )

        self.assertEqual(response.status_code, 400)

    def test_targeted_full_payload_reaches_query_service(self):
        """The backend contract preserves the optional channel selector."""
        payload = {"emitted_after": CUTOFF, "channel_id": 100123}

        response = self.client.post(
            "/replication/messages/full",
            json=payload,
            headers=self.headers,
        )

        self.assertEqual(response.status_code, 200)
        self.query_service.full_export.assert_called_once_with(payload)

    def test_acknowledgement_is_monotonic(self):
        """Stale HTTP acknowledgements cannot regress durable state."""
        first = self.client.post(
            "/replication/ack",
            json={"seq": 10},
            headers=self.headers,
        )
        stale = self.client.post(
            "/replication/ack",
            json={"seq": 3},
            headers=self.headers,
        )

        self.assertEqual(first.get_json()["last_ack_seq"], 10)
        self.assertEqual(stale.get_json()["last_ack_seq"], 10)


if __name__ == "__main__":
    unittest.main()
