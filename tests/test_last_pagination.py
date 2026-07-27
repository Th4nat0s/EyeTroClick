"""Tests for bounded and deterministic recent-message pagination."""

import importlib
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import clickhouse_driver


class BootstrapClient:
    """Provide schema metadata while importing the Flask application."""

    def __init__(self, **_kwargs):
        pass

    def execute(self, query, _parameters=None):
        """Return enough metadata for db_svr import initialization."""
        if "system.columns" in query:
            return [(column,) for column in ("id", "chat_id", "date", "insert_date")]
        if "SELECT min(" in query:
            return [(None,)]
        return []

    def disconnect(self):
        """Match the ClickHouse client lifecycle."""


class LastClient:
    """Capture one `/last` query and return configured rows."""

    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.parameters = None

    def execute(self, query, parameters):
        """Record query details and return fake ClickHouse rows."""
        self.query = query
        self.parameters = parameters
        return self.rows

    def disconnect(self):
        """Match the ClickHouse client lifecycle."""


class LastPaginationTests(unittest.TestCase):
    """Verify `/last` cannot issue unbounded or unstable page requests."""

    @classmethod
    def setUpClass(cls):
        """Import db_svr with isolated manifest storage."""
        cls.tempdir = tempfile.mkdtemp()
        cls.previous_manifest = os.environ.get("REPLICATION_MANIFEST_PATH")
        os.environ["REPLICATION_MANIFEST_PATH"] = os.path.join(
            cls.tempdir, "manifest.sqlite3"
        )
        sys.modules.pop("db_svr", None)
        with patch.object(clickhouse_driver, "Client", BootstrapClient):
            cls.backend = importlib.import_module("db_svr")
        cls.backend.app.config["TESTING"] = True
        cls.http = cls.backend.app.test_client()

    @classmethod
    def tearDownClass(cls):
        """Restore process state and remove isolated storage."""
        if cls.previous_manifest is None:
            os.environ.pop("REPLICATION_MANIFEST_PATH", None)
        else:
            os.environ["REPLICATION_MANIFEST_PATH"] = cls.previous_manifest
        shutil.rmtree(cls.tempdir)

    def test_rejects_invalid_and_unsafe_limits(self):
        """Reject malformed pagination and windows outside recent history."""
        cases = (
            ("?per_page=invalid", "per_page must be an integer"),
            ("?per_page=50001", "per_page exceeds limits"),
            ("?page=1001", "page exceeds limits"),
            ("?for=44641", "for exceeds the 31-day limit"),
        )
        for query, message in cases:
            with self.subTest(query=query):
                response = self.http.get("/getlast" + query)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.get_json()["error"], message)

        old = int(time.time()) - self.backend.LAST_MAX_WINDOW_SECONDS - 1
        response = self.http.get(f"/getlast?since={old}")
        self.assertEqual(response.status_code, 400)

        future = int(time.time()) + 1
        response = self.http.get(f"/getlast?since={future}")
        self.assertEqual(response.status_code, 400)

    def test_legacy_route_remains_available(self):
        """Keep the original streaming route registered for old clients."""
        routes = {rule.rule for rule in self.backend.app.url_map.iter_rules()}
        self.assertIn("/last", routes)
        self.assertIn("/getlast", routes)

    def test_uses_one_ordered_bounded_query(self):
        """Use deterministic ordering and bounded query parameters."""
        client = LastClient([])
        now = int(time.time())
        with patch.object(self.backend, "Client", lambda **_kwargs: client):
            response = self.http.get(
                f"/getlast?since={now - 120}&for=2&page=2&per_page=25"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "ORDER BY t.insert_date ASC, t.chat_id ASC, t.msg_id ASC", client.query
        )
        self.assertEqual(client.parameters["limit"], 26)
        self.assertEqual(client.parameters["offset"], 50)
        payload = json.loads(response.data.decode("utf-8"))
        self.assertEqual(payload["page"], 2)
        self.assertEqual(payload["per_page"], 25)
        self.assertFalse(payload["has_more"])


if __name__ == "__main__":
    unittest.main()
