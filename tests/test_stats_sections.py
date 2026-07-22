"""Tests for sectioned global statistics responses."""

import ast
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, request


SOURCE = Path("db_svr.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
FUNCTION = next(
    node
    for node in TREE.body
    if isinstance(node, ast.FunctionDef) and node.name == "_stats_section"
)
MONTHLY_QUERY_HELPER = next(
    node
    for node in TREE.body
    if isinstance(node, ast.FunctionDef)
    and node.name == "_channel_monthly_stats_query"
)
CHANNEL_STATS_FUNCTION = next(
    node
    for node in TREE.body
    if isinstance(node, ast.FunctionDef) and node.name == "get_stats_chan"
)
MESSAGE_HELPER = next(
    node
    for node in TREE.body
    if isinstance(node, ast.FunctionDef) and node.name == "message_rows_to_dicts"
)


class FakeClient:
    """Record section queries without requiring ClickHouse."""

    def __init__(self):
        self.queries = []

    def execute(self, query, _params):
        self.queries.append(query)
        if "countDistinct" in query:
            return [(3,)]
        if "count(msg_id)" in query:
            return [(12,)]
        return [("value",)]

    def disconnect(self):
        """Match the ClickHouse client lifecycle used by the endpoint."""


class ChannelStatsClient:
    """Return controlled channel and monthly statistics rows."""

    def __init__(self, monthly_rows):
        self.monthly_rows = monthly_rows

    def execute(self, query, _params):
        """Return rows appropriate to each channel-statistics query."""
        if "SELECT chat_id" in query:
            return [(1001385580796,)]
        if "toStartOfMonth" in query:
            return self.monthly_rows
        return []

    def disconnect(self):
        """Match the ClickHouse client lifecycle used by the endpoint."""


class StatsSectionsTest(unittest.TestCase):
    """Verify section selection avoids unrelated statistics queries."""

    def setUp(self):
        self.client = FakeClient()
        namespace = {
            "Client": lambda **_kwargs: self.client,
            "clickhouse_host": "localhost",
            "clickhouse_port": 9000,
            "database_name": "db",
            "table_name": "messages",
            "DATE_COLUMN": "date",
            "INSERT_DATE_COLUMN": "insert_date",
        }
        exec(
            compile(
                ast.Module(body=[FUNCTION], type_ignores=[]),
                "db_svr.py",
                "exec",
            ),
            namespace,
        )
        self.stats_section = namespace["_stats_section"]

    def channel_stats(self, monthly_rows):
        """Execute channel stats endpoint with controlled ClickHouse rows."""
        client = ChannelStatsClient(monthly_rows)
        flask_app = Flask(__name__)
        namespace = {
            "app": flask_app,
            "Client": lambda **_kwargs: client,
            "clickhouse_host": "localhost",
            "clickhouse_port": 9000,
            "database_name": "db",
            "table_name": "messages",
            "DATE_COLUMN": "date",
            "date": date,
            "datetime": datetime,
            "timedelta": timedelta,
            "request": request,
            "jsonify": jsonify,
            "_channel_monthly_stats_query": lambda chat_id: (
                f"SELECT toStartOfMonth(date) FROM messages WHERE chat_id = {chat_id}"
            ),
        }
        exec(
            compile(
                ast.Module(body=[CHANNEL_STATS_FUNCTION], type_ignores=[]),
                "db_svr.py",
                "exec",
            ),
            namespace,
        )
        with flask_app.test_request_context(
            "/get_stats_chan?chan_name=1001385580796"
        ):
            return namespace["get_stats_chan"]().get_json()

    def test_monthly_query_includes_historical_published_messages(self):
        """Monthly channel stats must not apply a rolling 24-month cutoff."""
        namespace = {
            "DATE_COLUMN": "date",
            "database_name": "db",
            "table_name": "messages",
        }
        exec(
            compile(
                ast.Module(body=[MONTHLY_QUERY_HELPER], type_ignores=[]),
                "db_svr.py",
                "exec",
            ),
            namespace,
        )

        query = namespace["_channel_monthly_stats_query"](1001385580796)

        self.assertIn("WHERE chat_id = 1001385580796", query)
        self.assertIn("toStartOfMonth(date)", query)
        self.assertNotIn("subtractMonths", query)

    def test_historical_only_monthly_result_is_preserved(self):
        """Historical-only channels remain represented by monthly rows."""
        monthly_rows = [("2022-04-01", "2022/04", 1)]
        result = self.channel_stats(monthly_rows)
        self.assertEqual(result["monthly"], [list(row) for row in monthly_rows])

    def test_mixed_monthly_result_is_preserved(self):
        """Mixed historical and recent months remain available together."""
        monthly_rows = [("2026-07-01", "2026/07", 2), ("2022-04-01", "2022/04", 1)]
        result = self.channel_stats(monthly_rows)
        self.assertEqual(result["monthly"], [list(row) for row in monthly_rows])

    def test_empty_monthly_result_remains_empty(self):
        """Empty channels still return an empty monthly series."""
        result = self.channel_stats([])
        self.assertEqual(result["monthly"], [])
        self.assertEqual(len(result["daily"]), 31)
        self.assertEqual(len(result["hourly"]), 24)

    def test_summary_does_not_execute_chart_queries(self):
        """Summary requests execute only summary queries."""
        result = self.stats_section("summary")

        self.assertEqual(result, {"chats": (3,), "msgs": (12,)})
        self.assertEqual(len(self.client.queries), 2)

    def test_unknown_section_is_rejected(self):
        """Unknown sections fail before any database query."""
        with self.assertRaises(ValueError):
            self.stats_section("unknown")
        self.assertEqual(self.client.queries, [])

    def test_message_rows_keep_id_and_timestamps(self):
        """Search responses expose fields required by the channel message view."""
        namespace = {
            "MESSAGE_RESULT_FIELDS": [
                "id",
                "chat_id",
                "date",
                "insert_date",
                "text",
            ]
        }
        exec(
            compile(
                ast.Module(body=[MESSAGE_HELPER], type_ignores=[]),
                "db_svr.py",
                "exec",
            ),
            namespace,
        )

        result = namespace["message_rows_to_dicts"](
            [
                (
                    192,
                    1001385580796,
                    "2022-04-05T22:01:56+00:00",
                    "2025-02-06T23:08:19+00:00",
                    "text",
                )
            ]
        )

        self.assertEqual(result[0]["id"], 192)
        self.assertEqual(result[0]["date"], "2022-04-05T22:01:56+00:00")
        self.assertEqual(result[0]["insert_date"], "2025-02-06T23:08:19+00:00")


if __name__ == "__main__":
    unittest.main()
