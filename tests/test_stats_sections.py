"""Tests for sectioned global statistics responses."""

import ast
import unittest
from pathlib import Path


SOURCE = Path("db_svr.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
FUNCTION = next(
    node
    for node in TREE.body
    if isinstance(node, ast.FunctionDef) and node.name == "_stats_section"
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
