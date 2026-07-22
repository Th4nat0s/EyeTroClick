"""Tests for targeted ClickHouse channel-boundary synchronization."""

from datetime import datetime
from unittest.mock import Mock, patch

import sync_last_ids


def config():
    """Return minimal ClickHouse boundary-sync configuration."""
    return {
        "clickhouse_host": "localhost",
        "clickhouse_port": 9000,
        "database_name": "mytme",
        "table_name": "lesmsg",
    }


def test_fetch_last_ids_for_ids_uses_absolute_ids_and_boundaries():
    """Fetch requested signed IDs and return canonical message boundaries."""
    client = Mock()
    client.execute.return_value = [
        (123, "channel", 4, datetime(2026, 7, 1), 9, datetime(2026, 7, 2))
    ]
    with patch.object(sync_last_ids, "Client", return_value=client):
        rows = sync_last_ids.fetch_last_ids_for_ids(config(), "date", [-123, 456])

    assert rows[0]["telegram_id"] == 123
    assert rows[0]["first_id"] == 4
    assert rows[0]["last_id"] == 9
    query_params = client.execute.call_args.args[1]
    assert query_params["telegram_ids"] == (123, 456)


def test_fetch_last_ids_for_ids_skips_empty_request():
    """Avoid opening a ClickHouse connection for no requested channels."""
    with patch.object(sync_last_ids, "Client") as client:
        assert sync_last_ids.fetch_last_ids_for_ids(config(), "date", []) == []
    client.assert_not_called()
