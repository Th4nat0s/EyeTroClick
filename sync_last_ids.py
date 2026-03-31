#!/usr/bin/env python3
# coding=utf-8

"""Synchronise les bornes de messages Telegram depuis ClickHouse vers le backend."""

import argparse
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import requests
import yaml
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError


LOG_FORMAT = "%(levelname)s %(message)s"
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(THIS_DIR, "gn_config.yaml")
MAX_BATCH_SIZE = 10_000


def load_config(config_path: str) -> Dict[str, object]:
    """Load and validate the YAML configuration file."""
    with open(config_path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}

    required_keys = [
        "clickhouse_host",
        "clickhouse_port",
        "database_name",
        "table_name",
        "api_key",
        "tagch",
    ]
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        raise ValueError(f"Missing config keys: {', '.join(missing)}")
    return config


def resolve_message_date_column(config: Dict[str, object]) -> str:
    """Detect the ClickHouse message timestamp column."""
    client = Client(
        host=config["clickhouse_host"],
        port=config["clickhouse_port"],
    )
    query = """
        SELECT name
        FROM system.columns
        WHERE database = %(database)s
          AND table = %(table)s
          AND name IN ('date', 'date_utc')
    """
    params = {
        "database": config["database_name"],
        "table": config["table_name"],
    }

    try:
        rows = client.execute(query, params)
    finally:
        client.disconnect()

    available = {row[0] for row in rows}
    if "date" in available:
        return "date"
    if "date_utc" in available:
        return "date_utc"
    raise ValueError("Unable to detect message date column: expected date or date_utc")


def format_backend_datetime(value: object) -> str:
    """Format a ClickHouse datetime value for the backend API."""
    if isinstance(value, datetime):
        dt_value = value
    else:
        normalized = str(value).strip().replace(" ", "T")
        dt_value = datetime.fromisoformat(normalized)

    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    else:
        dt_value = dt_value.astimezone(timezone.utc)

    return dt_value.isoformat(timespec="seconds")


def fetch_last_ids_batch(
    config: Dict[str, object],
    date_column: str,
    after_telegram_id: int = 0,
    telegram_id: Optional[int] = None,
    batch_size: int = MAX_BATCH_SIZE,
) -> List[Dict[str, object]]:
    """Fetch one batch of consolidated first/last message stats."""
    if batch_size < 1 or batch_size > MAX_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")

    client = Client(
        host=config["clickhouse_host"],
        port=config["clickhouse_port"],
    )
    params = {}
    where_parts = []
    if telegram_id is not None:
        where_parts.append("abs(chat_id) = %(telegram_id)s")
        params["telegram_id"] = telegram_id
    else:
        where_parts.append("abs(chat_id) > %(after_telegram_id)s")
        params["after_telegram_id"] = after_telegram_id

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)
    params["batch_size"] = batch_size

    query = f"""
        SELECT
            abs(chat_id) AS telegram_id,
            argMax(chat_name, msg_id) AS chat_name,
            min(msg_id) AS first_id,
            argMin({date_column}, msg_id) AS first_msg,
            max(msg_id) AS last_id,
            argMax({date_column}, msg_id) AS last_msg
        FROM {config["database_name"]}.{config["table_name"]}
        {where_clause}
        GROUP BY telegram_id
        ORDER BY telegram_id
        LIMIT %(batch_size)s
    """

    try:
        rows = client.execute(query, params)
    finally:
        client.disconnect()

    return [
        {
            "telegram_id": int(row[0]),
            "chat_name": row[1] or "",
            "first_id": int(row[2]),
            "first_msg": format_backend_datetime(row[3]),
            "last_id": int(row[4]),
            "last_msg": format_backend_datetime(row[5]),
        }
        for row in rows
        if (
            row[0] is not None
            and row[2] is not None
            and row[3] is not None
            and row[4] is not None
            and row[5] is not None
        )
    ]


def fetch_last_ids(
    config: Dict[str, object],
    date_column: str,
    telegram_id: Optional[int] = None,
    batch_size: int = MAX_BATCH_SIZE,
    limit: Optional[int] = None,
) -> Iterator[List[Dict[str, object]]]:
    """Yield batches of telegram_id/first+last message stats."""
    if telegram_id is not None:
        rows = fetch_last_ids_batch(
            config,
            date_column=date_column,
            telegram_id=telegram_id,
            batch_size=1,
        )
        if rows:
            yield rows
        return

    processed = 0
    after_telegram_id = 0

    while True:
        remaining = None if limit is None else max(limit - processed, 0)
        if remaining == 0:
            return

        current_batch_size = batch_size if remaining is None else min(batch_size, remaining)
        rows = fetch_last_ids_batch(
            config,
            date_column=date_column,
            after_telegram_id=after_telegram_id,
            batch_size=current_batch_size,
        )
        if not rows:
            return

        yield rows
        processed += len(rows)
        after_telegram_id = rows[-1]["telegram_id"]


def build_uri(row: Dict[str, object]) -> str:
    """Build a backend-compatible Telegram URI for a channel row."""
    chat_name = (row.get("chat_name") or "").strip()
    if chat_name:
        return f"https://t.me/{chat_name}"

    # Compat avec le backend actuel: il exige la présence de "uri"
    # même quand telegram_id est fourni.
    return f"https://t.me/{row['telegram_id']}"


def iter_payloads(
    rows: Iterable[Dict[str, object]],
    api_key: str,
) -> Iterator[Dict[str, object]]:
    """Convert ClickHouse rows into backend payloads."""
    for row in rows:
        yield {
            "api_key": api_key,
            "uri": build_uri(row),
            "telegram_id": str(row["telegram_id"]),
            "first_msg": row["first_msg"],
            "last_id": row["last_id"],
            "last_msg": row["last_msg"],
            "first_blood": True,
            "touch_last_seen": False,
            "type": "Telegram",
        }


def push_last_ids(
    config: Dict[str, object],
    rows: List[Dict[str, object]],
    dry_run: bool = False,
) -> Tuple[int, int]:
    """Push one batch of last_id updates to the backend."""
    success = 0
    failure = 0
    session = requests.Session()

    for payload in iter_payloads(rows, config["api_key"]):
        if dry_run:
            logging.info("DRY-RUN %s", payload)
            success += 1
            continue

        try:
            response = session.post(
                config["tagch"],
                json=payload,
                timeout=(10, 60),
            )
        except requests.RequestException as exc:
            failure += 1
            logging.error(
                "Push failed for telegram_id=%s: %s",
                payload["telegram_id"],
                exc,
            )
            continue

        if response.status_code != 200:
            failure += 1
            logging.error(
                "Push failed for telegram_id=%s: HTTP %s %s",
                payload["telegram_id"],
                response.status_code,
                response.text.strip(),
            )
            continue

        success += 1
        logging.info(
            "Updated telegram_id=%s last_id=%s",
            payload["telegram_id"],
            payload["last_id"],
        )

    session.close()
    return success, failure


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Consolide les last_id Telegram depuis ClickHouse vers le backend.",
    )
    parser.add_argument(
        "--config",
        default=CONFIG_PATH,
        help="Chemin vers le fichier gn_config.yaml",
    )
    parser.add_argument(
        "--telegram-id",
        type=int,
        help="Limite la synchronisation a un seul telegram_id",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche les payloads sans envoyer de requetes HTTP",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=MAX_BATCH_SIZE,
        help=f"Taille de batch par lot, max {MAX_BATCH_SIZE}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limite le nombre de telegram_id traites",
    )
    return parser.parse_args()


def main() -> int:
    """Run the last_id consolidation job."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    args = parse_args()

    try:
        config = load_config(args.config)
        date_column = resolve_message_date_column(config)
        if args.batch_size < 1 or args.batch_size > MAX_BATCH_SIZE:
            raise ValueError(
                f"--batch-size must be between 1 and {MAX_BATCH_SIZE}"
            )
        batches = fetch_last_ids(
            config,
            date_column=date_column,
            telegram_id=args.telegram_id,
            batch_size=args.batch_size,
            limit=args.limit,
        )
    except (OSError, ValueError, yaml.YAMLError, ClickHouseError) as exc:
        logging.error("%s", exc)
        return 1

    total_success = 0
    total_failure = 0
    batch_count = 0
    found_any = False

    try:
        for rows in batches:
            found_any = True
            batch_count += 1
            logging.info(
                "Processing batch %s with %s telegram_id.",
                batch_count,
                len(rows),
            )
            success, failure = push_last_ids(config, rows, dry_run=args.dry_run)
            total_success += success
            total_failure += failure
    except ClickHouseError as exc:
        logging.error("%s", exc)
        return 1

    if not found_any:
        logging.info("No telegram_id found to sync.")
        return 0

    logging.info(
        "Sync completed: %s success, %s failure across %s batch(es).",
        total_success,
        total_failure,
        batch_count,
    )
    return 0 if total_failure == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
