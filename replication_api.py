"""Bounded replication exports and Flask routes for DarkTroSync."""

import calendar
import hmac
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps

from flask import Blueprint, current_app, jsonify, request

from replication_manifest import ManifestCursorExpired

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MESSAGE_FIELDS = (
    "msg_id",
    "chat_id",
    "chat_name",
    "username",
    "sender_chat_id",
    "title",
    "emitted_at",
    "inserted_at",
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
)


def _positive_integer(value, field_name, allow_zero=False):
    """Parse a bounded API integer and report its field on failure."""
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    minimum = 0 if allow_zero else 1
    if parsed < minimum:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field_name} must be {qualifier}")
    return parsed


def _utc_datetime(value, field_name):
    """Parse an ISO-8601 timestamp and normalize it to UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _subtract_calendar_months(value, months):
    """Subtract whole calendar months while clamping invalid month days."""
    month_index = value.year * 12 + value.month - 1 - months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def validated_cutoff(value, now=None):
    """Return a UTC cutoff constrained to the latest 24 calendar months."""
    current = now or datetime.now(timezone.utc)
    current = current.astimezone(timezone.utc)
    minimum = _subtract_calendar_months(current, 24)
    cutoff = _utc_datetime(value, "emitted_after")
    cutoff = max(cutoff, minimum)
    if cutoff > current:
        raise ValueError("emitted_after cannot be in the future")
    return cutoff


def _json_value(value):
    """Convert ClickHouse values into JSON-safe canonical values."""
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return aware.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


@dataclass(frozen=True)
class ReplicationApiConfig:
    """Limits and credentials for the internal replication interface."""

    api_key: str
    consumer: str = "darktrosync"
    max_manifest_page: int = 1_000
    max_export_page: int = 10_000
    max_ranges: int = 100
    retention_days: int = 90
    cleanup_batch_size: int = 10_000


@dataclass(frozen=True)
class ReplicationQueryConfig:
    """ClickHouse location and hard export limits."""

    database_name: str
    table_name: str
    max_export_page: int = 10_000
    max_ranges: int = 100
    query_timeout_seconds: int = 30


@dataclass(frozen=True)
class FullExportSpec:
    """Validated full-export request values."""

    cutoff: datetime
    limit: int
    cursor: dict
    channel_id: object


@dataclass(frozen=True)
class RangeExportSpec:
    """Validated range-export request values."""

    cutoff: datetime
    ranges: list
    limit: int
    cursor: dict


@dataclass
class RangePageState:
    """Mutable result state while traversing commit ranges."""

    results: list = field(default_factory=list)
    completed_seqs: list = field(default_factory=list)
    next_cursor: object = None
    has_more: bool = False


class ReplicationQueryService:
    """Execute bounded, version-aware exports against the master table."""

    def __init__(
        self,
        client_factory,
        metadata_provider,
        config,
    ):
        self.client_factory = client_factory
        self.metadata_provider = metadata_provider
        self.database_name = self._identifier(config.database_name, "database")
        self.table_name = self._identifier(config.table_name, "table")
        self.max_export_page = _positive_integer(
            config.max_export_page, "max_export_page"
        )
        self.max_ranges = _positive_integer(config.max_ranges, "max_ranges")
        self.query_timeout_seconds = _positive_integer(
            config.query_timeout_seconds, "query_timeout_seconds"
        )

    @staticmethod
    def _identifier(value, field_name):
        normalized = str(value or "")
        if not IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError(f"Invalid ClickHouse {field_name} identifier")
        return normalized

    def _metadata(self):
        metadata = self.metadata_provider()
        date_column = self._identifier(metadata["date_column"], "date column")
        insert_column = self._identifier(
            metadata["insert_date_column"], "insert date column"
        )
        columns = set(metadata.get("columns") or ())
        version_expression = (
            "version"
            if "version" in columns
            else f"toUInt64(toUnixTimestamp({insert_column}))"
        )
        return date_column, insert_column, version_expression

    def metadata_available(self):
        """Return whether replication metadata can be resolved safely."""
        try:
            self._metadata()
        except (KeyError, ValueError):
            return False
        return True

    def _projection(self, date_column, insert_column, version_expression):
        return f"""
            msg_id, chat_id, chat_name, username, sender_chat_id, title,
            {date_column} AS emitted_at,
            {insert_column} AS inserted_at,
            document_present, document_name, document_type, document_size,
            msg_fwd, msg_fwd_username, msg_fwd_title, msg_fwd_id,
            text, lang, urls, hashtags,
            {version_expression} AS version
        """

    def _execute(self, query, parameters):
        client = self.client_factory()
        try:
            return client.execute(
                query,
                parameters,
                settings={"max_execution_time": self.query_timeout_seconds},
            )
        finally:
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                disconnect()

    @staticmethod
    def _records(rows):
        return [
            {field: _json_value(value) for field, value in zip(MESSAGE_FIELDS, row)}
            for row in rows
        ]

    def _page_size(self, payload):
        limit = _positive_integer(payload.get("limit", self.max_export_page), "limit")
        if limit > self.max_export_page:
            raise ValueError(f"limit cannot exceed {self.max_export_page}")
        return limit

    @staticmethod
    def _cursor(payload):
        cursor = payload.get("cursor") or {}
        if not isinstance(cursor, dict):
            raise ValueError("cursor must be an object")
        return {
            "chat_id": _positive_integer(
                cursor.get("chat_id", payload.get("after_chat_id", 0)),
                "cursor.chat_id",
                allow_zero=True,
            ),
            "msg_id": _positive_integer(
                cursor.get("msg_id", payload.get("after_msg_id", 0)),
                "cursor.msg_id",
                allow_zero=True,
            ),
            "version": _positive_integer(
                cursor.get("version", payload.get("after_version", 0)),
                "cursor.version",
                allow_zero=True,
            ),
        }

    def _full_spec(self, payload, now):
        """Validate a full-export payload into a compact query specification."""
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        cutoff = validated_cutoff(payload.get("emitted_after"), now=now)
        limit = self._page_size(payload)
        cursor = self._cursor(payload)
        channel_value = payload.get("channel_id")
        channel_id = (
            _positive_integer(channel_value, "channel_id")
            if channel_value is not None
            else None
        )
        return FullExportSpec(cutoff, limit, cursor, channel_id)

    def _full_query(self, spec):
        """Build one parameterized global or targeted full-export query."""
        date_column, insert_column, version_expression = self._metadata()
        projection = self._projection(date_column, insert_column, version_expression)
        parameters = {
            "cutoff": spec.cutoff.isoformat(),
            "after_chat_id": spec.cursor["chat_id"],
            "after_msg_id": spec.cursor["msg_id"],
            "after_version": spec.cursor["version"],
            "query_limit": spec.limit + 1,
        }
        if spec.channel_id is None:
            key_filter = f"""
                AND (
                    chat_id > %(after_chat_id)s
                    OR (chat_id = %(after_chat_id)s AND msg_id > %(after_msg_id)s)
                    OR (
                        chat_id = %(after_chat_id)s
                        AND msg_id = %(after_msg_id)s
                        AND {version_expression} > %(after_version)s
                    )
                )
            """
            order_by = f"chat_id, msg_id, {version_expression}"
        else:
            parameters["channel_id"] = spec.channel_id
            key_filter = f"""
                AND chat_id = %(channel_id)s
                AND (
                    msg_id > %(after_msg_id)s
                    OR (
                        msg_id = %(after_msg_id)s
                        AND {version_expression} > %(after_version)s
                    )
                )
            """
            order_by = f"msg_id, {version_expression}"

        query = f"""
            SELECT {projection}
            FROM {self.database_name}.{self.table_name}
            WHERE {date_column} >= parseDateTimeBestEffort(%(cutoff)s)
            {key_filter}
            ORDER BY {order_by}
            LIMIT %(query_limit)s
        """
        return query, parameters

    def full_export(self, payload, now=None):
        """Return one global or single-channel full-export page."""
        spec = self._full_spec(payload, now)
        query, parameters = self._full_query(spec)
        rows = self._execute(query, parameters)
        has_more = len(rows) > spec.limit
        records = self._records(rows[: spec.limit])
        next_cursor = None
        if records:
            last = records[-1]
            next_cursor = {
                "chat_id": int(last["chat_id"]),
                "msg_id": int(last["msg_id"]),
                "version": int(last["version"]),
            }
        return {
            "results": records,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "cutoff": spec.cutoff.isoformat(),
        }

    def _validated_ranges(self, payload):
        ranges = payload.get("ranges")
        if not isinstance(ranges, list) or not ranges:
            raise ValueError("ranges must be a non-empty list")
        if len(ranges) > self.max_ranges:
            raise ValueError(f"ranges cannot contain more than {self.max_ranges} items")
        normalized = []
        for item in ranges:
            if not isinstance(item, dict):
                raise ValueError("Each range must be an object")
            first_id = _positive_integer(item.get("first_msg_id"), "first_msg_id")
            last_id = _positive_integer(item.get("last_msg_id"), "last_msg_id")
            if first_id > last_id:
                raise ValueError("first_msg_id cannot exceed last_msg_id")
            normalized.append(
                {
                    "seq": _positive_integer(item.get("seq"), "seq"),
                    "chat_id": _positive_integer(item.get("chat_id"), "chat_id"),
                    "first_msg_id": first_id,
                    "last_msg_id": last_id,
                }
            )
        return normalized

    def _range_rows(self, item, cutoff, cursor, limit):
        date_column, insert_column, version_expression = self._metadata()
        projection = self._projection(date_column, insert_column, version_expression)
        query = f"""
            SELECT {projection}
            FROM {self.database_name}.{self.table_name}
            WHERE {date_column} >= parseDateTimeBestEffort(%(cutoff)s)
              AND chat_id = %(chat_id)s
              AND msg_id >= %(first_msg_id)s
              AND msg_id <= %(last_msg_id)s
              AND (
                  msg_id > %(after_msg_id)s
                  OR (
                      msg_id = %(after_msg_id)s
                      AND {version_expression} > %(after_version)s
                  )
              )
            ORDER BY msg_id, {version_expression}
            LIMIT %(query_limit)s
        """
        parameters = {
            "cutoff": cutoff.isoformat(),
            "chat_id": item["chat_id"],
            "first_msg_id": item["first_msg_id"],
            "last_msg_id": item["last_msg_id"],
            "after_msg_id": cursor["msg_id"],
            "after_version": cursor["version"],
            "query_limit": limit + 1,
        }
        return self._execute(query, parameters)

    def _range_spec(self, payload, now):
        """Validate a range-export payload and cursor."""
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        cutoff = validated_cutoff(payload.get("emitted_after"), now=now)
        ranges = self._validated_ranges(payload)
        limit = self._page_size(payload)
        cursor = payload.get("cursor") or {}
        if not isinstance(cursor, dict):
            raise ValueError("cursor must be an object")
        range_index = _positive_integer(
            cursor.get("range_index", 0), "cursor.range_index", allow_zero=True
        )
        if range_index >= len(ranges):
            raise ValueError("cursor.range_index is outside ranges")
        normalized_cursor = {
            "range_index": range_index,
            "msg_id": _positive_integer(
                cursor.get("msg_id", 0), "cursor.msg_id", allow_zero=True
            ),
            "version": _positive_integer(
                cursor.get("version", 0), "cursor.version", allow_zero=True
            ),
        }
        return RangeExportSpec(cutoff, ranges, limit, normalized_cursor)

    def _collect_range_page(self, spec):
        """Traverse validated ranges until a bounded page is complete."""
        state = RangePageState()
        start_index = spec.cursor["range_index"]
        for index in range(start_index, len(spec.ranges)):
            item = spec.ranges[index]
            cursor = (
                spec.cursor if index == start_index else {"msg_id": 0, "version": 0}
            )
            remaining = spec.limit - len(state.results)
            if remaining == 0:
                state.next_cursor = {
                    "range_index": index,
                    "msg_id": 0,
                    "version": 0,
                }
                state.has_more = True
                break
            rows = self._range_rows(item, spec.cutoff, cursor, remaining)
            range_has_more = len(rows) > remaining
            records = self._records(rows[:remaining])
            for record in records:
                record["manifest_seq"] = item["seq"]
            state.results.extend(records)
            if range_has_more:
                last = records[-1]
                state.next_cursor = {
                    "range_index": index,
                    "msg_id": int(last["msg_id"]),
                    "version": int(last["version"]),
                }
                state.has_more = True
                break
            state.completed_seqs.append(item["seq"])
        return state

    def range_export(self, payload, now=None):
        """Return one bounded page across ordered manifest ranges."""
        spec = self._range_spec(payload, now)
        state = self._collect_range_page(spec)
        return {
            "results": state.results,
            "completed_seqs": state.completed_seqs,
            "has_more": state.has_more,
            "next_cursor": state.next_cursor,
            "cutoff": spec.cutoff.isoformat(),
        }


def create_replication_blueprint(manifest_store, query_service, config):
    """Create the authenticated internal replication Flask blueprint."""
    blueprint = Blueprint("replication", __name__, url_prefix="/replication")

    def authenticated(view_function):
        @wraps(view_function)
        def wrapped(*args, **kwargs):
            if not config.api_key:
                return jsonify({"error": "Replication API is not configured"}), 503
            authorization = request.headers.get("Authorization", "")
            expected = f"Bearer {config.api_key}"
            if not hmac.compare_digest(authorization, expected):
                return jsonify({"error": "Not authorized"}), 401
            return view_function(*args, **kwargs)

        return wrapped

    @blueprint.errorhandler(ManifestCursorExpired)
    def cursor_expired(error):
        return (
            jsonify(
                {
                    "error": str(error),
                    "earliest_retained_seq": error.earliest_seq,
                }
            ),
            410,
        )

    @blueprint.errorhandler(ValueError)
    def invalid_request(error):
        return jsonify({"error": str(error)}), 400

    @blueprint.route("/head", methods=["GET"])
    @authenticated
    def manifest_head():
        return jsonify(manifest_store.head())

    @blueprint.route("/commits", methods=["GET"])
    @authenticated
    def manifest_commits():
        after_seq = _positive_integer(
            request.args.get("after_seq", 0), "after_seq", allow_zero=True
        )
        limit = _positive_integer(
            request.args.get("limit", config.max_manifest_page), "limit"
        )
        if limit > config.max_manifest_page:
            raise ValueError(f"limit cannot exceed {config.max_manifest_page}")
        return jsonify(manifest_store.commits_after(after_seq, limit))

    @blueprint.route("/messages/full", methods=["POST"])
    @authenticated
    def full_export():
        return jsonify(query_service.full_export(request.get_json(silent=True)))

    @blueprint.route("/messages/ranges", methods=["POST"])
    @authenticated
    def range_export():
        return jsonify(query_service.range_export(request.get_json(silent=True)))

    @blueprint.route("/ack", methods=["POST"])
    @authenticated
    def acknowledge():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        seq = _positive_integer(payload.get("seq"), "seq", allow_zero=True)
        stored = manifest_store.acknowledge(config.consumer, seq)
        deleted = manifest_store.cleanup(
            retention_days=config.retention_days,
            batch_size=config.cleanup_batch_size,
        )
        status = manifest_store.status()
        current_app.logger.info(
            "Replication manifest rows=%d seq=%s..%s size=%d ack=%d deleted=%d",
            status["commit_count"],
            status["earliest_seq"],
            status["latest_seq"],
            status["database_size"],
            stored,
            deleted,
        )
        return jsonify(
            {"consumer": config.consumer, "last_ack_seq": stored, "deleted": deleted}
        )

    @blueprint.route("/status", methods=["GET"])
    @authenticated
    def manifest_status():
        status = manifest_store.status()
        status["clickhouse_available"] = query_service.metadata_available()
        return jsonify(status)

    @blueprint.errorhandler(Exception)
    def internal_error(error):
        current_app.logger.exception("Replication API request failed", exc_info=error)
        return jsonify({"error": "Replication backend failure"}), 500

    return blueprint
