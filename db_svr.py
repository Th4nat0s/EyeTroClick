#!/usr/bin/env python3
# coding=utf-8

from flask import Flask, request, jsonify, Response
from clickhouse_driver import Client
from datetime import datetime, timedelta, date
from collections import defaultdict
import time
import os
import yaml
import json
import logging
import hashlib
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode

app = Flask(__name__)
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(THIS_DIR, "./gn_config.yaml")) as f:
    gn_config = yaml.safe_load(f)

# Configuration de la connexion
# clickhouse_host = 'localhost'
clickhouse_host = gn_config.get("clickhouse_host")
clickhouse_port = gn_config.get("clickhouse_port")
app_port = gn_config.get("app_port")
database_name = gn_config.get("database_name")
table_name = gn_config.get("table_name")
logger = logging.getLogger(__name__)


# Liste des colonnes valides pour éviter les injections SQL
valid_fields = [
    "id",
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
]

table_columns = set()
DATE_COLUMN = "date"
INSERT_DATE_COLUMN = "insert_date"
field_aliases = {}
queryable_fields = set(valid_fields)
star = ""
_last_metadata_refresh = 0.0
METADATA_REFRESH_INTERVAL = 60
_earliest_date = None
FORCE_EXACT_FIELDS = {"chat_id", "username_sender_exact"}
FORCE_INTEGER_FIELDS = {"chat_id", "username_sender_exact"}


def _normalize_iso_datetime(value: str) -> str:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return value


def _to_aware_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    normalized = _normalize_iso_datetime(str(value))
    return datetime.fromisoformat(normalized)


def refresh_earliest_date():
    global _earliest_date
    client = None
    try:
        client = Client(host=clickhouse_host, port=clickhouse_port)
        query = f"SELECT min({DATE_COLUMN}) FROM {database_name}.{table_name}"
        result = client.execute(query)
        min_date = result[0][0] if result and result[0] else None
        if isinstance(min_date, datetime):
            _earliest_date = _to_aware_datetime(min_date)
        elif min_date is not None:
            _earliest_date = _to_aware_datetime(str(min_date))
        else:
            _earliest_date = None
    except Exception as exc:
        logger.warning("Unable to fetch earliest date: %s", exc)
        _earliest_date = None
    finally:
        if client:
            client.disconnect()


def get_earliest_date():
    if _earliest_date is None:
        refresh_earliest_date()
    return _earliest_date


def introspect_table_columns():
    meta_client = None
    try:
        meta_client = Client(host=clickhouse_host, port=clickhouse_port)
        rows = meta_client.execute(
            "SELECT name FROM system.columns WHERE database = %(db)s AND table = %(tbl)s",
            {"db": database_name, "tbl": table_name},
        )
        return {row[0] for row in rows}
    except Exception as exc:
        logger.warning(
            "Unable to introspect schema for %s.%s: %s", database_name, table_name, exc
        )
        return set()
    finally:
        if meta_client:
            try:
                meta_client.disconnect()
            except Exception:
                pass


def refresh_table_metadata():
    global table_columns, DATE_COLUMN, INSERT_DATE_COLUMN, field_aliases, queryable_fields, star, _last_metadata_refresh
    columns = introspect_table_columns()
    if not columns:
        columns = set()
    table_columns = columns
    DATE_COLUMN = (
        "date"
        if "date" in columns or not columns
        else ("date_utc" if "date_utc" in columns else "date")
    )
    INSERT_DATE_COLUMN = (
        "insert_date"
        if "insert_date" in columns or not columns
        else (
            "insert_date_utc"
            if "insert_date_utc" in columns
            else "insert_date"
        )
    )
    field_aliases = {
        "date": DATE_COLUMN,
        "insert_date": INSERT_DATE_COLUMN,
        DATE_COLUMN: DATE_COLUMN,
        INSERT_DATE_COLUMN: INSERT_DATE_COLUMN,
        "username_sender_exact": "sender_chat_id",
        "chatname": "chat_name",
    }
    queryable_fields = set(valid_fields) | set(field_aliases.keys())
    star_clause = f""" msg_id,
        chat_id,
        chat_name,
        username,
        sender_chat_id,
        title,
        formatDateTime(toTimeZone({DATE_COLUMN}, 'UTC'), '%%Y-%%m-%%dT%%H:%%i:%%S+00:00') AS date,
        formatDateTime(toTimeZone({INSERT_DATE_COLUMN}, 'UTC'), '%%Y-%%m-%%dT%%H:%%i:%%S+00:00') AS insert_date,
        document_present,
        document_name,
        document_type,
        document_size,
        msg_fwd,
        msg_fwd_username,
        msg_fwd_title,
        msg_fwd_id,
        text,
        lang,
        urls,
        hashtags"""
    star = star_clause
    _last_metadata_refresh = time.time()
    refresh_earliest_date()


def ensure_table_metadata(force=False):
    global _last_metadata_refresh
    if force or not table_columns:
        refresh_table_metadata()
        return
    if time.time() - _last_metadata_refresh > METADATA_REFRESH_INTERVAL:
        refresh_table_metadata()


refresh_table_metadata()


@app.before_request
def sync_metadata():
    ensure_table_metadata()


def should_refresh_schema(error: Exception) -> bool:
    message = str(error).lower()
    keywords = ("unknown expression", "unknown identifier", "unknown column")
    return any(keyword in message for keyword in keywords)


def _execute_search_once(
    field,
    raw_value,
    method,
    count,
    *,
    upper_bound=None,
    lower_bound=None,
    fetch_extra=False,
):
    start_time = time.time()
    client = Client(host=clickhouse_host, port=clickhouse_port)
    query_limit = count + 1 if fetch_extra else count
    db_field = field_aliases.get(field, field)
    effective_method = method or "ILIKE"
    if field in FORCE_EXACT_FIELDS or db_field == "chat_id":
        effective_method = "IS"
    method_lower = effective_method.lower()

    arrayquery = db_field in ("urls", "hashtags")
    numerical = db_field in ("chat_id", "sender_chat_id") or field in FORCE_INTEGER_FIELDS
    params = {}
    svalue = "%(value)s"
    bound_value = raw_value

    if numerical:
        svalue = "%(value)i"
        try:
            bound_value = int(raw_value)
        except (TypeError, ValueError):
            client.disconnect()
            raise ValueError("chat_id must be an integer")

    table_alias = "t"
    if arrayquery:
        base_query = f"SELECT {star} FROM {database_name}.{table_name} AS {table_alias} WHERE "
        if method_lower == "like":
            base_query += f"arrayExists(u -> u LIKE {svalue}, {table_alias}.{db_field})"
        elif method_lower == "ilike":
            base_query += f"arrayExists(u -> positionCaseInsensitiveUTF8(u, {svalue}) > 0 , {table_alias}.{db_field})"
        else:
            base_query += f"arrayExists(u -> u = {svalue}, {table_alias}.{db_field})"
    else:
        if method_lower == "like":
            base_query = f"SELECT {star} FROM {database_name}.{table_name} AS {table_alias} WHERE {table_alias}.{db_field} LIKE {svalue}"
        elif method_lower == "ilike":
            base_query = f"SELECT {star} FROM {database_name}.{table_name} AS {table_alias} WHERE positionCaseInsensitiveUTF8({table_alias}.{db_field}, {svalue}) >0"
        else:
            base_query = f"SELECT {star} FROM {database_name}.{table_name} AS {table_alias} WHERE {table_alias}.{db_field} = {svalue}"

    date_filter_clause = ""
    if upper_bound:
        normalized = _normalize_iso_datetime(upper_bound)
        try:
            datetime.fromisoformat(normalized)
        except ValueError:
            client.disconnect()
            raise ValueError("before_date must be ISO 8601 formatted")
        params["upper_bound"] = normalized
        date_filter_clause += f" AND {table_alias}.{DATE_COLUMN} < parseDateTimeBestEffort(%(upper_bound)s)"
    if lower_bound:
        normalized_lower = _normalize_iso_datetime(lower_bound)
        try:
            datetime.fromisoformat(normalized_lower)
        except ValueError:
            client.disconnect()
            raise ValueError("lower bound must be ISO 8601 formatted")
        params["lower_bound"] = normalized_lower
        date_filter_clause += f" AND {table_alias}.{DATE_COLUMN} >= parseDateTimeBestEffort(%(lower_bound)s)"

    query = f"{base_query}{date_filter_clause} order by {table_alias}.{DATE_COLUMN} desc limit {query_limit}"
    params["value"] = (
        f"%{bound_value}%"
        if (method_lower == "like" and not numerical)
        else (f"{bound_value}" if not numerical else bound_value)
    )

    try:
        result = client.execute(query, params)
        column_names = valid_fields
        results_dict = [dict(zip(column_names, row)) for row in result]
        len_result = len(result)
        limit_reached = len_result >= query_limit

        if limit_reached:
            has_more = "True"
            if results_dict:
                results_dict = results_dict[:-1]
        else:
            has_more = "False"

        timing = f"{float(time.time() - start_time):.5f}"
        return {"has_more": has_more, "results": results_dict, "timing": timing}
    except Exception as exc:
        print(f"error: {exc}, \n {query}")
        raise
    finally:
        client.disconnect()


def perform_search_query(
    field,
    raw_value,
    method,
    count,
    *,
    before_date=None,
    fetch_extra=False,
):
    earliest = get_earliest_date()
    if earliest is None:
        return {"has_more": "False", "results": [], "timing": "0.00000"}

    try:
        cursor = (
            _to_aware_datetime(before_date)
            if before_date
            else datetime.now(timezone.utc)
        )
    except ValueError:
        raise ValueError("before_date must be ISO 8601 formatted")

    if cursor < earliest:
        cursor = earliest

    limit = max(1, count)
    all_results = []
    total_time = 0.0
    has_more = "False"
    safety_guard = 0

    while len(all_results) < limit and cursor >= earliest:
        month_start = cursor.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        window_upper = cursor
        window_cursor = window_upper
        while len(all_results) < limit and window_cursor >= month_start:
            remaining = limit - len(all_results)
            try:
                chunk = _execute_search_once(
                    field,
                    raw_value,
                    method,
                    remaining,
                    upper_bound=window_cursor.isoformat(),
                    lower_bound=month_start.isoformat(),
                    fetch_extra=True,
                )
            except ValueError as exc:
                raise exc
            except Exception as exc:
                if should_refresh_schema(exc) and safety_guard == 0:
                    ensure_table_metadata(force=True)
                    safety_guard += 1
                    continue
                raise

            try:
                total_time += float(chunk.get("timing", "0") or 0)
            except ValueError:
                pass

            chunk_results = chunk.get("results", [])
            if not chunk_results:
                break

            all_results.extend(chunk_results)
            last_date_str = chunk_results[-1].get("date")
            if last_date_str:
                window_cursor = _to_aware_datetime(last_date_str) - timedelta(
                    microseconds=1
                )
            else:
                window_cursor = window_cursor - timedelta(seconds=1)

            if len(all_results) >= limit:
                has_more = "True"
                break

            if chunk.get("has_more") == "True" and window_cursor >= month_start:
                continue
            else:
                break

        if len(all_results) >= limit:
            break

        cursor = month_start - timedelta(microseconds=1)

    if len(all_results) < limit and cursor < earliest:
        has_more = "False"
    elif len(all_results) >= limit:
        has_more = "True"
    else:
        has_more = "True" if cursor >= earliest else "False"

    trimmed_results = all_results[:limit]
    timing = f"{total_time:.5f}"
    next_cursor = trimmed_results[-1]["date"] if has_more == "True" and trimmed_results else None
    return {
        "has_more": has_more,
        "results": trimmed_results,
        "timing": timing,
        "next_cursor": next_cursor,
    }


def convert_dates_to_iso(data):
    for item in data:
        for field in ["date", "insert_date"]:
            if field in item and isinstance(item[field], str):
                try:
                    dt = parsedate_to_datetime(item[field])
                    item[field] = dt.astimezone(timezone.utc).isoformat(
                        timespec="seconds"
                    )
                except Exception as e:
                    print(f"Error converting {field}: {e}")
    return data


def parse_iso8601_flexible(date_str):
    if len(date_str) >= 5 and (date_str[-5] in ["+", "-"]) and date_str[-2:].isdigit():
        # Extrait le décalage horaire
        offset = date_str[-5:]
        # Transforme 'HHMM' en 'HH:MM'
        offset_fixed = offset[:3] + ":" + offset[3:]
        # Remplace dans la date
        date_str = date_str[:-5] + offset_fixed
    return datetime.fromisoformat(date_str)


def convert_record(record):
    """Convertit les champs date et datetime pour chaque record, merci Json"""
    record[6] = parse_iso8601_flexible(
        record[6]
    )  # Conversion de 'date' (7e champ) au format datetime
    record[7] = parse_iso8601_flexible(
        record[7]
    )  # Conversion de 'insert_date' (8e champ) au format datetime
    return record


def serialize_datetime(obj):
    """Convertit les objets datetime en chaînes de caractères."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type non sérialisable")


def valid_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


@app.route("/", methods=["GET"])
def home():
    """Serve a small landing page embedding the Telegram search UI."""
    message_count_text = "loading…"
    chat_room_count_text = "loading…"
    client = None
    try:
        client = Client(host=clickhouse_host, port=clickhouse_port)
        msg_result = client.execute(f"SELECT count() FROM {database_name}.{table_name}")
        room_result = client.execute(
            f"SELECT countDistinct(chat_id) FROM {database_name}.{table_name}"
        )
        if msg_result and msg_result[0]:
            message_count_text = f"{msg_result[0][0]:,}".replace(",", " ")
        if room_result and room_result[0]:
            chat_room_count_text = f"{room_result[0][0]:,}".replace(",", " ")
    except Exception as exc:
        logger.warning("Failed to fetch message count for landing page: %s", exc)
        message_count_text = "unavailable"
        chat_room_count_text = "unavailable"
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass

    html_page = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>Sneak in Telegram Data</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
        :root {
            color-scheme: dark;
        }
        * {
            box-sizing: border-box;
        }
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            background: #0d1117;
            color: #e6edf3;
        }
        a {
            color: #79c0ff;
        }
        .container {
            max-width: 960px;
            margin: 0 auto;
            padding: 2rem 1.5rem 4rem 1.5rem;
        }
        header {
            text-align: center;
            margin-bottom: 2rem;
        }
        header h1 {
            margin-bottom: 0.5rem;
        }
        header p {
            margin: 0.2rem 0;
        }
        form {
            background: #161b22;
            padding: 1.5rem;
            border-radius: 12px;
            border: 1px solid #30363d;
            box-shadow: 0 5px 20px rgba(0, 0, 0, 0.35);
        }
        label {
            font-size: 0.9rem;
        }
        select, input[type="text"], input[type="number"] {
            width: 100%;
            padding: 0.6rem 0.75rem;
            border-radius: 8px;
            border: 1px solid #30363d;
            background: #0d1117;
            color: #e6edf3;
            margin-top: 0.35rem;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 1rem;
        }
        .control-row {
            display: flex;
            gap: 0.5rem;
            margin-top: 1rem;
        }
        .control-row input[type="text"] {
            flex: 1;
        }
        button {
            background: #1f6feb;
            color: #fff;
            border: none;
            border-radius: 8px;
            padding: 0 1.5rem;
            font-size: 1rem;
            cursor: pointer;
        }
        button:hover {
            background: #388bfd;
        }
        .count {
            color: #9be9a8;
            font-weight: bold;
        }
        #results {
            margin-top: 2rem;
        }
        .card {
            border-radius: 12px;
            border: 1px solid #30363d;
            padding: 1rem 1.25rem;
            margin-bottom: 1.5rem;
            background: #161b22;
        }
        .card h3 {
            margin: 0 0 0.35rem 0;
            font-size: 1.05rem;
            color: #79c0ff;
        }
        .card pre {
            white-space: pre-wrap;
            word-break: break-word;
        }
        .tagline {
            font-size: 0.9rem;
            margin-top: 0.2rem;
            color: #8b949e;
        }
        .note {
            color: #ffa657;
            margin-top: 0.75rem;
        }
        .error {
            color: #ffa657;
        }
        code {
            background: #0d1117;
            border-radius: 4px;
            padding: 0.1rem 0.3rem;
        }
        .results-summary {
            margin-top: 1.5rem;
            color: #8b949e;
            font-size: 0.9rem;
        }
        .load-more-btn {
            display: none;
            margin-top: 1rem;
            align-self: flex-start;
            background: #238636;
            color: #fff;
            border: none;
            border-radius: 8px;
            padding: 0.6rem 1.2rem;
            cursor: pointer;
        }
        .load-more-btn:hover {
            background: #2ea043;
        }
        .pagination-controls {
            margin-top: 1rem;
            display: flex;
            align-items: center;
            gap: 1rem;
            width: 100%;
            justify-content: flex-start;
        }
        .loader-slot {
            flex: 1;
            display: flex;
            justify-content: center;
        }
        #loading {
            display: none;
            text-align: center;
        }
        .centered{
            width:120px;
            height:48px;
            position:relative;
            background:transparent;
            filter: blur(0.2px) contrast(2);
        }
        .blob-1,.blob-2{
            width:16px;
            height:16px;
            position:absolute;
            background:#fff;
            border-radius:50%;
            top:5%;
            left:5%;
            transform:translate(-5%,-5%);
        }
        .blob-1{
            left:15%;
            animation:osc-l 2.5s ease infinite;
        }
        .blob-2{
            left:85%;
            animation:osc-r 2.5s ease infinite;
            background:#0ff;
        }
        @keyframes osc-l{
            0%{left:15%;}
            50%{left:50%;}
            100%{left:15%;}
        }
        @keyframes osc-r{
            0%{left:85%;}
            50%{left:50%;}
            100%{left:85%;}
        }
        .modal-backdrop {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.65);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 999;
            visibility: hidden;
            opacity: 0;
            transition: opacity 0.2s ease, visibility 0.2s ease;
        }
        .modal-backdrop.visible {
            visibility: visible;
            opacity: 1;
        }
        .modal {
            background: #161b22;
            padding: 1.5rem;
            border-radius: 10px;
            border: 1px solid #30363d;
            max-width: 320px;
            text-align: center;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
        }
        .modal button {
            margin-top: 1rem;
            width: 100%;
        }
        @media (max-width: 600px) {
            .control-row {
                flex-direction: column;
            }
            button {
                width: 100%;
                padding: 0.75rem;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Sneak into Telegram Data</h1>
            <p>Inspect <span id="messageCount" class="count">__MESSAGE_COUNT__</span> Telegram messages.</p>
            <p class="tagline">On <span id="chatRoomCount" class="count">__CHATROOM_COUNT__</span> collected chats rooms.</p>
        </header>
        <form id="searchForm">
            <div class="grid">
                <div>
                    <label for="field">What</label>
                    <select id="field" name="field" required>
                        <option value="text">Message</option>
                        <option value="chat_name">Chat Name</option>
                        <option value="chat_id">Chat ID</option>
                        <option value="username_sender_exact">User ID</option>
                        <option value="hashtags">HashTags</option>
                        <option value="urls">Url</option>
                        <option value="document_name">Document</option>
                    </select>
                </div>
                <div>
                    <label for="method">How</label>
                    <select id="method" name="method" required>
                        <option value="LIKE">Contains</option>
                        <option value="ILIKE" selected>Insensitive Contains</option>
                        <option value="IS">Exact</option>
                    </select>
                </div>
                <div>
                    <label for="count">Max answers (max 1000)</label>
                    <input type="number" id="count" name="count" value="10" min="1" max="1000" />
                </div>
            </div>
            <div class="control-row">
                <input type="text" id="value" name="value" placeholder="Value to search" required />
                <button type="submit">Rechercher</button>
            </div>
        </form>
        <div id="resultsSummary" class="results-summary"></div>
        <section id="results"></section>
        <div class="pagination-controls">
            <button type="button" id="loadMore" class="load-more-btn">Load more</button>
            <div class="loader-slot">
                <div id="loading">
                    <div class="centered">
                        <div class="blob-1"></div>
                        <div class="blob-2"></div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <div id="modalBackdrop" class="modal-backdrop">
        <div class="modal">
            <p id="modalMessage"></p>
            <button type="button" id="modalClose">Close</button>
        </div>
    </div>
    <script>
    const searchState = {
        basePayload: null,
        nextCursor: null,
        totalLoaded: 0
    };

    function escapeHtml(text) {
        return text.replace(/[&<>"'`=\\/]/g, function (char) {
            return {
                '&': '&amp;',
                '<': '&lt;',
                '>': '&gt;',
                '"': '&quot;',
                "'": '&#039;',
                '/': '&#x2F;',
                '`': '&#x60;',
                '=': '&#x3D;',
                '\\\\': '&#x5C;'
            }[char];
        });
    }
    function formatBytes(bytes) {
        const size = Number(bytes);
        if (!Number.isFinite(size) || size < 0) {
            return '0 bytes';
        }
        const units = ['bytes', 'KB', 'MB', 'GB'];
        let value = size;
        let unitIndex = 0;
        while (value >= 1024 && unitIndex < units.length - 1) {
            value /= 1024;
            unitIndex += 1;
        }
        const formatted = value % 1 === 0 ? value.toFixed(0) : value.toFixed(1);
        return `${formatted} ${units[unitIndex]}`;
    }
    function syncMethodOptions() {
        const fieldSelect = document.getElementById('field');
        const methodSelect = document.getElementById('method');
        const chosenField = fieldSelect.value;
        const requiresExact = chosenField === 'chat_id' || chosenField === 'username_sender_exact';
        for (const option of methodSelect.options) {
            if (requiresExact) {
                option.disabled = option.value !== 'IS';
            } else {
                option.disabled = false;
            }
        }
        if (requiresExact) {
            methodSelect.value = 'IS';
        }
    }
    function showModal(message) {
        const backdrop = document.getElementById('modalBackdrop');
        const modalMessage = document.getElementById('modalMessage');
        modalMessage.textContent = message;
        backdrop.classList.add('visible');
    }
    document.addEventListener('click', (event) => {
        if (event.target.id === 'modalClose') {
            document.getElementById('modalBackdrop').classList.remove('visible');
        }
    });
    function renderResults(data, append = false) {
        const resultsContainer = document.getElementById('results');
        if (!append) {
            resultsContainer.innerHTML = '';
        }
        if (!data || !Array.isArray(data.results) || data.results.length === 0) {
            const message = document.createElement('p');
            message.textContent = append ? 'No more results.' : 'No results found.';
            resultsContainer.appendChild(message);
            return;
        }
        data.results.forEach(result => {
            const card = document.createElement('article');
            card.className = 'card';
            const text = result.text ? escapeHtml(result.text).replace(/\\n/g, '<br>') : '<em>No text</em>';
            const hashtags = Array.isArray(result.hashtags) ? result.hashtags.filter(Boolean).join(' ') : result.hashtags || '';
            const telegramLink = result.chat_name && result.id
                ? `<a target="_blank" rel="noopener" href="https://t.me/${encodeURIComponent(result.chat_name)}/${encodeURIComponent(result.id)}">open in Telegram</a>`
                : '';
            card.innerHTML = `
                <h3>${escapeHtml(String(result.chat_name || 'Unknown'))} &middot; ${escapeHtml(String(result.chat_id || ''))}</h3>
                <p class="tagline">Msg ${escapeHtml(String(result.id || ''))} ${telegramLink ? '&middot; ' + telegramLink : ''}</p>
                <p>On <strong>${result.date || 'n/a'}</strong> (inserted ${result.insert_date || 'n/a'})</p>
                <p><strong>${escapeHtml(String(result.username || 'anonymous'))}</strong> (${escapeHtml(String(result.sender_chat_id || 'unknown'))})</p>
                ${result.title ? `<p><strong>${escapeHtml(String(result.title))}</strong></p>` : ''}
                <pre>${text}</pre>
                ${result.msg_fwd == 1 ? `<p><strong>Forwarded from:</strong> ${escapeHtml(String(result.msg_fwd_title || ''))} (${escapeHtml(String(result.msg_fwd_username || ''))})</p>` : ''}
                ${result.document_present == 1 ? `<p><span aria-hidden="true">📄</span> ${escapeHtml(String(result.document_name || ''))} (${formatBytes(Number(result.document_size) || 0)}, ${escapeHtml(String(result.document_type || 'unknown'))})</p>` : ''}
                ${hashtags ? `<p><strong>${escapeHtml(hashtags)}</strong></p>` : ''}
            `;
            resultsContainer.appendChild(card);
        });
    }
    function updateSummary(lastBatchCount, timing) {
        const summary = document.getElementById('resultsSummary');
        if (searchState.totalLoaded === 0) {
            const timingText = timing ? ` (queried in ${timing}s)` : '';
            summary.textContent = `No results found${timingText}.`;
            return;
        }
        const parts = [`${searchState.totalLoaded} results loaded`];
        if (typeof lastBatchCount === 'number') {
            parts.push(`last batch ${lastBatchCount}`);
        }
        if (timing) {
            parts.push(`${timing}s`);
        }
        summary.textContent = parts.join(' · ');
    }
    function updateLoadMoreButton() {
        const button = document.getElementById('loadMore');
        if (searchState.nextCursor) {
            button.style.display = 'inline-flex';
        } else {
            button.style.display = 'none';
        }
    }
    function resetStateForNewSearch(payload) {
        searchState.basePayload = { ...payload };
        delete searchState.basePayload.before_date;
        searchState.nextCursor = null;
        searchState.totalLoaded = 0;
        document.getElementById('resultsSummary').textContent = '';
        document.getElementById('results').innerHTML = '';
        updateLoadMoreButton();
    }
    async function performSearch(payload, { append = false } = {}) {
        const loading = document.getElementById('loading');
        const resultsContainer = document.getElementById('results');
        loading.style.display = 'block';
        try {
            const response = await fetch('/search_go_telegrams', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });
            if (!response.ok) {
                throw new Error('Search failed');
            }
            const data = await response.json();
            renderResults(data, append);
            const batchCount = Array.isArray(data.results) ? data.results.length : 0;
            if (!append) {
                searchState.totalLoaded = batchCount;
            } else {
                searchState.totalLoaded += batchCount;
            }
            searchState.nextCursor = data.next_cursor || null;
            updateSummary(batchCount, data.timing);
            updateLoadMoreButton();
        } catch (error) {
            if (!append) {
                resultsContainer.innerHTML = '<p class="error">An error occurred during the search.</p>';
                document.getElementById('resultsSummary').textContent = '';
            } else {
                const notice = document.createElement('p');
                notice.className = 'error';
                notice.textContent = 'Could not load more results.';
                resultsContainer.appendChild(notice);
            }
            searchState.nextCursor = null;
            updateLoadMoreButton();
        } finally {
            loading.style.display = 'none';
        }
    }
    function buildPayloadFromForm() {
        const countInput = document.getElementById('count');
        const requestedCount = Math.min(Math.max(parseInt(countInput.value, 10) || 10, 1), 100);
        return {
            field: document.getElementById('field').value,
            method: document.getElementById('method').value,
            value: document.getElementById('value').value.trim(),
            count: requestedCount
        };
    }
    document.addEventListener('DOMContentLoaded', () => {
        syncMethodOptions();
        const fieldSelect = document.getElementById('field');
        const form = document.getElementById('searchForm');
        const loadMoreBtn = document.getElementById('loadMore');
        fieldSelect.addEventListener('change', syncMethodOptions);
        form.addEventListener('submit', async (event) => {
            event.preventDefault();
            const payload = buildPayloadFromForm();
            if (!payload.value) {
                alert('Please enter a value to search.');
                return;
            }
            if ((payload.field === 'chat_id' || payload.field === 'username_sender_exact') && !/^-?\\d+$/.test(payload.value)) {
                showModal('Only integer for chat id.');
                return;
            }
            resetStateForNewSearch(payload);
            await performSearch(payload, { append: false });
        });
        loadMoreBtn.addEventListener('click', async () => {
            if (!searchState.basePayload || !searchState.nextCursor) {
                return;
            }
            loadMoreBtn.disabled = true;
            loadMoreBtn.textContent = 'Loading…';
            const payload = {
                ...searchState.basePayload,
                before_date: searchState.nextCursor
            };
            await performSearch(payload, { append: true });
            loadMoreBtn.disabled = false;
            loadMoreBtn.textContent = 'Load more';
        });
    });
    </script>
</body>
</html>"""
    html_page = html_page.replace("__MESSAGE_COUNT__", message_count_text)
    html_page = html_page.replace("__CHATROOM_COUNT__", chat_room_count_text)
    return Response(html_page, mimetype="text/html")


@app.route("/search_go_telegrams", methods=["POST"])
def search_go_telegrams():
    """
    Bridge endpoint used by the embedded UI.
    It forwards the payload to the dedicated /search_latest route (which itself calls the legacy /search endpoint).
    """
    payload = request.get_json(silent=True) or {}
    field = payload.get("field")
    value = payload.get("value")
    method = payload.get("method", "ILIKE")
    if field in FORCE_EXACT_FIELDS:
        method = "IS"

    if not field or not value:
        return jsonify({"error": "Missing field or value parameter"}), 400

    try:
        requested_count = int(payload.get("count", 100))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid count parameter"}), 400

    if requested_count < 1:
        requested_count = 1
    if requested_count > 1000:
        requested_count = 1000

    limited_count = min(requested_count, 100)
    before_date = payload.get("before_date")
    query_params_dict = {
        "field": field,
        "value": value,
        "method": method,
        "count": limited_count,
    }
    if before_date:
        query_params_dict["before_date"] = before_date
    query_params = urlencode(query_params_dict)

    with app.test_request_context(f"/search_latest?{query_params}", method="GET"):
        proxied_response = search_latest()

    if isinstance(proxied_response, tuple):
        response, status = proxied_response
        return response, status
    return proxied_response


@app.route("/search_latest", methods=["GET"])
def search_latest():
    """
    Wrapper around /search that keeps only the newest 100 hits ordered by the logical 'date' field.
    """
    field = request.args.get("field")
    value = request.args.get("value")
    method = request.args.get("method", "ILIKE")
    count_param = request.args.get("count")
    before_date = request.args.get("before_date")

    if not field or not value:
        return jsonify({"error": "Missing field or value parameter"}), 400

    try:
        limit = int(count_param) if count_param else 10
    except ValueError:
        return jsonify({"error": "Invalid Count"}), 400

    if limit < 1:
        limit = 1
    if limit > 100:
        limit = 100

    if field in FORCE_EXACT_FIELDS:
        method = "IS"

    try:
        payload = perform_search_query(
            field, value, method, limit, before_date=before_date, fetch_extra=True
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        print(f"error: {exc}")
        return jsonify({"error": str(exc)}), 500

    return jsonify(payload)


# Route pour les requêtes de recherche
@app.route("/search", methods=["GET"])
def search():

    field = request.args.get("field")
    raw_value = request.args.get("value")
    method = request.args.get("method")
    count_param = request.args.get("count")

    try:
        count = int(count_param) if count_param else 10
        if count > 1001:
            return jsonify({"error": "Count exceeds limits"}), 400
    except ValueError:
        return jsonify({"error": "Invalid Count"}), 400

    if not field or not raw_value:
        return jsonify({"error": "Missing field or value parameter"}), 400

    if field not in queryable_fields:
        return jsonify({"error": "Invalid field parameter"}), 400

    if not method:
        method = "ILIKE"
    if field in FORCE_EXACT_FIELDS:
        method = "IS"

    try:
        result = perform_search_query(field, raw_value, method, count)
        result.pop("next_cursor", None)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        print(f"error: {exc}")
        return jsonify({"error": str(exc)}), 500


# Route pour avoir plein de messages
@app.route("/get_bulk_msgs", methods=["POST"])
def get_bulk_msg():
    # Connect to clickhouse
    client = Client(host=clickhouse_host, port=clickhouse_port)

    msgs = ", ".join(
        f"({chat_id},{msg_id})" for chat_id, msg_id in json.loads(request.get_json())
    )

    # For Test
    # msgs = (1002226407578, 2619),(1001906370364, 124), (1002496052449, 33), (1002420966456, 80), (1002431844842, 521), (1002471872732, 105), (1002431844842, 495)

    column_names = [
        "chat_id",
        "msg_id",
        "chat_name",
        "title",
        "username",
        "document_name",
        "document_size",
        "text",
    ]

    query_column = ", ".join(column_names)
    query = f"SELECT {query_column} FROM {database_name}.{table_name} WHERE (chat_id, msg_id) in ({msgs})"
    result = client.execute(query, {})
    del client

    results_dict = [dict(zip(column_names, row)) for row in result]
    hash_return = {}
    for item in results_dict:
        hash_return[f"{item.get('chat_id')}-{item.get('msg_id')}"] = item
    return jsonify(hash_return)


# Route pour récupérer un message
@app.route("/get_msg", methods=["GET"])
def get_msg():
    msg_id = request.args.get("msg_id")
    chat_id = request.args.get("channel_id")

    if not (valid_integer(msg_id) and valid_integer(chat_id)):
        return jsonify({})

    try:
        client = Client(host=clickhouse_host, port=clickhouse_port)

        query = f"""
            SELECT {star} 
            FROM {database_name}.{table_name}
            WHERE msg_id = %(msg_id)s AND chat_id = %(chat_id)s
            LIMIT 1
        """
        result = client.execute(query, {"msg_id": int(msg_id), "chat_id": int(chat_id)})
        column_names = valid_fields  # Make sure this matches the SELECT columns order
        results_dict = [dict(zip(column_names, row)) for row in result]

        return jsonify(results_dict)

    except Exception as e:
        print(f"[ERROR] get_msg failed: {e}")
        return jsonify({"error": "internal server error"}), 500

    finally:
        if client:
            client.disconnect()


# Routes pour les stats
@app.route("/get_stats_chan", methods=["GET"])
def get_stats_chan():
    # Connect to clickhouse
    client = Client(host=clickhouse_host, port=clickhouse_port)

    chat_name = request.args.get("chan_name")
    fresult = {}
    query = f"SELECT chat_id from {database_name}.{table_name} where chat_name = '{chat_name}' limit 1"
    result = client.execute(query, {})

    # On a pas trouvé le Chat name
    if len(result) == 0:
        # Seems stupid, mais ca prouve qu'on a des datas
        if chat_name.startswith("100"):
            chat_id = int(chat_name)
            query = f"SELECT chat_id from {database_name}.{table_name} where chat_id = {chat_id} limit 1"
            result = client.execute(query, {})

    if not result:
        # todo error et on se casse
        fresult["stats"] = False
        return jsonify(fresult)

    fresult["stats"] = True
    chat_id = int(result[0][0])

    # Get the count of inserted document by last 31 jours
    query = f"SELECT formatDateTime(toDate(toStartOfDay({DATE_COLUMN})), '%%d/%%m') as actual_date, count(*) as count FROM \
              {database_name}.{table_name}  WHERE {DATE_COLUMN} >= toStartOfDay(subtractDays(now(), 31)) and chat_id = {chat_id} \
              GROUP BY toStartOfDay({DATE_COLUMN}) order by toStartOfDay({DATE_COLUMN});"
    result = client.execute(query, {})

    # Créer une liste de dates sur 31 jours à partir d'aujourd'hui
    today = date.today()  # Obtenir la date actuelle
    date_list = [(today - timedelta(days=i)).strftime("%d/%m") for i in range(31)]

    # Convertir les dates de daily_data en un dictionnaire
    data_dict = {entry[0]: entry for entry in result}

    # Créer le tableau complet avec les 31 jours, remplis de 0 si data absente
    filled_data = []

    for i in range(31):
        date_obj = date_list[i]  # Obtenir la date actuelle dans la boucle
        date_display = date_list[i]  # Format 'jj/mm'

        if date_obj in data_dict:
            # Si la date est présente dans les données existantes, on l'ajoute telle quelle
            filled_data.append(data_dict[date_obj])
        else:
            # Si la date est absente, on l'ajoute avec la valeur 0
            filled_data.append((date_display, 0))

    fresult["daily"] = filled_data

    # Get the count of inserted document by last 24h
    query = f"SELECT toStartOfHour({DATE_COLUMN}) as actual_hour, formatDateTime(toStartOfHour({DATE_COLUMN}), '%%H:00') as hour_formatted, count(*) as count from\
             {database_name}.{table_name}  WHERE {DATE_COLUMN} >= subtractHours(now(), 24) and chat_id = {chat_id} GROUP BY actual_hour ORDER BY actual_hour DESC;"
    query = f"SELECT formatDateTime(toStartOfHour({DATE_COLUMN}), '%%H:00') as hour_formatted, count(*) as count FROM\
             {database_name}.{table_name} WHERE {DATE_COLUMN} >= subtractHours(now(), 24) AND chat_id = {chat_id} GROUP BY toStartOfHour({DATE_COLUMN})\
             ORDER BY toStartOfHour({DATE_COLUMN}) DESC;"

    result = client.execute(query, {})

    # 1. Créer une liste des dernières 24 heures à partir de l'heure actuelle
    now = datetime.now().replace(
        minute=0, second=0, microsecond=0
    )  # Obtenir la date et heure actuelle
    # hours_list = [(now - timedelta(hours=i)) for i in range(24)]  # Créer une liste des dernières 24 heures
    hours_format_display = [
        (now - timedelta(hours=i)).strftime("%H:00") for i in range(24)
    ]  # Format "hh.00"

    # 2. Convertir les heures de daily_data en un dictionnaire pour accès rapide
    # data_dict = {entry[0].replace(tzinfo=None): entry for entry in result}
    data_dict = {entry[0]: entry for entry in result}

    # 3. Créer le tableau complet avec les 24 heures
    filled_data = []

    for i in range(24):
        date_obj = hours_format_display[i]

        if date_obj in data_dict:
            # Si l'heure est présente dans les données existantes, on l'ajoute telle quelle
            filled_data.append(data_dict[date_obj])
        else:
            # Si l'heure est absente, on l'ajoute avec la valeur 0
            filled_data.append((date_obj, 0))

    fresult["hourly"] = filled_data

    # Get the count of inserted document by all months
    query = f"SELECT toStartOfMonth({DATE_COLUMN}) as month, formatDateTime(toStartOfMonth({DATE_COLUMN}), '%%Y/%%m') as month_formatted, count(*) as count FROM \
              {database_name}.{table_name} WHERE {DATE_COLUMN} >= subtractMonths(now(), 24) and chat_id = {chat_id} \
              GROUP BY month ORDER BY month DESC"
    result = client.execute(query, {})
    fresult["monthly"] = result

    del client
    return jsonify(fresult)


# Routes pour les stats
@app.route("/get_stats", methods=["GET"])
def get_stats():

    # Connect to clickhouse
    client = Client(host=clickhouse_host, port=clickhouse_port)

    fresult = {}

    # Get the count of inserted document by last 31 jours
    query = f"SELECT toDate({INSERT_DATE_COLUMN}) as actual_date, formatDateTime(toDate({INSERT_DATE_COLUMN}), '%%d/%%m') as day_formatted, count(*) as count FROM \
              {database_name}.{table_name} WHERE {INSERT_DATE_COLUMN} >= toStartOfDay(subtractDays(now(), 31)) GROUP BY actual_date  ORDER BY actual_date DESC;"
    result = client.execute(query, {})
    fresult["cdaily"] = result

    # Get the count of inserted document by last 24h
    query = f"SELECT toStartOfHour({INSERT_DATE_COLUMN}) as actual_hour, formatDateTime(toStartOfHour({INSERT_DATE_COLUMN}), '%%H:00') as hour_formatted, count(*) as count FROM {database_name}.{table_name}  WHERE {INSERT_DATE_COLUMN} >= subtractHours(now(), 24) GROUP BY actual_hour ORDER BY actual_hour DESC;"
    result = client.execute(query, {})
    fresult["chourly"] = result

    # Get the count of inserted document by 24 months
    query = f"SELECT toStartOfMonth({INSERT_DATE_COLUMN}) as month,     formatDateTime(toStartOfMonth({INSERT_DATE_COLUMN}), '%%Y/%%m') as month_formatted, count(*) as count FROM {database_name}.{table_name} WHERE {INSERT_DATE_COLUMN} >= subtractMonths(now(), 24) GROUP BY month ORDER BY month DESC limit 24"
    result = client.execute(query, {})
    fresult["cmonthly"] = result

    # Get the count of document in db by publish day on last 31 days
    query = f"SELECT toStartOfMonth({INSERT_DATE_COLUMN}) as month, formatDateTime(toDate({INSERT_DATE_COLUMN}), '%%y/%%m'), count(*) as count FROM {database_name}.{table_name} WHERE {INSERT_DATE_COLUMN} >= subtractMonths(now(), 24) GROUP BY month ORDER BY month ASC"
    query = f"SELECT toDate({DATE_COLUMN}) as actual_date, formatDateTime(toDate({DATE_COLUMN}), '%%d/%%m') as day_formatted, count(*) as count FROM \
            {database_name}.{table_name} WHERE {DATE_COLUMN} >= toStartOfDay(subtractDays(now(), 31)) GROUP BY actual_date  ORDER BY actual_date DESC;"
    result = client.execute(query, {})
    fresult["daily"] = result

    # Get the count of document in db by publish day on last 24h
    query = f"SELECT toStartOfHour({DATE_COLUMN}) as actual_hour, formatDateTime(toStartOfHour({DATE_COLUMN}), '%%H:00') as hour_formatted, count(*) as count FROM {database_name}.{table_name}  WHERE {DATE_COLUMN} >= subtractHours(now(), 24) GROUP BY actual_hour ORDER BY actual_hour DESC;"
    result = client.execute(query, {})
    fresult["hourly"] = result

    # Get the count of document in db by publish day on last 24 month
    query = f"SELECT toStartOfMonth({DATE_COLUMN}) as month, formatDateTime(toStartOfMonth({DATE_COLUMN}), '%%Y/%%m') as month_formatted, count(*) as count FROM {database_name}.{table_name} WHERE {INSERT_DATE_COLUMN} >= subtractMonths(now(), 24) GROUP BY month ORDER BY month DESC limit 24"
    result = client.execute(query, {})
    fresult["monthly"] = result

    # Get the number of differnet charts
    query = f"SELECT countDistinct(chat_id) as distinct_chat_id_count FROM {database_name}.{table_name}"
    result = client.execute(query, {})
    fresult["chats"] = result[0]

    # get the total nubmer on messages collecteds
    query = f"SELECT count(msg_id) as total_collected_messages FROM {database_name}.{table_name}"
    result = client.execute(query, {})
    fresult["msgs"] = result[0]

    # get the top 50 chatty chans
    query = f"SELECT      chat_id, chat_name, COUNT(msg_id) AS msg_count FROM {database_name}.{table_name} GROUP BY chat_id, chat_name ORDER BY msg_count DESC LIMIT 50; "
    result = client.execute(query, {})
    fresult["top50"] = result

    # Get stats about the db and compressions
    query = "SELECT name,  formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio FROM system.columns WHERE table = 'msg' GROUP BY name"
    result = client.execute(query, {})
    fresult["stats"] = result

    del client
    return jsonify(fresult)


# Route pour avoir un message
@app.route("/user_brief", methods=["GET"])
def user_brief():

    # Connect to clickhouse
    client = Client(host=clickhouse_host, port=clickhouse_port)

    user_id = request.args.get("user_id")
    s_max = 500
    has_more = False
    json_result = {}
    if valid_integer(user_id):
        query = f"SELECT count(*) FROM {database_name}.{table_name} WHERE sender_chat_id = {user_id}"
        json_result["msg_count"] = client.execute(query, {})[0][0]
        if json_result["msg_count"] > 0:
            query = f"SELECT chat_id, chat_name FROM {database_name}.{table_name} WHERE sender_chat_id = {user_id} group by chat_id, chat_name"
            json_result["user_chats"] = client.execute(query, {})
            query = f"SELECT {DATE_COLUMN} FROM {database_name}.{table_name} WHERE sender_chat_id == {user_id} order by {DATE_COLUMN} desc limit 1"
            json_result["last_msg"] = client.execute(query, {})[0][0]
            query = f"SELECT {DATE_COLUMN} FROM {database_name}.{table_name}  WHERE sender_chat_id == {user_id} order by {DATE_COLUMN} asc limit 1"
            json_result["first_msg"] = client.execute(query, {})[0][0]
            query = f"SELECT {star} FROM {database_name}.{table_name}  WHERE sender_chat_id == {user_id} order by {DATE_COLUMN} desc limit {s_max+1}"
            result = client.execute(query, {})
            has_more = False
            if len(result) >= s_max + 1:
                has_more = True
            json_result["last_msgs"] = client.execute(query, {})[:-1]
        else:
            json_result["user_chats"] = None
            json_result["last_msg"] = None
            json_result["first_msg"] = None
            json_result["last_msgs"] = None
        json_result["has_more"] = has_more
        # Préparer les résulats
        del client
        return jsonify(json_result)
    else:
        del client
        return jsonify({})


@app.route("/stats_msg", methods=["GET"])
def stats_msg():
    # Connect to clickhouse
    client = Client(host=clickhouse_host, port=clickhouse_port)

    query = "select count(msg_id), chat_id ,chat_name from tme_prod.msg  where date > toDateTime('2024-09-04 00:00:00') and date < toDateTime('2024-09-04 23:59:59')  group by chat_id,chat_name order by count(msg_id) desc limit 25"
    result = client.execute(query, {})

    del client
    return jsonify(result)


@app.route("/index", methods=["GET"])
def index():
    """
    Provides last 500 messages collected
    With "final" statement
    """
    # Connect to clickhouse
    client = Client(host=clickhouse_host, port=clickhouse_port)
    # query = f"select date, chat_id, msg_id, chat_name from {database_name}.{table_name} final where date > now() - INTERVAL 1 HOUR order by date desc"
    query = f"select formatDateTime(toTimeZone({DATE_COLUMN}, 'UTC'), '%%Y-%%m-%%dT%%H:%%i:%%S+00:00') AS date, chat_id, msg_id, chat_name from {database_name}.{table_name} order by {INSERT_DATE_COLUMN} desc, msg_id desc limit 500"
    result = client.execute(query, {})

    del client
    return jsonify(result)


# Route pour les last messages
@app.route("/count", methods=["GET"])
def count():
    # Connect to clickhouse
    client = Client(host=clickhouse_host, port=clickhouse_port)

    query = f"select count(chat_name) from {database_name}.{table_name}"
    result = client.execute(query, {})

    del client
    return jsonify({"count": result[0][0]})


@app.route("/last", methods=["GET"])
def last():
    """
    # Route qui donne les last messages importés,
    # Filter out ce qui est "vide" (pas attachement, et pas text)
    # Filter out ce qui est + de 2 ans.
    # donne les messages collectés dans la clickhouse
    # 2 params,
    #   since = timestamp du debut.
    #   for = nombre de minutes a fournir.
    #
    #  wget "http://localhost:6000/last?since=1749342874&for=15" -O -  | jq .
    """

    if request.args.get("since"):
        since = request.args.get("since")  # Get Unix TimeStamp
    else:
        since = int(round(time.time() * 1000))  # Sinon c'est NOW

    if request.args.get("for"):
        tfor = request.args.get("for")  # minutes to fetch
    else:
        tfor = 5  # Si pas précisé c'est 5

    # LIMITS and default
    if not valid_integer(since):  # Si bad integer = Now
        since = int(round(time.time() * 1000))
    else:
        since = int(since)
    if not valid_integer(tfor):
        tfor = 5
    else:
        tfor = int(tfor)
    tfor = (tfor * 60) + since  # convert to millisec

    page_size = 50000  # Taille des records par réponse (chunk)

    def generate():
        """
        Generator of message with pagination for query
        """
        client = Client(host=clickhouse_host, port=clickhouse_port)

        messages = 0
        offset = 0

        # on ne demande que la date spécifé dans le post
        # on ne prends pas les message de plus de 2 ans
        # on ne prends pas les vide
        while True:
            # Requête SQL avec pagination et limite qui ne choppe pas les texte vide
            query = f"""
            SELECT {star} 
            FROM {database_name}.{table_name} 
            WHERE {INSERT_DATE_COLUMN} >= toDateTime({since}) 
              AND {INSERT_DATE_COLUMN} <= toDateTime({tfor}) 
              AND {DATE_COLUMN} >= dateSub(now(), INTERVAL 2 YEAR) 
              AND ((document_present = 1) OR (text != '')) 
            LIMIT {page_size} OFFSET {offset}
            """

            # Exécuter la sql
            result = client.execute(query, {})
            print(f"SQL page fetched, offset: {offset}")  # Kindoff debug

            # Si aucun résultat n'est retourné, arrêter
            if not result:
                break

            # Préparer les résultats
            column_names = valid_fields
            results_dict = [dict(zip(column_names, row)) for row in result]
            out_dict = []

            for msg in results_dict:
                messages += 1
                # Convertir les objets datetime en compatible json
                msg["insert_date"] = msg.get("insert_date")
                msg["date"] = msg.get("date")

                htext = f"On {msg.get('date')} on Telegram\n"
                htext += f"The following data was collected from the channel {msg.get('chat_name')}/{msg.get('chat_id')} with message id {msg.get('id')}\n"
                htext += (
                    f"User {msg.get('username')}/{msg.get('sender_chat_id')} wrote\n"
                )
                htext += f"Subject: {msg.get('title')}\n"
                htext += "Content: " + msg.get("text") + "\n"
                if msg.get("msg_fwd") == 1:
                    htext += f"It was a forward from the channel {msg.get('msg_fwd_username')}/{msg.get('msg_fwd_id')}\n"
                if msg.get("document_present") == 1:
                    htext += f"The document {msg.get('document_name')}/{msg.get('document_type')} with a size of {msg.get('document_size')} bytes was attached to this messages.\n"
                htext += f"\nThis message was acquired on {msg.get('insert_date')}\n"

                out_dict.append(
                    {
                        "date": msg.get("insert_date"),
                        "text": htext,
                        "text_hash": hashlib.md5(
                            msg.get("text").encode("utf-8", "ignore")
                        ).hexdigest(),
                        "channel_id": msg.get("chat_id"),
                        "channel_name": msg.get("chat_name"),
                        "msg_id": msg.get("id"),
                    }
                )

            # Convertir en JSON et envoyer un chunk
            yield json.dumps(
                {"results": out_dict, "length": len(out_dict)},
                default=serialize_datetime,
            ) + "\n"

            # Incrémenter l'offset pour la page suivante
            offset += page_size

        print(f"Send Messages {messages}")
        del client

    return Response(generate(), content_type="application/json")


@app.route("/insert_records", methods=["POST"])
def insert_records():
    """
    # Collect messages to integrate into the database.
    """

    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid or missing JSON data"}), 400

    records = json.loads(data).get("records")

    if not records:
        return jsonify({"error": "No records found in the JSON data"}), 400

    # Conversion des champs datetime pour chaque record
    records = [convert_record(record) for record in records]

    # Connect to clickhouse
    client = Client(host=clickhouse_host, port=clickhouse_port)

    try:
        client.execute(f"INSERT INTO {database_name}.{table_name} VALUES", records)
        logger.info(f"Inserted {len(records)} records into ClickHouse")
        return jsonify({"status": "success", "inserted_records": len(records)}), 200
    except Exception as e:
        logger.error(f"Failed to insert records: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        del client


@app.route("/graph", methods=["GET"])
def get_graph():
    chat_id = request.args.get("chat_id")

    clickhouse_client = Client(host=clickhouse_host, port=clickhouse_port)
    if not chat_id:
        return jsonify({"error": "chat_id is required"}), 400

    try:
        # Requête pour récupérer les nouds (utilisateurs du chat_id)
        nodes_query = f"""
            SELECT DISTINCT
                sender_chat_id AS id,
                username AS label,
                chat_name
            FROM tme_prod.msg
            WHERE chat_id = {chat_id}
            AND msg_fwd_id != 0 
            AND sender_chat_id != {chat_id}
            limit 100
        """

        # Exécution des requêtes
        nodes_result = clickhouse_client.execute(nodes_query)
        # edges_result = clickhouse_client.execute(edges_query)

        # Dictionnaire pour assurer l'unicité des nœuds
        nodes_map = {}

        chat_name = ""
        for row in nodes_result:
            user_id = row[0]
            username = row[1].replace(
                "None", ""
            )  # suite a une non gestion historique, certain champ usernane sont none
            chat_name = row[2]

            if user_id not in nodes_map:
                nodes_map[user_id] = {"id": user_id, "labels": set()}
            nodes_map[user_id]["labels"].add(
                username
            )  # Utilisation d'un set pour stocker les labels

        # Liste des nœuds et des arêtes
        nodes = [
            {
                "id": 1,
                "label": f"{chat_id}\n{chat_name}",
                "shape": "box",
                "color": "purple",
            }
        ]  # Ajouter le channel principal

        edges = []
        seen_edges = set()

        for row in nodes_result:
            from_id = 1  # Assumons que le noeud source est toujours 1
            to_id = row[0]  # id du nœud cible
            edge_label = ""  # Label pour l'arête

            edge = (from_id, to_id)  # Crée un tuple représentant l'arête

            # Vérifie si l'arête n'a pas encore été ajoutée
            if edge not in seen_edges:
                edges.append({"from": from_id, "to": to_id, "label": edge_label})
                seen_edges.add(
                    edge
                )  # Ajoute l'arête à l'ensemble pour éviter les doublons

        # Ajouter les utilisateurs comme nœuds
        for user_id, user_data in nodes_map.items():
            nodes.append(
                {
                    "id": user_data["id"],
                    "label": f'{user_data["id"]}\n' + "\n".join(user_data["labels"]),
                }
            )

        # Vérifier si chaque utilisateur est aussi dans d'autres chats
        for user_id in nodes_map.keys():
            other_chats_query = f"""
                SELECT DISTINCT chat_id, chat_name
                FROM tme_prod.msg
                WHERE sender_chat_id = {user_id}
                AND chat_id != {chat_id} 
            """
            other_chats_result = clickhouse_client.execute(other_chats_query)

            for other_chat in other_chats_result:
                other_chat_id = other_chat[0]
                other_chat_node_id = (
                    f"{other_chat_id}"  # ID unique pour éviter les conflits
                )

                # Ajouter le nouveau chat en rouge s'il n'existe pas déjà
                if not any(n["id"] == other_chat_node_id for n in nodes):
                    nodes.append(
                        {
                            "id": other_chat_node_id,
                            "label": f"{other_chat_id}\n{other_chat[1]}",
                            "color": "red",
                            "shape": "box",
                        }
                    )

                # Vérifie si user_id est différent de other_chat_node_id avant d'ajouter l'arête
                if str(user_id) != other_chat_node_id:
                    edges.append({"from": user_id, "to": other_chat_node_id})

        # Retourner les données au format JSON
        return jsonify({"nodes": nodes, "edges": edges})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        del clickhouse_client


@app.route("/user_talk/<int:user>", methods=["GET"])
def user_talk(user):
    # Requête ClickHouse pour récupérer les données pour un user donné

    client = Client(host=clickhouse_host, port=clickhouse_port)
    query = f"""
    SELECT
        toDate(date) AS day,
        chat_id,
        chat_name,
        COUNT(*) AS count
    FROM tme_prod.msg
    WHERE sender_chat_id = {user}
    GROUP BY day, chat_id, chat_name
    ORDER BY day
    """

    # Exécuter la requête ClickHouse et obtenir le résultat
    result = client.execute(query)
    if not result:
        return jsonify({})

    # Organiser les données dans un dictionnaire pour le traitement
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    # Remplir le dictionnaire avec les données
    for row in result:
        day, chat_id, chat_name, count = row
        data[day][chat_id]["name"] = chat_name
        data[day][chat_id][
            "count"
        ] += count  # Incrémenter le count pour chaque chat_id et jour

    # Transformer le dictionnaire en une liste de dictionnaires pour le format JSON
    formatted_data = []
    for day, chats in data.items():
        for chat_id, chat_info in chats.items():
            formatted_data.append(
                {
                    "day": str(day),  # Convertir la date en chaîne pour le JSON
                    "chat_id": chat_id,
                    "chat_name": chat_info["name"],
                    "count": chat_info["count"],
                }
            )
    del client
    return jsonify(formatted_data)


@app.route("/user_dailytalk/<int:user_id>")
def user_dailytalk(user_id):
    client = Client(host=clickhouse_host, port=clickhouse_port)
    # Requête pour récupérer les données depuis ClickHouse
    query = f"""
    SELECT
        toHour(date) AS hour,                  -- Extraire l'heure
        toDayOfWeek(date) AS day_of_week,      -- Extraire le jour de la semaine (1 = lundi, 2 = mardi, ...)
        count(*) AS message_count              -- Compter le nombre de messages
    FROM
        tme_prod.msg  -- Nom de la table
    WHERE
        sender_chat_id = {user_id}             -- Filtrer pour l'utilisateur avec l'ID spécifique (sender_chat_id)
    GROUP BY
        day_of_week,                           -- Regrouper par jour de la semaine
        hour                                   -- Regrouper par heure
    ORDER BY
        day_of_week ASC,                       -- Trier par jour de la semaine (lundi = 1, dimanche = 7)
        hour ASC                               -- Trier par heure (0 à 23)
    """
    # Exécuter la requête
    result = client.execute(query)
    if not result:  # Si pas de data, empty reponse.
        return jsonify({})

    # Créer des dictionnaires pour les données de la heatmap
    heatmap_data = {
        "Monday": [0] * 24,
        "Tuesday": [0] * 24,
        "Wednesday": [0] * 24,
        "Thursday": [0] * 24,
        "Friday": [0] * 24,
        "Saturday": [0] * 24,
        "Sunday": [0] * 24,
    }

    # Mapper les numéros de jours de la semaine (1 = Monday, 7 = Sunday) aux noms de jours
    jours_map = {
        1: "Monday",
        2: "Tuesday",
        3: "Wednesday",
        4: "Thursday",
        5: "Friday",
        6: "Saturday",
        7: "Sunday",
    }

    # remplir les données de la heatmap
    for row in result:
        hour = row[0]  # Heure de la date (0-23)
        day_of_week = jours_map[row[1]]  # Jour de la semaine
        count = row[2]  # Nombre de messages

        # Ajouter les messages dans le bon jour et heure
        heatmap_data[day_of_week][hour] = count

    del client
    # Convertir le dictionnaire en JSON
    return jsonify(heatmap_data)


@app.route("/user_details/<int:user_id>")
def user_details(user_id):
    client = Client(host=clickhouse_host, port=clickhouse_port)
    # Requête pour récupérer les données depuis ClickHouse
    if not valid_integer(user_id):
        return jsonify({"results": False})

    query = f" select date from  tme_prod.msg where sender_chat_id = {user_id} order by date asc limit 1;"
    data = client.execute(query)
    if not data:
        return jsonify({"results": False})
    date_in = data[0][0].strftime("%d/%m/%Y")

    query = f" select date from  tme_prod.msg where sender_chat_id = {user_id} order by date desc limit 1;"
    data = client.execute(query)
    date_out = data[0][0].strftime("%d/%m/%Y")

    resume = f"Account {user_id} is active since {date_in} to {date_out}"

    query = f"SELECT distinct(chat_id,chat_name, username ) FROM tme_prod.msg where sender_chat_id == {user_id}"

    pseudos = []
    data = client.execute(query)
    for line in data:
        username = line[0][2].replace(
            "None", ""
        )  # suite a une non gestion historique, certain champ usernane sont none
        if username == " ":
            pseudos.append(("Unknown pseudo", line[0][0], line[0][1]))
        else:
            pseudos.append((username, line[0][0], line[0][1]))

    return jsonify({"resume": resume, "pseudos": pseudos, "results": True})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=app_port)
