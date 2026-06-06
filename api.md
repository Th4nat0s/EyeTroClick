# API

Base URL examples use `http://127.0.0.1:6000`.

All responses are JSON unless noted otherwise. Errors use HTTP status codes and an `error` field when possible.

## Conventions

### Optional flags

Boolean query flags are enabled when present with a non-false value.

False values:

```text
0, false, no, off
```

Example:

```bash
wget -qO- "http://127.0.0.1:6000/get_channel/1001737065444?id=1&timestamp=1" | jq .
```

Compatibility aliases:

```text
id, ids, --id
timestamp, timestamps, --timestamp
```

### IDs and timestamps

Routes returning channel messages hide IDs and timestamps by default.

Use `id=1` to include identifiers.
Use `timestamp=1` to include date fields.

### Databases

Channel metadata comes from local SQLite `app.db`, table `comms`.

Message content comes from ClickHouse unless route says SQLite only.

## GET /get_channel/<channel_id>

Return metadata for one Telegram channel and its latest messages.

Optional query parameters:

- `limit=<count>`: number of messages to return. Default `100`, minimum `1`, maximum `65000`.
- `id=1`: include message/channel IDs.
- `timestamp=1`: include message timestamps.

Channel lookup uses SQLite `comms.telegram_id` first. If no row matches, it falls back to SQLite `comms.id`.

Messages are read from ClickHouse with:

```sql
abs(chat_id) = telegram_id
ORDER BY date DESC
LIMIT <limit>
```

Default call:

```bash
wget -qO- "http://127.0.0.1:6000/get_channel/1001737065444" | jq .
```

Custom message count:

```bash
wget -qO- "http://127.0.0.1:6000/get_channel/1001737065444?limit=250" | jq .
```

Response:

```json
{
  "results": true,
  "channel_name": "Channel name",
  "url": "https://t.me/channel",
  "description": "Channel description",
  "messages": [
    {
      "chat_name": "Channel name",
      "username": "sender",
      "title": "",
      "text": "message text",
      "lang": "en",
      "urls": [],
      "hashtags": []
    }
  ]
}
```

With IDs and timestamps:

```bash
wget -qO- "http://127.0.0.1:6000/get_channel/1001737065444?limit=250&id=1&timestamp=1" | jq .
```

Extra fields:

```json
{
  "channel_id": "1001737065444",
  "messages": [
    {
      "id": 123,
      "chat_id": 1001737065444,
      "sender_chat_id": 123456789,
      "msg_fwd_id": 0,
      "date": "2026-05-28T10:00:00+00:00",
      "insert_date": "2026-05-28T10:00:10+00:00"
    }
  ]
}
```

Errors:

```json
{"results": false}
```

HTTP `404` when channel not found.

```json
{"error": "sqlite lookup failed"}
```

HTTP `500` when SQLite lookup fails.

```json
{"error": "clickhouse lookup failed"}
```

HTTP `500` when message lookup fails.

## GET /getchatrandoms/<count>

Return random Telegram chats from SQLite, validated against ClickHouse.

Filters:

```sql
telegram_id IS NOT NULL
telegram_id != ''
last_id > 300
```

`count` is capped at `1000`.

Each selected channel is checked in ClickHouse. If ClickHouse has no messages
for that `telegram_id`, the channel is disabled for this route by setting
`comms.last_id = 0`, then another random channel is selected when possible.

Example:

```bash
wget -qO- "http://127.0.0.1:6000/getchatrandoms/10" | jq .
```

Response:

```json
{
  "results": true,
  "count": 10,
  "disabled_empty_channels": 0,
  "chats": [
    {
      "channel_id": "1001737065444",
      "channel_name": "Channel name",
      "url": "https://t.me/channel",
      "description": "Channel description",
      "last_id": 1234
    }
  ]
}
```

Errors:

```json
{"error": "Invalid count"}
```

HTTP `400` when `count < 1`.

```json
{"error": "sqlite lookup failed"}
```

HTTP `500` when SQLite lookup fails.

## GET /get_msg

Return one ClickHouse message.

Parameters:

```text
channel_id=<chat_id>
msg_id=<message_id>
```

Example:

```bash
wget -qO- "http://127.0.0.1:6000/get_msg?channel_id=1001737065444&msg_id=123" | jq .
```

Response is a JSON array with zero or one message.

## POST /get_bulk_msgs

Return multiple messages from ClickHouse.

Body is a JSON string containing an array of `[chat_id, msg_id]` pairs.

Example:

```bash
wget -qO- \
  --header="Content-Type: application/json" \
  --post-data='"[[1001737065444,123],[1001737065444,124]]"' \
  "http://127.0.0.1:6000/get_bulk_msgs" | jq .
```

Response is an object keyed by `<chat_id>-<msg_id>`.

## GET /search

Search ClickHouse messages.

Parameters:

```text
field=<field>
value=<value>
method=IS|LIKE|ILIKE
count=<limit>
```

Example:

```bash
wget -qO- "http://127.0.0.1:6000/search?field=text&value=test&method=ILIKE&count=10" | jq .
```

`count` max is `1001`.

## GET /search_latest

Same search API as `/search`, but returns newest results ordered by logical message date.

Parameters:

```text
field=<field>
value=<value>
method=IS|LIKE|ILIKE
count=<limit>
before_date=<ISO date, optional>
```

`count` max is `100`.

Example:

```bash
wget -qO- "http://127.0.0.1:6000/search_latest?field=text&value=test&method=ILIKE&count=10" | jq .
```

## POST /search_go_telegrams

Bridge endpoint used by UI.

JSON body:

```json
{
  "field": "text",
  "value": "test",
  "method": "ILIKE",
  "count": 10,
  "before_date": "2026-05-28T10:00:00+00:00"
}
```

Example:

```bash
wget -qO- \
  --header="Content-Type: application/json" \
  --post-data='{"field":"text","value":"test","method":"ILIKE","count":10}' \
  "http://127.0.0.1:6000/search_go_telegrams" | jq .
```

## GET /search_channel_text

Search text inside one channel.

Parameters:

```text
chat_id=<chat_id>
text=<search text>
method=like|ilike
count=<limit>
```

`count` max is `50`.

Example:

```bash
wget -qO- "http://127.0.0.1:6000/search_channel_text?chat_id=1001737065444&text=test&method=ilike&count=10" | jq .
```

## GET /count

Return total message count in ClickHouse.

Example:

```bash
wget -qO- "http://127.0.0.1:6000/count" | jq .
```

## GET /last

Stream recently inserted messages for ingestion.

Parameters:

```text
since=<unix timestamp seconds>
for=<minutes>
```

Example:

```bash
wget -qO- "http://127.0.0.1:6000/last?since=1749342874&for=15" | jq .
```

Notes:

```text
Only messages inserted between since and since+for are returned.
Messages older than 2 years are skipped.
Empty text without attachment is skipped.
Response is newline-delimited JSON chunks.
```

## GET /translate

Translate text with LibreTranslate.

Parameters:

```text
text=<text>
source=<source lang, optional>
target=<target lang>
```

## GET /get_stats_chan

Return channel statistics.

Parameter:

```text
chan_name=<channel name or id>
```

## GET /get_stats

Return global statistics.

## GET /user_brief

Return short user activity summary.

Parameter:

```text
user=<sender_chat_id>
```

## GET /user_details/<user_id>

Return detailed user activity, including active dates and channels.

## GET /user_talk/<user>

Return user talk data.

## GET /user_dailytalk/<user_id>

Return user daily talk data.

## GET /stats_msg

Return message stats.

## GET /index

Return ClickHouse index data.

## POST /insert_records

Insert ClickHouse records.

## GET /graph

Return channel/user graph data.
