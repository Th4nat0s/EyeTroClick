# Synchronization routes and tools

Synchronization keeps channel metadata aligned with message data. Channel
message IDs and dates come from ClickHouse; metadata updates are sent to the
configured Eyetroduit endpoint.

## `sync_last_ids.py`

The command reads each channel's first and last message boundary from
ClickHouse, then sends one update per channel to `tagch`.

```bash
.venv/bin/python sync_last_ids.py --config gn_config.yaml
```

Useful options:

```text
--telegram-id ID   process one channel
--batch-size N     ClickHouse batch size, maximum 10000
--limit N          process at most N channels
--dry-run          print payloads without HTTP updates
```

The job paginates by ascending absolute Telegram channel ID. Each payload
contains `telegram_id`, `first_msg`, `last_id`, `last_msg`, `first_blood`, and
`touch_last_seen`. Non-200 responses count as failures; exit status `2` means
at least one channel update failed.

## `POST /insert_records`

Insert message records into ClickHouse. Legacy callers may send the existing
JSON-string body; newer callers may send an object containing `records` and an
optional stable `batch_id`.

```json
{
  "batch_id": "durable-batch-id",
  "records": []
}
```

The endpoint validates the payload before insertion and returns success only
after the message batch is accepted. Callers should retry non-2xx responses
with the same `batch_id`.

## `GET /last`

Return recently inserted messages.

Parameters:

```text
since=<unix timestamp in milliseconds>
for=<window length in minutes>
```

Defaults: current time and a five-minute window. This route is useful for
short-interval consumers that need to discover newly inserted messages.

## `GET /count`

Return total message count:

```json
{"count": 1234}
```

## `GET /get_channel/<channel_id>`

Return channel metadata plus recent messages. Use `?id=1&timestamp=1` to keep
message IDs and timestamps in the response.
