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

## Daily missing-boundary repair

eYeTr0du1t may call `POST /sync_missing_metadata` from its frequent database
maintenance job. The endpoint accepts a bounded `telegram_ids` list, reads
first/last message boundaries from ClickHouse, and updates the configured
eYeTr0du1t `tagch` endpoint. Protect it with `metadata_sync_api_key` (or the
`METADATA_SYNC_API_KEY` environment variable). Reads are safe alongside
ClickHouse inserts; callers should avoid overlapping repair requests.

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

Legacy route retained for existing consumers. It keeps its existing streaming
contract and is not subject to the bounded pagination contract below.

## `GET /getlast`

Return recently inserted messages.

Parameters:

```text
since=<unix timestamp in seconds, maximum 31 days old>
for=<window length in minutes, maximum 44640>
page=<zero-based page number, maximum 1000>
per_page=<results per page, maximum 50000>
```

Defaults: previous five minutes, page `0`, and `per_page=50000`. The window
cannot end in the future or exceed 31 days. Results use deterministic
`insert_date`, `chat_id`, `msg_id` ascending order. Each request returns one
bounded page and includes `has_more`, `page`, and `per_page`; request the next
page when `has_more` is true. This route is intended for recent synchronization,
not unrestricted database export.

## `GET /count`

Return total message count:

```json
{"count": 1234}
```

## `GET /get_channel/<channel_id>`

Return channel metadata plus recent messages. Use `?id=1&timestamp=1` to keep
message IDs and timestamps in the response.
