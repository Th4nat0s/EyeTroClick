# EyeTroClick

EyeTroClick is the ClickHouse message backend used by Eyetroduit. It provides
message search, analysis endpoints, bulk insertion, and a bounded replication
interface for DarkTroSync.

## ClickHouse schema

Example message table:

```sql
CREATE TABLE msg
(
    msg_id               UInt64,
    chat_id              Int64,
    chat_name            String,
    username             String,
    sender_chat_id       Int64,
    title                String,
    date_utc             DateTime('UTC'),
    insert_date_utc      DateTime('UTC'),
    document_present     UInt8,
    document_name        String,
    document_type        String,
    document_size        UInt64,
    msg_fwd              UInt8,
    msg_fwd_username     String,
    msg_fwd_title        String,
    msg_fwd_id           UInt64,
    text                 String,
    lang                 String,
    urls                 Array(String),
    hashtags             Array(String)
)
ENGINE = MergeTree
ORDER BY msg_id;
```

## Replication manifest

Every successful `/insert_records` request is recorded in a compact SQLite
manifest after ClickHouse accepts the messages. The manifest stores one message
ID range per channel and insert batch, rather than one row per message.

Set these values in `gn_config.yaml` or through the documented environment
override for the secret:

```yaml
replication_manifest_path: '/var/lib/eyetroclick/replication.sqlite3'
replication_api_key: ''
replication_consumer: 'darktrosync'
replication_manifest_page_size: 1000
replication_export_page_size: 10000
replication_max_ranges: 100
replication_query_timeout_seconds: 30
replication_retention_days: 90
replication_cleanup_batch_size: 10000
```

Use `REPLICATION_API_KEY` instead of storing the internal bearer token in the
configuration file. The manifest directory must be writable by the EyeTroClick
service account and must live on persistent storage.

SQLite uses WAL mode, full synchronous commits, bounded lock waits, and short
transactions. Manifest cleanup runs after acknowledgements and removes only
rows that are both older than retention and acknowledged by every registered
consumer. Maintenance uses incremental vacuum and a passive WAL checkpoint.

## Insert contract

`/insert_records` remains compatible with the legacy JSON-string payload and
also accepts a regular JSON object. Importers should add a stable `batch_id`:

```json
{
  "batch_id": "45e1fb12-74e6-4f42-b886-f1e306bd66bb",
  "records": []
}
```

Legacy requests receive a deterministic batch ID derived from sorted
`(chat_id, msg_id)` keys. EyeTroClick performs these operations in order:

1. Insert the messages into ClickHouse.
2. Commit grouped channel ranges to SQLite.
3. Return HTTP 200.

If the SQLite commit fails after ClickHouse succeeds, the endpoint returns
HTTP 500. The importer must retry the same batch. ReplacingMergeTree handles the
ClickHouse replay and the SQLite uniqueness constraint handles the manifest
replay. Reusing a `batch_id` with different records is rejected. All direct or
bulk import tools must honor this retry contract.

## Internal replication API

The following routes require `Authorization: Bearer <replication_api_key>` and
are intended only for the Eyetroduit proxy:

- `GET /replication/head`: retained sequence bounds and server time.
- `GET /replication/commits?after_seq=0&limit=1000`: ordered commit ranges.
- `POST /replication/messages/full`: global or single-channel full export.
- `POST /replication/messages/ranges`: export committed ranges.
- `POST /replication/ack`: monotonic consumer acknowledgement.
- `GET /replication/status`: lightweight manifest status.

Full exports accept an optional `channel_id`. All exports enforce a server-side
24-calendar-month window using the Telegram emission timestamp. A requested
older cutoff is clamped to the current retention boundary. Pagination uses
version-aware keyset cursors and never uses OFFSET or ClickHouse FINAL.
ClickHouse also enforces the configured execution timeout on every export.

Example targeted request:

```json
{
  "emitted_after": "2024-07-21T12:00:00Z",
  "channel_id": 1001234567890,
  "limit": 5000,
  "cursor": null
}
```

Range exports return `completed_seqs`, including ranges for which every message
is outside retention. Consumers may acknowledge a sequence only after its
messages are durable locally. If a commit cursor predates retained rows,
`/replication/commits` returns HTTP 410 and the consumer must run a new full
synchronization.

## Verification

Run the unit tests and quality checks from the repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -v
black --check replication_manifest.py replication_api.py tests
PYLINTHOME=/tmp/pylint-eye .venv/bin/pylint \
  replication_manifest.py replication_api.py tests
```
