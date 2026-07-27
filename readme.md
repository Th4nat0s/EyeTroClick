# EyeTroClick

EyeTroClick is the ClickHouse message backend used by Eyetroduit. It provides
message search, channel analysis, translation, bulk insertion, and channel
metadata APIs.

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

## Configuration

Copy `gn_config.yaml.sample` to `gn_config.yaml`, then set the ClickHouse,
Flask, backend, and translation service values for your deployment.

Important settings:

```yaml
clickhouse_host: '127.0.0.1'
clickhouse_port: 9000
app_port: 6000
database_name: 'mytme'
table_name: 'lesmsg'
api_key: 'change-me'
tagch: 'http://127.0.0.1:5000/mediasview/api_upd_tmedia'
```

## Documentation

- [Statistics routes](docs/stats.md)
- [Synchronization routes and tools](docs/synchro.md)
- [Complete API reference](api.md)

## Verification

Run from repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -v
git diff --check
```
